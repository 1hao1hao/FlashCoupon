package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"

	"go.opentelemetry.io/otel/propagation"
)

const transactionTopic = "FlashCouponTransactionGrant"
const transactionLedger = "fc:transaction:reservations"
const transactionSchema = `CREATE TABLE IF NOT EXISTS transaction_events (
 id CHAR(64) PRIMARY KEY, decision VARCHAR(16) NOT NULL,
 event_json JSON NOT NULL, trace_json JSON NOT NULL,
 created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)) ENGINE=InnoDB`

type transactionRequest struct {
	m, u                int64
	tid, request, trace string
}
type TransactionRuntime struct {
	S        *Service
	P        rocketmq.TransactionProducer
	requests sync.Map
}

func (t *TransactionRuntime) append(ctx context.Context, ev Event, decision, rawTrace string) error {
	ctx, end := t.S.stage(ctx, "sql.transaction_admission")
	var err error
	defer func() { end(err) }()
	tx, e := t.S.Store.DB.BeginTx(ctx, nil)
	if e != nil {
		err = e
		return e
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO transaction_events(id,decision,event_json,trace_json) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE id=id", ev.ID, decision, encode(ev), rawTrace)
	if err != nil {
		return err
	}
	err = tx.Commit()
	return err
}
func (t *TransactionRuntime) decision(ctx context.Context, id string) primitive.LocalTransactionState {
	var decision string
	err := t.S.Store.DB.QueryRowContext(ctx, "SELECT decision FROM transaction_events WHERE id=?", id).Scan(&decision)
	if err == nil {
		if decision == "accepted" {
			return primitive.CommitMessageState
		}
		return primitive.RollbackMessageState
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return primitive.UnknowState
	}
	// Redis and this MySQL append are deliberately NOT claimed to be one ACID
	// transaction. The separate accepted reservation can repair a missing SQL fact
	// during Broker checkback, including producer death after Redis acceptance.
	raw, err := t.S.R.HGet(ctx, transactionLedger, id).Result()
	if err != nil {
		return primitive.UnknowState
	}
	var ev Event
	if json.Unmarshal([]byte(raw), &ev) != nil || ev.ID != id {
		return primitive.UnknowState
	}
	tr := t.S.R.HGet(ctx, prefix(ev.MerchantID, ev.TemplateID)+"trace", id).Val()
	if t.append(ctx, ev, "accepted", tr) != nil {
		return primitive.UnknowState
	}
	return primitive.CommitMessageState
}
func (t *TransactionRuntime) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	key := string(msg.Body)
	knownCtx, knownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer knownCancel()
	var known string
	if err := t.S.Store.DB.QueryRowContext(knownCtx, "SELECT decision FROM transaction_events WHERE id=?", key).Scan(&known); err == nil {
		if known == "accepted" {
			return primitive.CommitMessageState
		}
		return primitive.RollbackMessageState
	}
	value, ok := t.requests.Load(key)
	if !ok {
		return primitive.UnknowState
	}
	req := value.(transactionRequest)
	ctx, cancel := context.WithTimeout(restoreTrace(context.Background(), req.trace), 30*time.Second)
	defer cancel()
	ctx = faultContext(ctx, key)
	ctx, end := t.S.stage(ctx, "mq.transaction_local")
	defer end(nil)
	if env("TX_TEST_ROLLBACK", "0") == "1" {
		if t.append(ctx, Event{ID: key}, "rejected", req.trace) != nil {
			return primitive.UnknowState
		}
		return primitive.RollbackMessageState
	}
	ev, err := t.S.claimWithMode(ctx, req.m, req.u, req.tid, req.request, "transaction")
	if err != nil {
		var reject *ClaimError
		if errors.As(err, &reject) && reject.Code != 5 {
			if t.append(ctx, Event{ID: key}, "rejected", req.trace) == nil {
				return primitive.RollbackMessageState
			}
		}
		return primitive.UnknowState
	}
	faultPoint(ctx, "tx_after_reservation")
	if t.append(ctx, ev, "accepted", req.trace) != nil {
		return primitive.UnknowState
	}
	faultPoint(ctx, "tx_after_local_commit")
	if env("TX_TEST_UNKNOWN", "0") == "1" {
		return primitive.UnknowState
	}
	return primitive.CommitMessageState
}
func (t *TransactionRuntime) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.S.Metrics.Retry.WithLabelValues("transaction_checkback").Inc()
	decision := t.decision(ctx, string(msg.Body))
	t.S.Metrics.Forward.WithLabelValues(fmt.Sprintf("tx_check_%d", decision)).Inc()
	return decision
}
func (m *Messaging) startTransaction(ctx context.Context, ns []string) error {
	if env("TX_EXPERIMENT_ENABLED", "0") != "1" {
		return nil
	}
	if _, err := m.S.Store.DB.ExecContext(ctx, transactionSchema); err != nil {
		return err
	}
	t := &TransactionRuntime{S: m.S}
	var err error
	t.P, err = rocketmq.NewTransactionProducer(t, producer.WithGroupName("fc-transaction-experiment"), producer.WithNsResolver(primitive.NewPassthroughResolver(ns)), producer.WithRetry(2))
	if err != nil {
		return err
	}
	if err = t.P.Start(); err != nil {
		return err
	}
	m.TX = t.P
	m.S.Transaction = t
	m.start(func() { t.recover(ctx) })
	return nil
}
func (s *Service) TransactionClaim(ctx context.Context, m, u int64, tid, request string) (Event, error) {
	ev := Event{ID: eventID(m, u, tid, request), MerchantID: m, UserID: u, TemplateID: tid}
	if s.Transaction == nil {
		return ev, fmt.Errorf("transaction experiment disabled")
	}
	tpl, err := s.Template(ctx, m, tid)
	if err != nil {
		return ev, err
	}
	if tpl.Mode != "transaction" {
		return ev, &ClaimError{4, "requires transaction template"}
	}
	t := s.Transaction
	t.requests.Store(ev.ID, transactionRequest{m, u, tid, request, carrier(ctx)})
	defer t.requests.Delete(ev.ID)
	msg := primitive.NewMessage(transactionTopic, []byte(ev.ID))
	props := propagation.MapCarrier{}
	traceContext.Inject(ctx, props)
	for k, v := range props {
		msg.WithProperty(k, v)
	}
	msg.WithProperty("fc_sent_ns", fmt.Sprint(time.Now().UnixNano()))
	ctx, endSend := s.stage(ctx, "mq.transaction_send")
	result, err := t.P.SendMessageInTransaction(ctx, msg)
	endSend(err)
	if err != nil {
		return ev, err
	}
	if result.State == primitive.RollbackMessageState {
		return ev, &ClaimError{4, "transaction rolled back"}
	}
	var raw []byte
	err = s.Store.DB.QueryRowContext(ctx, "SELECT event_json FROM transaction_events WHERE id=? AND decision='accepted'", ev.ID).Scan(&raw)
	if err != nil {
		return ev, fmt.Errorf("transaction pending checkback")
	}
	err = json.Unmarshal(raw, &ev)
	return ev, err
}
func (m *Messaging) subscribeTransactions() error {
	if env("TX_EXPERIMENT_ENABLED", "0") != "1" {
		return nil
	}
	return m.C.Subscribe(transactionTopic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		if !m.beginConsume() {
			return consumer.ConsumeRetryLater, nil
		}
		defer m.consumeWG.Done()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for _, msg := range msgs {
			var raw []byte
			var tr string
			if err := m.S.Store.DB.QueryRowContext(ctx, "SELECT event_json,trace_json FROM transaction_events WHERE id=? AND decision='accepted'", string(msg.Body)).Scan(&raw, &tr); err != nil {
				return consumer.ConsumeRetryLater, err
			}
			var ev Event
			if err := json.Unmarshal(raw, &ev); err != nil {
				return consumer.ConsumeRetryLater, err
			}
			delivery := restoreTrace(ctx, tr)
			if _, err := m.S.Deliver(delivery, ev, "transaction"); err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	})
}

// Broker checkback has a finite budget. Redis reservations remain the recovery
// source after that budget; a producer restart must not require the lost HTTP
// request or an in-memory transaction map. Re-publication stays on the TX topic.
func (t *TransactionRuntime) recover(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	var cursor uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, cancel := context.WithTimeout(ctx, 12*time.Second)
			// A single page per tick makes recovery latency grow by 15 seconds for
			// every historical page. Scan several bounded pages per maintenance job.
			for page := 0; page < 16 && job.Err() == nil; page++ {
				entries, next, err := t.S.R.HScan(job, transactionLedger, cursor, "*", 64).Result()
				if err == nil {
					for i := 0; i+1 < len(entries) && job.Err() == nil; i += 2 {
						var ev Event
						if json.Unmarshal([]byte(entries[i+1]), &ev) != nil {
							continue
						}
						if t.S.R.HGet(job, prefix(ev.MerchantID, ev.TemplateID)+"states", ev.ID).Val() != "pending" {
							continue
						}
						tr := t.S.R.HGet(job, prefix(ev.MerchantID, ev.TemplateID)+"trace", ev.ID).Val()
						if t.append(job, ev, "accepted", tr) != nil {
							continue
						}
						leased, err := t.S.R.SetNX(job, "fc:tx:retry:"+ev.ID, 1, 30*time.Second).Result()
						if err != nil || !leased {
							continue
						}
						recovered := restoreTrace(job, tr)
						recovered, end := t.S.stage(recovered, "transaction.recover")
						msg := primitive.NewMessage(transactionTopic, []byte(ev.ID))
						props := propagation.MapCarrier{}
						traceContext.Inject(recovered, props)
						for k, v := range props {
							msg.WithProperty(k, v)
						}
						_, err = t.P.SendMessageInTransaction(recovered, msg)
						end(err)
						t.S.Metrics.Retry.WithLabelValues("transaction_republish").Inc()
					}
					if job.Err() == nil {
						cursor = next
					}
				}
				if err != nil || cursor == 0 {
					break
				}
			}
			cancel()
		}
	}
}
