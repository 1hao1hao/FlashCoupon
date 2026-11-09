package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net"
	"strings"
	"time"
)

const topic = "FlashCouponGrant"

type Messaging struct {
	P rocketmq.Producer
	C rocketmq.PushConsumer
	S *Service
}

func (s *Service) StartMessaging(ctx context.Context) (*Messaging, error) {
	ns := strings.Split(s.Config.Nameserver, ",")
	for i, addr := range ns {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil || len(ips) == 0 {
			return nil, fmt.Errorf("resolve nameserver %s: %v", host, err)
		}
		ns[i] = net.JoinHostPort(ips[0], port)
	}
	m := &Messaging{S: s}
	var e error
	if s.Config.Role == "all" || s.Config.Role == "relay" {
		m.P, e = rocketmq.NewProducer(producer.WithGroupName("fc-relay"), producer.WithNsResolver(primitive.NewPassthroughResolver(ns)), producer.WithRetry(2))
		if e != nil {
			return nil, e
		}
		if e = m.P.Start(); e != nil {
			return nil, e
		}
		if e = s.R.XGroupCreateMkStream(ctx, stream, "relay", "0").Err(); e != nil && !strings.Contains(e.Error(), "BUSYGROUP") {
			m.P.Shutdown()
			return nil, e
		}
		go m.relay(ctx)
	}
	if s.Config.Role == "all" || s.Config.Role == "worker" {
		m.C, e = rocketmq.NewPushConsumer(consumer.WithGroupName("fc-workers"), consumer.WithNsResolver(primitive.NewPassthroughResolver(ns)), consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
		if e != nil {
			m.Close()
			return nil, e
		}
		e = m.C.Subscribe(topic, consumer.MessageSelector{}, func(c context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, msg := range msgs {
				var ev Event
				if e := json.Unmarshal(msg.Body, &ev); e != nil || ev.ID != eventIDValid(ev) {
					if err := s.R.XAdd(c, &redis.XAddArgs{Stream: "fc:deadletters", Values: map[string]any{"source": "rocketmq", "body": string(msg.Body), "reason": "invalid event"}}).Err(); err != nil {
						return consumer.ConsumeRetryLater, err
					}
					continue
				}
				if _, err := s.Deliver(c, ev, "async"); err != nil {
					slog.Error("worker retry", "id", ev.ID, "error", err)
					return consumer.ConsumeRetryLater, err
				}
			}
			return consumer.ConsumeSuccess, nil
		})
		if e != nil {
			m.Close()
			return nil, e
		}
		if e = m.C.Start(); e != nil {
			m.Close()
			return nil, e
		}
		if e = s.R.XGroupCreateMkStream(ctx, batchStream, "batch", "0").Err(); e != nil && !strings.Contains(e.Error(), "BUSYGROUP") {
			m.Close()
			return nil, e
		}
		go s.batchWorker(ctx)
	}
	return m, nil
}

// Events are trusted only if they match an atomically accepted reservation.
func eventIDValid(ev Event) string {
	if len(ev.ID) == 64 && ev.UserID > 0 && ev.MerchantID > 0 && ev.TemplateID != "" && ev.AcceptedAt > 0 {
		return ev.ID
	}
	return ""
}
func (m *Messaging) Close() {
	if m.C != nil {
		m.C.Shutdown()
	}
	if m.P != nil {
		m.P.Shutdown()
	}
}
func (m *Messaging) relay(ctx context.Context) {
	name := "relay-" + id()
	handle := func(msg redis.XMessage) {
		raw, ok := msg.Values["event"].(string)
		if !ok {
			if m.S.R.XAdd(ctx, &redis.XAddArgs{Stream: "fc:deadletters", Values: map[string]any{"source": "stream", "id": msg.ID, "reason": "missing event"}}).Err() == nil {
				m.S.R.XAck(ctx, stream, "relay", msg.ID)
			}
			return
		}
		sendCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		result, e := m.P.SendSync(sendCtx, primitive.NewMessage(topic, []byte(raw)))
		if e != nil {
			slog.Warn("relay send pending", "id", msg.ID, "error", e)
			return
		}
		if result.Status != primitive.SendOK {
			slog.Warn("broker did not confirm durable send", "id", msg.ID, "status", result.Status)
			return
		}
		if e = m.S.R.XAck(ctx, stream, "relay", msg.ID).Err(); e != nil {
			slog.Warn("relay ack retry", "error", e)
		}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		start := "0-0"
		for {
			msgs, next, e := m.S.R.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: "relay", Consumer: name, MinIdle: 10 * time.Second, Start: start, Count: 100}).Result()
			if e != nil {
				break
			}
			for _, msg := range msgs {
				handle(msg)
			}
			if next == "0-0" {
				break
			}
			start = next
		}
		rows, e := m.S.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: name, Streams: []string{stream, ">"}, Count: 100, Block: time.Second}).Result()
		if e != nil && e != redis.Nil && ctx.Err() == nil {
			slog.Warn("relay read", "error", e)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
		for _, r := range rows {
			for _, msg := range r.Messages {
				handle(msg)
			}
		}
	}
}

type Audit struct {
	MerchantID     int64  `json:"merchant_id"`
	TemplateID     string `json:"template_id"`
	Initial        int64  `json:"initial"`
	DBRemaining    int64  `json:"db_remaining"`
	Issued         int64  `json:"issued"`
	Projected      int64  `json:"projected"`
	RedisRemaining int64  `json:"redis_remaining"`
	Pending        int64  `json:"pending"`
	Failed         int64  `json:"failed"`
	Consistent     bool   `json:"consistent"`
	Error          string `json:"error,omitempty"`
}

func (s *Service) Reconcile(ctx context.Context, repair bool) ([]Audit, error) {
	ts, e := s.Store.Templates(ctx)
	if e != nil {
		return nil, e
	}
	out := []Audit{}
	for _, t := range ts {
		a := Audit{MerchantID: t.MerchantID, TemplateID: t.ID, Initial: t.Stock, DBRemaining: t.Remaining, RedisRemaining: -1}
		if repair {
			if e = s.RepairProjection(ctx, t); e != nil {
				return nil, e
			}
			if e = s.Store.ProjectTemplate(ctx, t); e != nil {
				return nil, e
			}
		}
		if e = s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE merchant_id=? AND template_id=? AND state='issued'", t.MerchantID, t.ID).Scan(&a.Issued); e != nil {
			return nil, e
		}
		if e = s.Store.Coupons.QueryRowContext(ctx, "SELECT COUNT(*) FROM coupons WHERE merchant_id=? AND template_id=?", t.MerchantID, t.ID).Scan(&a.Projected); e != nil {
			return nil, e
		}
		a.Consistent = a.Initial == a.DBRemaining+a.Issued && a.Issued == a.Projected
		if t.Mode == "async" {
			p := prefix(t.MerchantID, t.ID)
			a.RedisRemaining, e = s.R.Get(ctx, p+"stock").Int64()
			if e != nil {
				a.Error = "missing Redis inventory; restore AOF"
				a.Consistent = false
				out = append(out, a)
				continue
			}
			var cursor uint64
			revision := s.R.Get(ctx, p+"revision").Val()
			if revision == "" {
				revision = "0"
			}
			users := map[string]int64{}
			seen := map[string]bool{}
			for {
				entries, next, err := s.R.HScan(ctx, p+"reservations", cursor, "*", 200).Result()
				if err != nil {
					return nil, err
				}
				fields := make([]string, 0, len(entries)/2)
				for i := 0; i < len(entries); i += 2 {
					fields = append(fields, entries[i])
				}
				var states []interface{}
				if len(fields) > 0 {
					states, err = s.R.HMGet(ctx, p+"states", fields...).Result()
					if err != nil {
						return nil, err
					}
				}
				for i := 0; i < len(entries); i += 2 {
					if seen[entries[i]] {
						continue
					}
					seen[entries[i]] = true
					var ev Event
					if err = json.Unmarshal([]byte(entries[i+1]), &ev); err != nil {
						return nil, err
					}
					state, ok := states[i/2].(string)
					if !ok {
						return nil, fmt.Errorf("missing state for %s", ev.ID)
					}
					switch state {
					case "pending":
						a.Pending++
					case "failed":
						a.Failed++
					case "issued":
					default:
						return nil, fmt.Errorf("unknown reservation state %s", state)
					}
					if state != "failed" {
						users[formatInt(ev.UserID)]++
					}
					if repair && state == "pending" && time.Now().Unix()-ev.AcceptedAt >= 15 {
						// Re-publish through the same outbox; DB idempotency protects committed grants.
						ok, err := s.R.SetNX(ctx, "fc:retry:"+ev.ID, 1, 30*time.Second).Result()
						if err != nil {
							return nil, err
						}
						if ok {
							if err = s.R.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": entries[i+1]}}).Err(); err != nil {
								return nil, err
							}
						}
					}
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
			// Pending includes grants committed but not yet finalized. Count issued reservation states,
			// so the Redis accounting identity remains meaningful while the pipeline is in flight.
			total, e := s.R.HLen(ctx, p+"reservations").Result()
			if e != nil {
				return nil, e
			}
			if repair && t.Stock-int64(len(seen))+a.Failed >= 0 {
				expected := t.Stock - int64(len(seen)) + a.Failed
				n, err := repairInventory.Run(ctx, s.R, []string{p + "revision", p + "stock", p + "users", p + "meta"}, revision, expected, encode(users)).Int()
				if err != nil {
					return nil, err
				}
				if n == 1 {
					a.RedisRemaining = expected
				}
			}
			a.Consistent = a.Consistent && a.RedisRemaining+total-a.Failed == a.Initial
			if a.Pending == 0 {
				a.Consistent = a.Consistent && a.RedisRemaining == a.DBRemaining
			}
		}
		out = append(out, a)
	}
	return out, nil
}
func (s *Service) Maintenance(ctx context.Context) {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			job, cancel := context.WithTimeout(ctx, 12*time.Second)
			locked, _ := s.R.SetNX(job, "fc:maintenance", 1, 14*time.Second).Result()
			if locked {
				if _, e := s.Reconcile(job, true); e != nil {
					slog.Error("reconcile", "error", e)
				}
				if e := s.Store.Expire(job); e != nil {
					slog.Error("expire", "error", e)
				}
			}
			cancel()
		}
	}
}
