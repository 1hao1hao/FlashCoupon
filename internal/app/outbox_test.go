package app

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type outboxSendFixture struct {
	rocketmq.Producer
	send func(context.Context) (*primitive.SendResult, error)
}

func (f outboxSendFixture) SendSync(ctx context.Context, _ ...*primitive.Message) (*primitive.SendResult, error) {
	return f.send(ctx)
}

// Real Redis + controlled Producer result verifies ACK ordering, including the
// crash window after confirmed send. Broker/process recovery is tested separately
// by scripts/faults.py and queue_experiment.py against the real Broker.
func TestRelayAcknowledgementOrder(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("real Redis required")
	}
	s, err := NewService(LoadConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err = s.createOutboxGroup(ctx); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"error", "unconfirmed", "confirmed-before-ack-crash", "confirmed"} {
		t.Run(outcome, func(t *testing.T) {
			mid, e := s.R.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": "ack-order-fixture"}}).Result()
			if e != nil {
				t.Fatal(e)
			}
			job, cancel := context.WithCancel(ctx)
			defer cancel()
			m := &Messaging{S: s}
			m.P = outboxSendFixture{send: func(context.Context) (*primitive.SendResult, error) {
				if outcome != "confirmed" {
					cancel()
				}
				if outcome == "error" {
					return nil, fmt.Errorf("send confirmation lost")
				}
				status := primitive.SendOK
				if outcome == "unconfirmed" {
					status = primitive.SendFlushDiskTimeout
				}
				return &primitive.SendResult{Status: status}, nil
			}}
			done := make(chan struct{})
			go func() { m.relay(job); close(done) }()
			if outcome == "confirmed" {
				eventually(t, 5*time.Second, func() bool { return s.R.XPending(ctx, stream, "relay").Val().Count == 0 })
				// Wait for this entry to have actually been delivered before checking ACK.
				eventually(t, 5*time.Second, func() bool {
					gs, _ := s.R.XInfoGroups(ctx, stream).Result()
					for _, g := range gs {
						if g.Name == "relay" && g.LastDeliveredID == mid {
							return s.R.XPending(ctx, stream, "relay").Val().Count == 0
						}
					}
					return false
				})
				cancel()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("relay failed to exit")
			}
			pending := s.R.XPending(ctx, stream, "relay").Val().Count
			if outcome == "confirmed" {
				if pending != 0 {
					t.Fatal("confirmed send not ACKed")
				}
			} else if pending != 1 {
				t.Fatal("unconfirmed/ACK-loss entry not retained", pending)
			}
			_, _, e = s.cleanOutbox(ctx, stream, "0-0")
			if e != nil {
				t.Fatal(e)
			}
			rows, _ := s.R.XRange(ctx, stream, mid, mid).Result()
			if outcome != "confirmed" && len(rows) != 1 {
				t.Fatal("cleanup lost unconfirmed event")
			}
			// Fixture cleanup only; no real business reservation exists for this payload.
			s.R.XAck(ctx, stream, "relay", mid)
			s.cleanOutbox(ctx, stream, "0-0")
		})
	}
}

func TestOutboxCleanupLifecycle(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("real Redis required")
	}
	ctx := context.Background()
	s, err := NewService(LoadConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	key := "fc:test-outbox:" + id()
	defer s.R.Del(ctx, key)
	add := func() string {
		v, e := s.R.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]any{"event": "fixture"}}).Result()
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	clean := func() int64 {
		_, n, e := s.cleanOutbox(ctx, key, "0-0")
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	first := add()
	if clean() != 0 {
		t.Fatal("deleted without a group")
	}
	if e := s.R.XGroupCreate(ctx, key, "relay", "0").Err(); e != nil {
		t.Fatal(e)
	}
	second := add()
	third := add()
	if clean() != 0 {
		t.Fatal("deleted unread messages")
	}
	if e := s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: "dead-relay", Streams: []string{key, ">"}, Count: 2}).Err(); e != nil {
		t.Fatal(e)
	}
	// A confirmed MQ send followed by a lost ACK is indistinguishable from an
	// unconfirmed send in Redis: both MUST remain in PEL and in the Stream.
	if clean() != 0 {
		t.Fatal("deleted pending/lost ACK")
	}
	if e := s.R.XGroupCreate(ctx, key, "observer", "0").Err(); e != nil {
		t.Fatal(e)
	}
	s.R.XAck(ctx, key, "relay", second)
	if clean() != 0 {
		t.Fatal("deleted while another group has unread data")
	}
	s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "observer", Consumer: "observer", Streams: []string{key, ">"}, Count: 3})
	s.R.XAck(ctx, key, "observer", second)
	if clean() != 1 {
		t.Fatal("ACKed out-of-order entry not reclaimed")
	}
	rows, _ := s.R.XRange(ctx, key, "-", "+").Result()
	if len(rows) != 2 || rows[0].ID != first || rows[1].ID != third {
		t.Fatal("unconfirmed payload lost", rows)
	}
	if _, _, e := s.R.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: key, Group: "relay", Consumer: "new-relay", MinIdle: time.Millisecond, Start: "0-0", Count: 10}).Result(); e != nil {
		t.Fatal(e)
	}
	s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: "new-relay", Streams: []string{key, ">"}, Count: 1})
	s.R.XAck(ctx, key, "relay", first, third)
	s.R.XAck(ctx, key, "observer", first, third)
	if clean() != 2 {
		t.Fatal("recovered ACKs not cleaned")
	}
	// Multiple Relay cleaners and ACKs racing. No payload may disappear before ACK.
	ids := []string{}
	for i := 0; i < 300; i++ {
		ids = append(ids, add())
	}
	for _, g := range []string{"relay", "observer"} {
		s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: g, Consumer: g, Streams: []string{key, ">"}, Count: 300})
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cursor := "0-0"
			for n := 0; n < 8; n++ {
				var e error
				cursor, _, e = s.cleanOutbox(ctx, key, cursor)
				if e != nil {
					t.Error(e)
				}
			}
		}()
	}
	for _, g := range []string{"relay", "observer"} {
		s.R.XAck(ctx, key, g, ids[1:]...)
	}
	wg.Wait()
	cursor := "0-0"
	for i := 0; i < 5; i++ {
		cursor, _, err = s.cleanOutbox(ctx, key, cursor)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, _ = s.R.XRange(ctx, key, "-", "+").Result()
	if len(rows) != 1 || rows[0].ID != ids[0] {
		t.Fatal("pending first entry not preserved", len(rows))
	}
	for _, g := range []string{"relay", "observer"} {
		s.R.XAck(ctx, key, g, ids[0])
	}
	if clean() != 1 {
		t.Fatal("last ACK not cleaned")
	}
	for i := 0; i < 8; i++ {
		s.R.XGroupCreate(ctx, key, "extra-"+id(), "0")
	}
	add()
	if clean() != 0 {
		t.Fatal("group bound must fail closed")
	}
}

func TestStreamWorkerRecovery(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("real middleware required")
	}
	cfg := LoadConfig()
	cfg.Role = "worker"
	cfg.TestStreamWorker = true
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.createOutboxGroup(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	tpl, err := s.Create(ctx, Template{MerchantID: now, Name: "stream-recovery", Stock: 2, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 1, EndsAt: now + 300, ValidUntil: now + 600, Mode: "async"})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := s.Claim(ctx, tpl.MerchantID, now, tpl.ID, "crash-after-deliver")
	if err != nil {
		t.Fatal(err)
	}
	// Deliver succeeded but process died before ACK. Cleaner must preserve PEL,
	// new Worker reclaims and replays the same transaction without double issuance.
	if err = s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: "crashed-worker", Streams: []string{stream, ">"}, Count: 10000}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Deliver(ctx, ev, "async"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, tpl.MerchantID, now+1, tpl.ID, "unread-after-crash"); err != nil {
		t.Fatal(err)
	}
	m, err := s.StartMessaging(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 45*time.Second, func() bool {
		rows, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
		return e == nil && len(rows) == 1 && rows[0].Consistent && rows[0].Pending == 0 && rows[0].Issued == 2 && s.R.XLen(ctx, stream).Val() == 0
	})
	cancel()
	m.Close()
	if n := s.R.HLen(context.Background(), prefix(tpl.MerchantID, tpl.ID)+"reservations").Val(); n != 2 {
		t.Fatal("cleanup removed recovery ledger", n)
	}
	// Event remains replayable from the reservation even with no queue payload.
	if _, err = s.Deliver(context.Background(), ev, "async"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReconcileFor(context.Background(), true, formatInt(tpl.MerchantID), tpl.ID); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileAfterOutboxDeletion(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("real middleware required")
	}
	cfg := LoadConfig()
	cfg.Role = "worker"
	cfg.TestStreamWorker = true
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.createOutboxGroup(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	tpl, err := s.Create(ctx, Template{MerchantID: now + 100, Name: "cleaned-outbox-recovery", Stock: 1, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 60, EndsAt: now + 300, ValidUntil: now + 600, Mode: "async"})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := s.Claim(ctx, tpl.MerchantID, now, tpl.ID, "lost-downstream")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: "confirmed-relay", Streams: []string{stream, ">"}, Count: 1}).Result()
	if err != nil || len(rows) != 1 || len(rows[0].Messages) != 1 {
		t.Fatal(rows, err)
	}
	mid := rows[0].Messages[0].ID
	// Simulate downstream loss after a legitimate handoff/ACK and cleanup. Aging
	// this isolated fixture avoids a wall-clock 15s sleep; no production clock or
	// admission logic is changed. The reservation is the sole replay payload.
	s.R.XAck(ctx, stream, "relay", mid)
	if _, n, e := s.cleanOutbox(ctx, stream, "0-0"); e != nil || n != 1 {
		t.Fatal("payload not cleaned", n, e)
	}
	ev.AcceptedAt = now - 20
	if err = s.R.HSet(ctx, prefix(tpl.MerchantID, tpl.ID)+"reservations", ev.ID, encode(ev)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReconcileFor(ctx, true, formatInt(tpl.MerchantID), tpl.ID); err != nil {
		t.Fatal(err)
	}
	if s.R.XLen(ctx, stream).Val() != 1 {
		t.Fatal("missing ledger-based republication")
	}
	m, err := s.StartMessaging(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() bool {
		a, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
		return e == nil && len(a) == 1 && a[0].Consistent && a[0].Issued == 1 && a[0].Pending == 0
	})
	cancel()
	m.Close()
}
