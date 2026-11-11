package app

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/cleanup.lua
var cleanupLua string
var cleanupScript = redis.NewScript(cleanupLua)

// Each invocation examines at most 128 entries and 8 groups. The group frontier
// and PEL checks run in the same Redis script as deletion, so concurrent Relay
// ACK, XAUTOCLAIM and cleaners cannot introduce a check/delete race.
func (s *Service) cleanOutbox(ctx context.Context, key, cursor string) (string, int64, error) {
	started := time.Now()
	defer func() { s.Metrics.CleanupTime.Observe(time.Since(started).Seconds()) }()
	result, err := cleanupScript.Run(ctx, s.R, []string{key}, cursor).Slice()
	if err != nil {
		s.Metrics.Cleanup.WithLabelValues("error").Inc()
		return cursor, 0, err
	}
	removed := result[1].(int64)
	s.Metrics.Cleanup.WithLabelValues("deleted").Add(float64(removed))
	s.Metrics.Cleanup.WithLabelValues("scanned").Add(float64(result[2].(int64)))
	s.Metrics.Cleanup.WithLabelValues("blocked").Add(float64(result[3].(int64)))
	return result[0].(string), removed, nil
}

func (s *Service) outboxCleaner(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	cursor := "0-0"
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, cancel := context.WithTimeout(ctx, 2*time.Second)
			next, _, err := s.cleanOutbox(job, stream, cursor)
			cancel()
			if err == nil {
				cursor = next
			}
		}
	}
}

func (s *Service) createOutboxGroup(ctx context.Context) error {
	err := s.R.XGroupCreateMkStream(ctx, stream, "relay", "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Test-only alternative transport. It deliberately shares Claim, Deliver,
// compensation and the same group lifecycle with the MQ pipeline. Never run
// alongside Relay; mode changes require stopping both consumers and draining.
func (m *Messaging) startStreamWorker(ctx context.Context) error {
	if err := m.S.createOutboxGroup(ctx); err != nil {
		return err
	}
	for i := 0; i < 20; i++ {
		m.start(func() {
			m.readOutbox(ctx, 1, func(msg redis.XMessage) {
				raw, ok := msg.Values["event"].(string)
				var ev Event
				if !ok || json.Unmarshal([]byte(raw), &ev) != nil || eventIDValid(ev) == "" {
					if m.S.R.XAdd(ctx, &redis.XAddArgs{Stream: "fc:deadletters", Values: map[string]any{"source": "stream-worker", "id": msg.ID, "reason": "invalid event"}}).Err() != nil {
						return
					}
				} else {
					job, cancel := context.WithTimeout(ctx, 30*time.Second)
					_, err := m.S.Deliver(job, ev, "async")
					cancel()
					if err != nil {
						m.S.Metrics.Retry.WithLabelValues("stream_worker").Inc()
						return
					}
				}
				if err := m.S.R.XAck(ctx, stream, "relay", msg.ID).Err(); err != nil {
					m.S.Metrics.Retry.WithLabelValues("ack").Inc()
				}
			})
		})
	}
	return nil
}

func (m *Messaging) readOutbox(ctx context.Context, count int64, handle func(redis.XMessage)) {
	name := "outbox-" + id()
	for ctx.Err() == nil {
		cursor := "0-0"
		for ctx.Err() == nil {
			msgs, next, err := m.S.R.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: "relay", Consumer: name, MinIdle: 10 * time.Second, Start: cursor, Count: count}).Result()
			if err != nil {
				break
			}
			for _, msg := range msgs {
				handle(msg)
			}
			if next == "0-0" {
				break
			}
			cursor = next
		}
		rows, err := m.S.R.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "relay", Consumer: name, Streams: []string{stream, ">"}, Count: count, Block: time.Second}).Result()
		if err != nil && err != redis.Nil && ctx.Err() == nil {
			m.S.Metrics.Retry.WithLabelValues("stream_read").Inc()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		for _, row := range rows {
			for _, msg := range row.Messages {
				handle(msg)
			}
		}
	}
}

func (m *Messaging) startWorkerTasks(ctx context.Context) error {
	if err := m.S.R.XGroupCreateMkStream(ctx, batchStream, "batch", "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("batch group: %w", err)
	}
	m.start(func() { m.S.batchWorker(ctx) })
	m.start(func() { m.S.Maintenance(ctx) })
	return nil
}
