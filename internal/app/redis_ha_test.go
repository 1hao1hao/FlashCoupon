package app

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"
)

func TestRedisHAConfiguration(t *testing.T) {
	for _, c := range []Config{{RedisMode: "invalid"}, {RedisMode: "sentinel"}, {RedisWaitReplicas: -1}, {RedisWaitReplicas: 1, RedisWaitTimeoutMS: 0}} {
		if r, e := newRedisClient(c); e == nil {
			r.Close()
			t.Fatal("unsafe configuration accepted")
		}
	}
	t.Setenv("REDIS_WAIT_REPLICAS", "oops")
	if _, e := newRedisClient(LoadConfig()); e == nil {
		t.Fatal("invalid WAIT silently disabled")
	}
}
func TestRedisHAObservedMasterConcurrent(t *testing.T) {
	s := &Service{Metrics: NewMetrics("test"), Config: Config{Role: "test"}}
	r := newRedisRuntime(s)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				r.observeMaster("first")
				r.observeMaster("second")
			}
		}()
	}
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.master == "" || r.verified == nil {
		t.Fatal("missing observation")
	}
}
func TestRedisHASameConnectionIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" || os.Getenv("REDIS_MODE") != "sentinel" {
		t.Skip("real Sentinel lab required")
	}
	cfg := LoadConfig()
	cfg.Role = "api"
	cfg.RedisWaitReplicas = 1
	cfg.RedisWaitTimeoutMS = 500
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().Unix()
	tpl, e := s.Create(ctx, Template{MerchantID: time.Now().UnixNano() % 1000000000000, Name: "same-connection-WAIT", Stock: 1, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 1, EndsAt: now + 600, ValidUntil: now + 900, Mode: "async"})
	if e != nil {
		t.Fatal(e)
	}
	ev := Event{ID: eventID(tpl.MerchantID, 123, tpl.ID, "pinned"), MerchantID: tpl.MerchantID, UserID: 123, TemplateID: tpl.ID}
	p := prefix(tpl.MerchantID, tpl.ID)
	keys := []string{p + "meta", p + "stock", p + "users", p + "reservations", p + "states", stream, p + "revision", p + "trace"}
	args := []any{ev.ID, "123", encode(ev), "{}", "async", tpl.Stock}
	c := s.R.Conn()
	defer c.Close()
	before, e := c.ClientID(ctx).Result()
	if e != nil {
		t.Fatal(e)
	}
	result, e := s.reserveOnConnection(ctx, c, keys, args)
	if e != nil {
		t.Fatal(e)
	}
	after, e := c.ClientID(ctx).Result()
	if e != nil || before != after {
		t.Fatal("WAIT socket changed", before, after, e)
	}
	var accepted Event
	if e = json.Unmarshal([]byte(result[1].(string)), &accepted); e != nil {
		t.Fatal(e)
	}
	// A replay on another pooled connection needs a new barrier and confirmations.
	c2 := s.R.Conn()
	defer c2.Close()
	replay, e := s.reserveOnConnection(ctx, c2, keys, args)
	if e != nil || replay[1] != result[1] {
		t.Fatal("replay changed reservation", e)
	}
	if n := s.R.HLen(ctx, p+"reservations").Val(); n != 1 {
		t.Fatal(n)
	}
	eventually(t, 30*time.Second, func() bool {
		a, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
		return e == nil && a[0].Consistent && a[0].Pending == 0 && a[0].Issued == 1
	})
}
