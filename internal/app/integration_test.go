package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/xuri/excelize/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("eventual condition timed out")
}
func TestIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("INTEGRATION=1 enables real MySQL/Redis/RocketMQ/optional Proxy tests")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, e := NewService(LoadConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Init(ctx); e != nil {
		t.Fatal(e)
	}
	messaging, e := s.StartMessaging(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer messaging.Close()
	router := s.Router()
	merchant := time.Now().UnixNano()%1000000000 + 1000
	create := func(mode string, stock, limit int64) Template {
		t.Helper()
		now := time.Now().Unix()
		v, e := s.Create(ctx, Template{MerchantID: merchant, Name: "integration", Stock: stock, PerUser: limit, Threshold: 1000, Discount: 200, StartsAt: now - 5, EndsAt: now + 600, ValidUntil: now + 1200, Mode: mode})
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	request := func(u int64, path string, body any) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(encode(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+SignToken(u, time.Now().Add(time.Hour).Unix(), s.Config.UserKey))
		router.ServeHTTP(w, req)
		return w
	}
	t.Run("concurrent_stock_and_duplicate_requests", func(t *testing.T) {
		template := create("async", 30, 1)
		var wg sync.WaitGroup
		var mu sync.Mutex
		accepted := map[string]Event{}
		unexpected := []string{}
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				u := int64(i + 1)
				if i < 50 {
					u = 1
				}
				w := request(u, "/v1/claims", map[string]any{"merchant_id": merchant, "template_id": template.ID, "request_id": "one"})
				mu.Lock()
				defer mu.Unlock()
				if w.Code == 202 {
					var res struct {
						Event Event `json:"event"`
					}
					if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
						unexpected = append(unexpected, err.Error())
					}
					accepted[res.Event.ID] = res.Event
				} else if w.Code != 409 {
					unexpected = append(unexpected, w.Body.String())
				}
			}(i)
		}
		wg.Wait()
		if len(unexpected) > 0 {
			t.Fatal(unexpected)
		}
		if len(accepted) != 30 {
			t.Fatalf("unique accepted=%d", len(accepted))
		}
		eventually(t, 90*time.Second, func() bool {
			var count int
			s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE merchant_id=? AND template_id=? AND state='issued'", merchant, template.ID).Scan(&count)
			var projected int
			s.Store.Coupons.QueryRowContext(ctx, "SELECT COUNT(*) FROM coupons WHERE merchant_id=? AND template_id=?", merchant, template.ID).Scan(&projected)
			return count == 30 && projected == 30 && s.R.Get(ctx, prefix(merchant, template.ID)+"stock").Val() == "0"
		})
		for _, ev := range accepted {
			for n := 0; n < 3; n++ {
				s.R.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": encode(ev)}})
			}
		}
		// Explicit delivery replays verify DB and projection idempotency without relying on timing.
		for _, ev := range accepted {
			if _, e = s.Deliver(ctx, ev, "async"); e != nil {
				t.Fatal(e)
			}
		}
		var total, users int
		s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*),COUNT(DISTINCT user_id) FROM grants WHERE merchant_id=? AND template_id=? AND state='issued'", merchant, template.ID).Scan(&total, &users)
		if total != 30 || users != 30 {
			t.Fatal(total, users)
		}
		t.Logf("stock=30, concurrent requests=200, unique accepted=%d, final issued=%d, unique users=%d", len(accepted), total, users)
	})
	t.Run("direct_settlement_redemption", func(t *testing.T) {
		template := create("direct", 10, 2)
		u := merchant + 10
		body := map[string]any{"merchant_id": merchant, "template_id": template.ID, "request_id": "direct-one"}
		w := request(u, "/v1/claims/direct", body)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		w = request(u, "/v1/claims/direct", body)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		cid := eventID(merchant, u, template.ID, "direct-one")
		offers, e := s.Settle(ctx, u, merchant, 1000)
		if e != nil || len(offers) != 1 {
			t.Fatal(offers, e)
		}
		var wg sync.WaitGroup
		codes := make(chan int, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				codes <- request(u, "/v1/coupons/"+cid+"/redeem", map[string]any{"merchant_id": merchant, "amount": 1000, "order_id": "order-1"}).Code
			}()
		}
		wg.Wait()
		close(codes)
		for c := range codes {
			if c != 200 {
				t.Fatal("idempotent redemption", c)
			}
		}
		w = request(u, "/v1/coupons/"+cid+"/redeem", map[string]any{"merchant_id": merchant, "amount": 1000, "order_id": "order-2"})
		if w.Code != 409 {
			t.Fatal(w.Code)
		}
		if w = request(u, "/v1/claims/direct", map[string]any{"merchant_id": merchant, "template_id": template.ID, "request_id": "direct-two"}); w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		second := eventID(merchant, u, template.ID, "direct-two")
		w = request(u, "/v1/coupons/"+second+"/redeem", map[string]any{"merchant_id": merchant, "amount": 1000, "order_id": "order-1"})
		if w.Code != 409 {
			t.Fatal("same order used two coupons", w.Code)
		}
		if os, err := s.Settle(ctx, u, merchant, 999); err != nil || len(os) != 0 {
			t.Fatal(os, err)
		}
	})
	t.Run("batch_streaming_dedup", func(t *testing.T) {
		template := create("async", 10, 1)
		file := excelize.NewFile()
		defer file.Close()
		rows := []any{"user_id", merchant + 101, merchant + 102, merchant + 101, merchant + 103}
		for i, v := range rows {
			file.SetCellValue("Sheet1", fmt.Sprintf("A%d", i+1), v)
		}
		buf, e := file.WriteToBuffer()
		if e != nil {
			t.Fatal(e)
		}
		b, e := s.Import(ctx, merchant, template.ID, buf)
		if e != nil {
			t.Fatal(e)
		}
		if b.Total != 3 {
			t.Fatal(b)
		}
		eventually(t, 90*time.Second, func() bool { b, e = s.Batch(ctx, b.ID); return e == nil && b.Processed == 3 && b.Accepted == 3 })
		eventually(t, 90*time.Second, func() bool {
			var count int
			s.Store.Coupons.QueryRowContext(ctx, "SELECT COUNT(*) FROM coupons WHERE merchant_id=? AND template_id=?", merchant, template.ID).Scan(&count)
			return count == 3
		})
		t.Logf("batch unique=%d accepted=%d rejected=%d", b.Total, b.Accepted, b.Rejected)
	})
	t.Run("compensation_once", func(t *testing.T) {
		template := create("async", 1, 1)
		// A permanent SQL rejection simulates a manually discovered inventory inconsistency.
		s.Store.DB.ExecContext(ctx, "UPDATE templates SET remaining=0 WHERE merchant_id=? AND id=?", merchant, template.ID)
		ev, e := s.Claim(ctx, merchant, merchant+200, template.ID, "compensate")
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 3; i++ {
			state, err := s.Deliver(ctx, ev, "async")
			if err != nil || state != "failed" {
				t.Fatal(state, err)
			}
		}
		if n := s.R.Get(ctx, prefix(merchant, template.ID)+"stock").Val(); n != "1" {
			t.Fatal("refund repeated", n)
		}
		if n := s.R.HGet(ctx, prefix(merchant, template.ID)+"users", formatInt(ev.UserID)).Val(); n != "0" {
			t.Fatal(n)
		}
		s.Store.DB.ExecContext(ctx, "UPDATE templates SET remaining=1 WHERE merchant_id=? AND id=?", merchant, template.ID)
	})
	t.Run("expiry_and_cache_loss", func(t *testing.T) {
		template := create("direct", 2, 1)
		u := merchant + 300
		ev := Event{ID: eventID(merchant, u, template.ID, "exp"), TemplateID: template.ID, MerchantID: merchant, UserID: u, AcceptedAt: time.Now().Unix()}
		if _, e = s.Deliver(ctx, ev, "direct"); e != nil {
			t.Fatal(e)
		}
		s.Store.Coupons.ExecContext(ctx, "UPDATE coupons SET expires_at=? WHERE user_id=? AND id=?", time.Now().Unix()-1, u, ev.ID)
		s.R.Del(ctx, couponKey(u, ev.ID), userIndex(u), userIndex(u)+":ready")
		if e = s.Store.Expire(ctx); e != nil {
			t.Fatal(e)
		}
		if _, e = s.Store.Redeem(ctx, u, merchant, ev.ID, "expired", 1000); e == nil {
			t.Fatal("redeemed expired coupon")
		}
		offers, e := s.Settle(ctx, u, merchant, 1000)
		if e != nil || len(offers) != 0 {
			t.Fatal(offers, e)
		}
	})
	t.Run("redis_inventory_loss_fails_closed", func(t *testing.T) {
		template := create("async", 1, 1)
		p := prefix(merchant, template.ID)
		s.R.Del(ctx, p+"meta", p+"stock")
		if e = s.Warm(ctx, template); e == nil {
			t.Fatal("silently reset lost inventory")
		}
		if _, e = s.Claim(ctx, merchant, 999, template.ID, "lost"); e == nil {
			t.Fatal("accepted on missing inventory")
		}
		bid := id()
		if _, err := s.Store.DB.ExecContext(ctx, "INSERT INTO batches(id,merchant_id,template_id,state) VALUES(?,?,?,'ready')", bid, merchant, template.ID); err != nil {
			t.Fatal(err)
		}
		msg := redis.XMessage{Values: map[string]interface{}{"batch": bid, "user": "999"}}
		if err := s.processBatch(ctx, msg); err == nil {
			t.Fatal("missing inventory was permanently acknowledged by batch processing")
		}
		var processed int
		if err := s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM batch_items WHERE batch_id=?", bid).Scan(&processed); err != nil || processed != 0 {
			t.Fatal("transient batch error must leave no terminal progress", processed, err)
		}
		// Restore this untouched fixture, never an in-flight real activity.
		s.Store.DB.ExecContext(ctx, "UPDATE templates SET redis_initialized=FALSE WHERE merchant_id=? AND id=?", merchant, template.ID)
		if e = s.Warm(ctx, template); e != nil {
			t.Fatal(e)
		}
		if err := s.processBatch(ctx, msg); err != nil {
			t.Fatal("batch did not resume after inventory restoration", err)
		}
	})
	t.Run("projection_and_inventory_repair", func(t *testing.T) {
		template := create("direct", 2, 1)
		u := merchant + 400
		ev := Event{ID: eventID(merchant, u, template.ID, "projection"), TemplateID: template.ID, MerchantID: merchant, UserID: u, AcceptedAt: time.Now().Unix()}
		if _, state, err := s.Store.Grant(ctx, ev, "direct"); err != nil || state != "issued" {
			t.Fatal(state, err)
		}
		if err := s.RepairProjection(ctx, template); err != nil {
			t.Fatal(err)
		}
		cs, err := s.Store.UserCoupons(ctx, u)
		if err != nil || len(cs) != 1 {
			t.Fatal(cs, err)
		}
		async := create("async", 5, 1)
		p := prefix(merchant, async.ID)
		s.R.Set(ctx, p+"stock", 99, 0)
		s.R.HSet(ctx, p+"users", "999", 5)
		if _, err = s.Reconcile(ctx, true); err != nil {
			t.Fatal(err)
		}
		if s.R.Get(ctx, p+"stock").Val() != "5" || s.R.HLen(ctx, p+"users").Val() != 0 {
			t.Fatal("inventory not repaired")
		}
		if err = s.SetStatus(ctx, merchant, async.ID, "paused"); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Claim(ctx, merchant, u, async.ID, "paused"); err == nil {
			t.Fatal("paused template accepted claim")
		}
		if err = s.SetStatus(ctx, merchant, async.ID, "active"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("int64_user_identity", func(t *testing.T) {
		template := create("async", 1, 1)
		u := int64(9007199254740993)
		ev, err := s.Claim(ctx, merchant, u, template.ID, "large-id")
		if err != nil || ev.UserID != u {
			t.Fatal("user ID rounded", ev, err)
		}
		state, err := s.Deliver(ctx, ev, "async")
		if err != nil || state != "issued" {
			t.Fatal(state, err)
		}
	})
	t.Run("final_audit", func(t *testing.T) {
		eventually(t, 90*time.Second, func() bool {
			as, e := s.Reconcile(ctx, false)
			if e != nil {
				t.Log(e)
				return false
			}
			for _, a := range as {
				if a.MerchantID == merchant && (!a.Consistent || a.Pending > 0) {
					return false
				}
			}
			return true
		})
		t.Log("all fixture templates reconcile: stock, ledger, projection and reservations")
	})
	cancel()
}
