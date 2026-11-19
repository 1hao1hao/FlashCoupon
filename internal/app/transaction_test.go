package app

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTransactionBrokerIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" || os.Getenv("TX_EXPERIMENT_ENABLED") != "1" {
		t.Skip("real Broker and isolated transaction topic required")
	}
	cfg := LoadConfig()
	cfg.Role = "all"
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
	m, err := s.StartMessaging(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, test := range []struct {
		name              string
		unknown, rollback bool
		expected          int64
	}{{"commit", false, false, 1}, {"unknown_checkback", true, false, 1}, {"rollback", false, true, 0}} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TX_TEST_UNKNOWN", map[bool]string{true: "1", false: "0"}[test.unknown])
			t.Setenv("TX_TEST_ROLLBACK", map[bool]string{true: "1", false: "0"}[test.rollback])
			now := time.Now().Unix()
			tpl, err := s.Create(ctx, Template{MerchantID: time.Now().UnixNano()%1000000000 + 1, Name: test.name, Stock: 1, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 1, EndsAt: now + 600, ValidUntil: now + 900, Mode: "transaction"})
			if err != nil {
				t.Fatal(err)
			}
			ev, err := s.TransactionClaim(ctx, tpl.MerchantID, 123, tpl.ID, "same")
			if test.rollback {
				if err == nil {
					t.Fatal("rollback accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			eventually(t, 150*time.Second, func() bool {
				a, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
				return e == nil && a[0].Issued == test.expected && a[0].Projected == test.expected && a[0].Pending == 0 && a[0].Consistent
			})
			if !test.rollback {
				_, err = s.TransactionClaim(ctx, tpl.MerchantID, 123, tpl.ID, "same")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.Deliver(ctx, ev, "transaction"); err != nil {
					t.Fatal(err)
				}
				var count int
				if err = s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE id=?", ev.ID).Scan(&count); err != nil || count != 1 {
					t.Fatal(count, err)
				}
			}
			if test.unknown {
				families, _ := s.Metrics.Registry.Gather()
				found := false
				for _, f := range families {
					if f.GetName() == "flashcoupon_message_retries_total" {
						for _, metric := range f.Metric {
							for _, l := range metric.Label {
								if l.GetValue() == "transaction_checkback" && metric.Counter.GetValue() > 0 {
									found = true
								}
							}
						}
					}
				}
				if !found {
					t.Fatal("Unknown was not resolved by actual Broker checkback")
				}
			}
		})
	}
	t.Run("orphan_reservation_without_half_message", func(t *testing.T) {
		t.Setenv("TX_TEST_UNKNOWN", "0")
		t.Setenv("TX_TEST_ROLLBACK", "0")
		now := time.Now().Unix()
		tpl, err := s.Create(ctx, Template{MerchantID: time.Now().UnixNano()%1000000000 + 1, Name: "orphan-tx", Stock: 1, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 1, EndsAt: now + 600, ValidUntil: now + 900, Mode: "transaction"})
		if err != nil {
			t.Fatal(err)
		}
		// Build a durable accepted reservation with no Broker half-message and no
		// admission SQL fact. Only the background recovery scanner can publish it.
		ev, err := s.claimWithMode(ctx, tpl.MerchantID, 456, tpl.ID, "orphan", "transaction")
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, 90*time.Second, func() bool {
			a, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
			return e == nil && a[0].Issued == 1 && a[0].Projected == 1 && a[0].Pending == 0 && a[0].Consistent
		})
		var count int
		if err := s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM transaction_events WHERE id=? AND decision='accepted'", ev.ID).Scan(&count); err != nil || count != 1 {
			t.Fatal(count, err)
		}
	})
	t.Run("concurrent_admission_and_delivery", func(t *testing.T) {
		t.Setenv("TX_TEST_UNKNOWN", "0")
		t.Setenv("TX_TEST_ROLLBACK", "0")
		now := time.Now().Unix()
		tpl, err := s.Create(ctx, Template{MerchantID: time.Now().UnixNano()%1000000000 + 1, Name: "concurrent-tx", Stock: 100, PerUser: 1, Threshold: 1000, Discount: 100, StartsAt: now - 1, EndsAt: now + 600, ValidUntil: now + 900, Mode: "transaction"})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		sem := make(chan struct{}, 20)
		for u := int64(1000); u < 1100; u++ {
			wg.Add(1)
			go func(user int64) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if _, err := s.TransactionClaim(ctx, tpl.MerchantID, user, tpl.ID, "concurrent"); err != nil {
					t.Errorf("admission: %v", err)
				}
			}(u)
		}
		wg.Wait()
		eventually(t, 150*time.Second, func() bool {
			a, e := s.ReconcileFor(ctx, false, formatInt(tpl.MerchantID), tpl.ID)
			return e == nil && a[0].Issued == 100 && a[0].Projected == 100 && a[0].Pending == 0 && a[0].Consistent
		})
	})
}
