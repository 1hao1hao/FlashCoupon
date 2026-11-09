package app

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"sort"
	"sync"
	"time"
)

type Offer struct {
	CouponID string `json:"coupon_id"`
	Discount int64  `json:"discount"`
	Payable  int64  `json:"payable"`
}

func Offers(coupons []Coupon, m, amount, now int64) []Offer {
	n := len(coupons)
	workers := 8
	if n < workers {
		workers = n
	}
	out := []Offer{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			local := []Offer{}
			for i := start; i < n; i += workers {
				c := coupons[i]
				if c.MerchantID != m || c.Status != "available" || c.ExpiresAt <= now || amount < c.Threshold {
					continue
				}
				d := c.Discount
				if d > amount {
					d = amount
				}
				local = append(local, Offer{c.ID, d, amount - d})
			}
			mu.Lock()
			out = append(out, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Discount == out[j].Discount {
			return out[i].CouponID < out[j].CouponID
		}
		return out[i].Discount > out[j].Discount
	})
	return out
}
func (s *Service) Settle(ctx context.Context, u, m, amount int64) ([]Offer, error) {
	// A complete DB list fills the cache when cold. The ready marker only lives briefly,
	// so a late projection or cache loss is repaired automatically.
	ready := s.R.Exists(ctx, userIndex(u)+":ready").Val() > 0
	if !ready {
		cs, e := s.Store.UserCoupons(ctx, u)
		if e != nil {
			return nil, e
		}
		for _, c := range cs {
			if e = s.CacheCoupon(ctx, c); e != nil {
				return nil, e
			}
		}
		s.R.Set(ctx, userIndex(u)+":ready", 1, 30*time.Second)
	}
	now := time.Now().Unix()
	ids, e := s.R.ZRangeByScore(ctx, userIndex(u), &redis.ZRangeBy{Min: "(" + formatInt(now), Max: "+inf", Offset: 0, Count: 1000}).Result()
	if e != nil {
		return nil, e
	}
	p := s.R.Pipeline()
	cmds := make([]*redis.StringCmd, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, p.Get(ctx, couponKey(u, id)))
	}
	if len(cmds) > 0 {
		_, e = p.Exec(ctx)
		if e != nil && e != redis.Nil {
			return nil, e
		}
	}
	cs := []Coupon{}
	missing := false
	for _, cmd := range cmds {
		raw, err := cmd.Result()
		if err == redis.Nil {
			missing = true
			continue
		}
		if err != nil {
			return nil, err
		}
		var c Coupon
		if err = json.Unmarshal([]byte(raw), &c); err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	if missing {
		fresh, err := s.Store.UserCoupons(ctx, u)
		if err != nil {
			return nil, err
		}
		cs = fresh
		for _, c := range cs {
			if err = s.CacheCoupon(ctx, c); err != nil {
				return nil, err
			}
		}
	}
	return Offers(cs, m, amount, now), nil
}
