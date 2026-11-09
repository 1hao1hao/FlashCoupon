package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

// A revision CAS prevents a reconciliation snapshot from overwriting a concurrent claim/finalize.
var repairInventory = redis.NewScript(`
if (redis.call('GET',KEYS[1]) or '0')~=ARGV[1] then return 0 end
if redis.call('EXISTS',KEYS[2])==0 or redis.call('EXISTS',KEYS[4])==0 then return -1 end
local users=cjson.decode(ARGV[3]); redis.call('SET',KEYS[2],ARGV[2]); redis.call('DEL',KEYS[3])
for u,n in pairs(users) do redis.call('HSET',KEYS[3],u,n) end
redis.call('INCR',KEYS[1]); return 1`)

func (s *Service) RepairProjection(ctx context.Context, t Template) error {
	projected, err := s.Store.Coupons.QueryContext(ctx, "SELECT id FROM coupons WHERE merchant_id=? AND template_id=?", t.MerchantID, t.ID)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for projected.Next() {
		var cid string
		if err = projected.Scan(&cid); err != nil {
			projected.Close()
			return err
		}
		existing[cid] = true
	}
	err = projected.Err()
	projected.Close()
	if err != nil {
		return err
	}
	rows, e := s.Store.DB.QueryContext(ctx, "SELECT coupon_json FROM grants WHERE merchant_id=? AND template_id=? AND state='issued'", t.MerchantID, t.ID)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			return e
		}
		var c Coupon
		if e = json.Unmarshal(raw, &c); e != nil {
			return e
		}
		if !existing[c.ID] {
			if e = s.Store.ProjectCoupon(ctx, c); e != nil {
				return e
			}
			if e = s.CacheCoupon(ctx, c); e != nil {
				return e
			}
		}
	}
	return rows.Err()
}
func (s *Service) SetStatus(ctx context.Context, m int64, tid, status string) error {
	if status != "active" && status != "paused" {
		return fmt.Errorf("status must be active or paused")
	}
	t, e := s.Store.Template(ctx, m, tid)
	if e != nil {
		return e
	}
	p := prefix(m, tid)
	if t.Mode == "async" && status == "paused" {
		if s.R.Exists(ctx, p+"meta").Val() == 0 {
			return fmt.Errorf("inventory not initialized")
		}
		if e = s.R.HSet(ctx, p+"meta", "status", status).Err(); e != nil {
			return e
		}
	}
	if _, e = s.Store.DB.ExecContext(ctx, "UPDATE templates SET status=? WHERE merchant_id=? AND id=?", status, m, tid); e != nil {
		return e
	}
	if t.Mode == "async" && status == "active" {
		if s.R.Exists(ctx, p+"meta", p+"stock").Val() != 2 {
			return fmt.Errorf("inventory not initialized")
		}
		if e = s.R.HSet(ctx, p+"meta", "status", status).Err(); e != nil {
			return e
		}
	}
	s.R.Del(ctx, p+"cache")
	t.Status = status
	return s.Store.ProjectTemplate(ctx, t)
}
