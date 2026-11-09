package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type Store struct{ DB, Coupons *sql.DB }

func OpenStore(c Config) (*Store, error) {
	db, e := sql.Open("mysql", c.ControlDSN)
	if e != nil {
		return nil, e
	}
	cp, e := sql.Open("mysql", c.CouponDSN)
	if e != nil {
		db.Close()
		return nil, e
	}
	for _, d := range []*sql.DB{db, cp} {
		d.SetMaxOpenConns(40)
		d.SetMaxIdleConns(10)
		d.SetConnMaxLifetime(3 * time.Minute)
	}
	s := &Store{db, cp}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e = db.PingContext(ctx); e != nil {
		return nil, e
	}
	if e = cp.PingContext(ctx); e != nil {
		return nil, e
	}
	return s, nil
}

const templateColumns = "id,merchant_id,name,stock,remaining,per_user,threshold_amount,discount,starts_at,ends_at,valid_until,mode,status"

type scanner interface{ Scan(...any) error }

func scanTemplate(r scanner) (t Template, e error) {
	e = r.Scan(&t.ID, &t.MerchantID, &t.Name, &t.Stock, &t.Remaining, &t.PerUser, &t.Threshold, &t.Discount, &t.StartsAt, &t.EndsAt, &t.ValidUntil, &t.Mode, &t.Status)
	return
}
func (s *Store) Template(ctx context.Context, m int64, id string) (Template, error) {
	return scanTemplate(s.DB.QueryRowContext(ctx, "SELECT "+templateColumns+" FROM templates WHERE merchant_id=? AND id=?", m, id))
}
func (s *Store) CreateTemplate(ctx context.Context, t Template) error {
	_, e := s.DB.ExecContext(ctx, "INSERT INTO templates ("+templateColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", t.ID, t.MerchantID, t.Name, t.Stock, t.Stock, t.PerUser, t.Threshold, t.Discount, t.StartsAt, t.EndsAt, t.ValidUntil, t.Mode, t.Status)
	return e
}
func (s *Store) ProjectTemplate(ctx context.Context, t Template) error {
	_, e := s.Coupons.ExecContext(ctx, "INSERT INTO merchant_templates ("+templateColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),status=VALUES(status),remaining=VALUES(remaining)", t.ID, t.MerchantID, t.Name, t.Stock, t.Remaining, t.PerUser, t.Threshold, t.Discount, t.StartsAt, t.EndsAt, t.ValidUntil, t.Mode, t.Status)
	return e
}
func (s *Store) Templates(ctx context.Context) ([]Template, error) {
	rows, e := s.DB.QueryContext(ctx, "SELECT "+templateColumns+" FROM templates")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Template{}
	for rows.Next() {
		t, e := scanTemplate(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Grant is the authoritative local transaction. Projection failure never refunds an issued grant.
// All contenders lock the template, then check idempotency and user limits under that lock.
func (s *Store) Grant(ctx context.Context, ev Event, mode string) (Coupon, string, error) {
	var c Coupon
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return c, "", e
	}
	defer tx.Rollback()
	t, e := scanTemplate(tx.QueryRowContext(ctx, "SELECT "+templateColumns+" FROM templates WHERE merchant_id=? AND id=? FOR UPDATE", ev.MerchantID, ev.TemplateID))
	if e != nil {
		return c, "", e
	}
	var state, reason string
	var raw []byte
	e = tx.QueryRowContext(ctx, "SELECT state,reason,coupon_json FROM grants WHERE id=?", ev.ID).Scan(&state, &reason, &raw)
	if e == nil {
		if state == "issued" {
			e = json.Unmarshal(raw, &c)
		}
		return c, state, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return c, "", e
	}
	var count int64
	e = tx.QueryRowContext(ctx, "SELECT issued FROM user_limits WHERE merchant_id=? AND template_id=? AND user_id=?", ev.MerchantID, ev.TemplateID, ev.UserID).Scan(&count)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return c, "", e
	}
	reason = ""
	switch {
	case t.Mode != mode:
		reason = "wrong issuance mode"
	case ev.UserID <= 0:
		reason = "invalid user"
	case ev.AcceptedAt < t.StartsAt || ev.AcceptedAt >= t.EndsAt:
		reason = "outside activity"
	case t.Remaining <= 0:
		reason = "database stock exhausted"
	case count >= t.PerUser:
		reason = "database user limit"
	case mode == "direct" && t.Status != "active":
		reason = "inactive"
	}
	if reason != "" {
		_, e = tx.ExecContext(ctx, "INSERT INTO grants(id,merchant_id,template_id,user_id,state,reason,accepted_at) VALUES(?,?,?,?,'failed',?,?)", ev.ID, ev.MerchantID, ev.TemplateID, ev.UserID, reason, ev.AcceptedAt)
		if e != nil {
			return c, "", e
		}
		return c, "failed", tx.Commit()
	}
	c = Coupon{ID: ev.ID, TemplateID: t.ID, MerchantID: t.MerchantID, UserID: ev.UserID, Threshold: t.Threshold, Discount: t.Discount, ExpiresAt: t.ValidUntil, Status: "available"}
	if _, e = tx.ExecContext(ctx, "UPDATE templates SET remaining=remaining-1 WHERE merchant_id=? AND id=? AND remaining>0", t.MerchantID, t.ID); e != nil {
		return c, "", e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO user_limits(merchant_id,template_id,user_id,issued) VALUES(?,?,?,1) ON DUPLICATE KEY UPDATE issued=issued+1", t.MerchantID, t.ID, ev.UserID); e != nil {
		return c, "", e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO grants(id,merchant_id,template_id,user_id,state,coupon_json,accepted_at) VALUES(?,?,?,?,'issued',?,?)", ev.ID, t.MerchantID, t.ID, ev.UserID, encode(c), ev.AcceptedAt); e != nil {
		return c, "", e
	}
	return c, "issued", tx.Commit()
}
func (s *Store) ProjectCoupon(ctx context.Context, c Coupon) error {
	_, e := s.Coupons.ExecContext(ctx, "INSERT INTO coupons(user_id,id,merchant_id,template_id,threshold_amount,discount,expires_at,status) VALUES(?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=id", c.UserID, c.ID, c.MerchantID, c.TemplateID, c.Threshold, c.Discount, c.ExpiresAt, c.Status)
	return e
}
func scanCoupon(r scanner) (c Coupon, e error) {
	var order sql.NullString
	e = r.Scan(&c.ID, &c.TemplateID, &c.MerchantID, &c.UserID, &c.Threshold, &c.Discount, &c.ExpiresAt, &c.Status, &order)
	c.OrderID = order.String
	return
}

const couponColumns = "id,template_id,merchant_id,user_id,threshold_amount,discount,expires_at,status,order_id"

func (s *Store) UserCoupons(ctx context.Context, u int64) ([]Coupon, error) {
	rows, e := s.Coupons.QueryContext(ctx, "SELECT "+couponColumns+" FROM coupons WHERE user_id=? ORDER BY id LIMIT 1000", u)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Coupon{}
	for rows.Next() {
		c, e := scanCoupon(rows)
		if e != nil {
			return nil, e
		}
		if c.Status == "available" && c.ExpiresAt <= time.Now().Unix() {
			c.Status = "expired"
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) Redeem(ctx context.Context, u, m int64, cid, order string, amount int64) (Coupon, error) {
	c, e := scanCoupon(s.Coupons.QueryRowContext(ctx, "SELECT "+couponColumns+" FROM coupons WHERE user_id=? AND id=?", u, cid))
	if e != nil {
		return c, e
	}
	if c.MerchantID != m {
		return c, fmt.Errorf("merchant mismatch")
	}
	if c.Status == "used" && c.OrderID == order {
		return c, nil
	}
	if amount < c.Threshold {
		return c, fmt.Errorf("threshold not met")
	}
	r, e := s.Coupons.ExecContext(ctx, "UPDATE coupons SET status='used',order_id=? WHERE user_id=? AND id=? AND merchant_id=? AND status='available' AND expires_at>? AND threshold_amount<=?", order, u, cid, m, time.Now().Unix(), amount)
	if e != nil {
		return c, e
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		c, e = scanCoupon(s.Coupons.QueryRowContext(ctx, "SELECT "+couponColumns+" FROM coupons WHERE user_id=? AND id=?", u, cid))
		if e == nil && c.Status == "used" && c.OrderID == order {
			return c, nil
		}
		return c, fmt.Errorf("coupon unavailable")
	}
	c.Status = "used"
	c.OrderID = order
	return c, nil
}
func (s *Store) Expire(ctx context.Context) error {
	_, e := s.Coupons.ExecContext(ctx, "UPDATE coupons SET status='expired' WHERE status='available' AND expires_at<=?", time.Now().Unix())
	return e
}
