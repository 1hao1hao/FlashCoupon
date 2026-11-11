package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

//go:embed lua/claim.lua
var claimLua string

//go:embed lua/finalize.lua
var finalizeLua string

//go:embed lua/warm.lua
var warmLua string

//go:embed lua/limit.lua
var limitLua string
var claimScript = redis.NewScript(claimLua)
var finalizeScript = redis.NewScript(finalizeLua)
var warmScript = redis.NewScript(warmLua)
var limitScript = redis.NewScript(limitLua)

const stream = "fc:outbox"

type Service struct {
	Store   *Store
	R       *redis.Client
	Config  Config
	Metrics *Metrics
}

func NewService(c Config) (*Service, error) {
	if c.Role != "api" && c.Role != "relay" && c.Role != "worker" && c.Role != "all" {
		return nil, fmt.Errorf("invalid ROLE %q", c.Role)
	}
	if c.TestStreamWorker && c.Role != "worker" && c.Role != "all" {
		return nil, fmt.Errorf("TEST_STREAM_WORKER requires worker role")
	}
	s, e := OpenStore(c)
	if e != nil {
		return nil, e
	}
	r := redis.NewClient(&redis.Options{Addr: c.RedisAddr, PoolSize: 100, PoolTimeout: 30 * time.Second})
	return &Service{Store: s, R: r, Config: c, Metrics: NewMetrics(c.Role)}, nil
}
func (s *Service) Close() { s.R.Close(); s.Store.DB.Close(); s.Store.Coupons.Close() }
func bloomOffsets(key string) []int64 {
	h := sha256.Sum256([]byte(key))
	v := []int64{}
	for i := 0; i < 4; i++ {
		v = append(v, int64(binary.BigEndian.Uint32(h[i*4:])%(1<<24)))
	}
	return v
}
func (s *Service) bloomAdd(ctx context.Context, key string) error {
	p := s.R.Pipeline()
	for _, o := range bloomOffsets(key) {
		p.SetBit(ctx, "fc:bloom", o, 1)
	}
	_, e := p.Exec(ctx)
	return e
}
func (s *Service) bloomMayExist(ctx context.Context, key string) bool {
	if s.R.Exists(ctx, "fc:bloom-ready").Val() == 0 {
		return true
	}
	p := s.R.Pipeline()
	cmds := []*redis.IntCmd{}
	for _, o := range bloomOffsets(key) {
		cmds = append(cmds, p.GetBit(ctx, "fc:bloom", o))
	}
	if _, e := p.Exec(ctx); e != nil {
		return true
	}
	for _, c := range cmds {
		if c.Val() == 0 {
			return false
		}
	}
	return true
}
func (s *Service) Init(ctx context.Context) error {
	if e := s.R.Ping(ctx).Err(); e != nil {
		return e
	}
	ts, e := s.Store.Templates(ctx)
	if e != nil {
		return e
	}
	// Do not clear the bitset: a concurrent create may have already added its bits.
	for _, t := range ts {
		if e = s.bloomAdd(ctx, prefix(t.MerchantID, t.ID)); e != nil {
			return e
		}
		if e = s.Store.ProjectTemplate(ctx, t); e != nil {
			return e
		}
	}
	return s.R.Set(ctx, "fc:bloom-ready", 1, 0).Err()
}
func (s *Service) Template(ctx context.Context, m int64, id string) (Template, error) {
	var t Template
	k := prefix(m, id)
	key := k + "cache"
	raw, e := s.R.Get(ctx, key).Result()
	if e == nil {
		if raw == "null" {
			return t, sql.ErrNoRows
		}
		e = json.Unmarshal([]byte(raw), &t)
		return t, e
	}
	if !s.bloomMayExist(ctx, k) {
		return t, sql.ErrNoRows
	}
	token := idToken()
	locked, e := s.R.SetNX(ctx, k+"lock", token, 3*time.Second).Result()
	if e != nil {
		return t, e
	}
	if !locked {
		select {
		case <-ctx.Done():
			return t, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return s.Template(ctx, m, id)
	}
	defer s.R.Eval(context.Background(), "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0", []string{k + "lock"}, token)
	t, e = s.Store.Template(ctx, m, id)
	if errors.Is(e, sql.ErrNoRows) {
		s.R.Set(ctx, key, "null", 5*time.Second)
		return t, e
	}
	if e != nil {
		return t, e
	}
	if e = s.R.Set(ctx, key, encode(t), 60*time.Second).Err(); e != nil {
		return t, e
	}
	return t, nil
}
func idToken() string { return id() }
func (s *Service) Create(ctx context.Context, t Template) (Template, error) {
	if e := t.Validate(); e != nil {
		return t, e
	}
	t.ID = id()
	t.Status = "active"
	t.Remaining = t.Stock
	// Add bits before the DB insert to prevent a Bloom false negative during creation.
	if e := s.bloomAdd(ctx, prefix(t.MerchantID, t.ID)); e != nil {
		return t, e
	}
	if e := s.Store.CreateTemplate(ctx, t); e != nil {
		return t, e
	}
	if e := s.Store.ProjectTemplate(ctx, t); e != nil {
		return t, fmt.Errorf("template %s saved; projection retry via reconcile: %w", t.ID, e)
	}
	if t.Mode == "async" {
		if e := s.Warm(ctx, t); e != nil {
			return t, e
		}
	}
	return t, nil
}
func (s *Service) Warm(ctx context.Context, t Template) error {
	var initialized bool
	if e := s.Store.DB.QueryRowContext(ctx, "SELECT redis_initialized FROM templates WHERE merchant_id=? AND id=?", t.MerchantID, t.ID).Scan(&initialized); e != nil {
		return e
	}
	p := prefix(t.MerchantID, t.ID)
	if initialized {
		if s.R.Exists(ctx, p+"meta", p+"stock").Val() != 2 {
			return fmt.Errorf("Redis state lost: restore AOF before reopening claims")
		}
		return nil
	}
	// Mark initialization durably before exposing Redis inventory. A crash between
	// these operations leaves admission closed, rather than permitting a fresh reset.
	if _, e := s.Store.DB.ExecContext(ctx, "UPDATE templates SET redis_initialized=TRUE WHERE merchant_id=? AND id=?", t.MerchantID, t.ID); e != nil {
		return e
	}
	n, e := warmScript.Run(ctx, s.R, []string{p + "meta", p + "stock", p + "reservations"}, t.Status, t.StartsAt, t.EndsAt, t.PerUser, t.Remaining).Int()
	if e != nil {
		return e
	}
	if n < 0 {
		return fmt.Errorf("partial Redis state; restore required")
	}
	return nil
}

type ClaimError struct {
	Code    int
	Message string
}

func (e *ClaimError) Error() string { return e.Message }
func (s *Service) Claim(ctx context.Context, m, u int64, tID, request string) (Event, error) {
	ev := Event{ID: eventID(m, u, tID, request), TemplateID: tID, MerchantID: m, UserID: u}
	t, e := s.Template(ctx, m, tID)
	if e != nil {
		return ev, e
	}
	if t.Mode != "async" {
		return ev, &ClaimError{4, "template requires direct endpoint"}
	}
	p := prefix(m, tID)
	res, e := claimScript.Run(ctx, s.R, []string{p + "meta", p + "stock", p + "users", p + "reservations", p + "states", stream, p + "revision"}, ev.ID, strconv.FormatInt(u, 10), encode(ev)).Slice()
	if e != nil {
		return ev, e
	}
	code := int(res[0].(int64))
	if code != 0 {
		return ev, &ClaimError{code, fmt.Sprint(res[1])}
	}
	e = json.Unmarshal([]byte(res[1].(string)), &ev)
	return ev, e
}
func (s *Service) Finalize(ctx context.Context, ev Event, state string) error {
	p := prefix(ev.MerchantID, ev.TemplateID)
	return finalizeScript.Run(ctx, s.R, []string{p + "states", p + "stock", p + "users", p + "revision"}, ev.ID, state, ev.UserID).Err()
}
func (s *Service) CacheCoupon(ctx context.Context, c Coupon) error {
	// Re-read projection, because a redelivered event may refer to an already redeemed coupon.
	actual, e := scanCoupon(s.Store.Coupons.QueryRowContext(ctx, "SELECT "+couponColumns+" FROM coupons WHERE user_id=? AND id=?", c.UserID, c.ID))
	if e != nil {
		return e
	}
	c = actual
	p := s.R.TxPipeline()
	p.Set(ctx, couponKey(c.UserID, c.ID), encode(c), 10*time.Minute)
	if c.Status == "available" && c.ExpiresAt > time.Now().Unix() {
		p.ZAdd(ctx, userIndex(c.UserID), redis.Z{Score: float64(c.ExpiresAt), Member: c.ID})
	} else {
		p.ZRem(ctx, userIndex(c.UserID), c.ID)
	}
	_, e = p.Exec(ctx)
	return e
}
func (s *Service) Deliver(ctx context.Context, ev Event, mode string) (state string, err error) {
	defer func() {
		outcome := state
		if err != nil {
			outcome = "error"
		}
		s.Metrics.Attempts.WithLabelValues(mode, outcome).Inc()
	}()
	if mode == "async" {
		raw, err := s.R.HGet(ctx, prefix(ev.MerchantID, ev.TemplateID)+"reservations", ev.ID).Result()
		if err != nil {
			return "", fmt.Errorf("reservation lookup: %w", err)
		}
		var original Event
		if err = json.Unmarshal([]byte(raw), &original); err != nil {
			return "", err
		}
		if original != ev {
			return "", fmt.Errorf("event does not match accepted reservation")
		}
	}
	start := time.Now()
	c, state, e := s.Store.Grant(ctx, ev, mode)
	s.Metrics.Stage.WithLabelValues("grant_transaction").Observe(time.Since(start).Seconds())
	if e != nil {
		return "", e
	}
	if state == "issued" {
		start = time.Now()
		e = s.Store.ProjectCoupon(ctx, c)
		s.Metrics.Stage.WithLabelValues("coupon_projection").Observe(time.Since(start).Seconds())
		if e != nil {
			return state, e
		}
		start = time.Now()
		e = s.CacheCoupon(ctx, c)
		s.Metrics.Stage.WithLabelValues("coupon_cache").Observe(time.Since(start).Seconds())
		if e != nil {
			return state, e
		}
	}
	if mode == "async" {
		e = s.Finalize(ctx, ev, state)
	}
	return state, e
}
