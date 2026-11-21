package app

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	RedisMode, RedisPassword, SentinelPassword, RedisMasterName                 string
	SentinelAddrs, RedisObserveAddrs                                            []string
	RedisWaitReplicas, RedisWaitTimeoutMS                                       int
	TestDisableCleanup                                                          bool
	ControlDSN, CouponDSN, RedisAddr, Nameserver, Addr, AdminKey, UserKey, Role string
	TestStreamWorker                                                            bool
	Rate                                                                        int
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func LoadConfig() Config {
	r, _ := strconv.Atoi(env("RATE_LIMIT", "5000"))
	w, we := strconv.Atoi(env("REDIS_WAIT_REPLICAS", "0"))
	if we != nil {
		w = -1
	}
	timeout, te := strconv.Atoi(env("REDIS_WAIT_TIMEOUT_MS", "200"))
	if te != nil {
		timeout = -1
	}
	return Config{
		RedisMode: env("REDIS_MODE", "standalone"), RedisPassword: os.Getenv("REDIS_PASSWORD"), SentinelPassword: os.Getenv("REDIS_SENTINEL_PASSWORD"), RedisMasterName: env("REDIS_MASTER_NAME", "flashcoupon"), SentinelAddrs: splitAddresses(os.Getenv("REDIS_SENTINEL_ADDRS")), RedisObserveAddrs: splitAddresses(os.Getenv("REDIS_OBSERVE_ADDRS")), RedisWaitReplicas: w, RedisWaitTimeoutMS: timeout,
		ControlDSN: env("CONTROL_DSN", "root:flashcoupon@tcp(localhost:3306)/control?parseTime=true&loc=UTC"),
		CouponDSN:  env("COUPON_DSN", "root:flashcoupon@tcp(localhost:3306)/control?parseTime=true&loc=UTC"),
		RedisAddr:  env("REDIS_ADDR", "localhost:6379"), Nameserver: env("MQ_NAMESERVER", "localhost:9876"),
		Addr: env("HTTP_ADDR", ":8080"), AdminKey: env("ADMIN_KEY", "dev-admin-key"), UserKey: env("USER_KEY", "dev-user-key"), Role: env("ROLE", "all"), TestStreamWorker: env("TEST_STREAM_WORKER", "0") == "1", TestDisableCleanup: env("TEST_DISABLE_OUTBOX_CLEANUP", "0") == "1", Rate: r}
}

type Template struct {
	ID         string `json:"id"`
	MerchantID int64  `json:"merchant_id"`
	Name       string `json:"name"`
	Stock      int64  `json:"stock"`
	Remaining  int64  `json:"remaining"`
	PerUser    int64  `json:"per_user"`
	Threshold  int64  `json:"threshold"`
	Discount   int64  `json:"discount"`
	StartsAt   int64  `json:"starts_at"`
	EndsAt     int64  `json:"ends_at"`
	ValidUntil int64  `json:"valid_until"`
	Mode       string `json:"mode"`
	Status     string `json:"status"`
}

func (t Template) Validate() error {
	if t.MerchantID <= 0 || t.Name == "" || len(t.Name) > 120 || t.Stock <= 0 || t.Stock > 10000000 || t.PerUser < 1 || t.PerUser > 100 || t.Discount <= 0 || t.Threshold < 0 || t.Discount > 100000000000 || t.Threshold > 100000000000 {
		return fmt.Errorf("invalid template fields (money is integer cents)")
	}
	if t.EndsAt <= t.StartsAt || t.ValidUntil < t.EndsAt || t.EndsAt <= time.Now().Unix() {
		return fmt.Errorf("invalid activity/validity time")
	}
	if t.Mode != "async" && t.Mode != "direct" && !(t.Mode == "transaction" && env("TX_EXPERIMENT_ENABLED", "0") == "1") {
		return fmt.Errorf("mode must be async or direct")
	}
	return nil
}

type Event struct {
	ID         string `json:"id"`
	TemplateID string `json:"template_id"`
	MerchantID int64  `json:"merchant_id"`
	UserID     int64  `json:"user_id"`
	AcceptedAt int64  `json:"accepted_at"`
}
type Coupon struct {
	ID         string `json:"id"`
	TemplateID string `json:"template_id"`
	MerchantID int64  `json:"merchant_id"`
	UserID     int64  `json:"user_id"`
	Threshold  int64  `json:"threshold"`
	Discount   int64  `json:"discount"`
	ExpiresAt  int64  `json:"expires_at"`
	Status     string `json:"status"`
	OrderID    string `json:"order_id,omitempty"`
}

func eventID(m, u int64, t, r string) string {
	s := sha256.Sum256([]byte(fmt.Sprintf("%d/%d/%s/%s", m, u, t, r)))
	return hex.EncodeToString(s[:])
}
func id() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func encode(v any) string                 { b, _ := json.Marshal(v); return string(b) }
func prefix(m int64, t string) string     { return fmt.Sprintf("fc:t:%d:%s:", m, t) }
func userIndex(u int64) string            { return fmt.Sprintf("fc:u:%d:coupons", u) }
func couponKey(u int64, id string) string { return fmt.Sprintf("fc:u:%d:c:%s", u, id) }
