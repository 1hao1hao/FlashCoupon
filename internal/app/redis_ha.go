package app

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func splitAddresses(raw string) []string {
	var out []string
	for _, a := range strings.Split(raw, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

type redisConnectionHook struct{ r *RedisRuntime }

func (h redisConnectionHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err == nil {
			h.r.Connections.Inc()
		} else {
			h.r.Errors.WithLabelValues("dial").Inc()
		}
		return conn, err
	}
}
func (h redisConnectionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil && err != redis.Nil {
			h.r.Errors.WithLabelValues("command").Inc()
		}
		return err
	}
}
func (h redisConnectionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if err != nil && err != redis.Nil {
			h.r.Errors.WithLabelValues("pipeline").Inc()
		}
		return err
	}
}
func newRedisClient(c Config) (*redis.Client, error) {
	mode := c.RedisMode
	if mode == "" {
		mode = "standalone"
	}
	if mode != "standalone" && mode != "sentinel" {
		return nil, fmt.Errorf("invalid REDIS_MODE")
	}
	if c.RedisWaitReplicas < 0 || c.RedisWaitReplicas > 64 || (c.RedisWaitReplicas > 0 && (c.RedisWaitTimeoutMS < 1 || c.RedisWaitTimeoutMS > 30000)) {
		return nil, fmt.Errorf("invalid Redis WAIT configuration")
	}
	read := 3 * time.Second
	if wait := time.Duration(c.RedisWaitTimeoutMS)*time.Millisecond + time.Second; wait > read {
		read = wait
	}
	// No transparent replay of an ambiguous write or a pipeline on a new socket.
	if mode == "sentinel" {
		if len(c.SentinelAddrs) == 0 || c.RedisMasterName == "" {
			return nil, fmt.Errorf("sentinel addresses and master name required")
		}
		for _, a := range c.SentinelAddrs {
			if _, _, e := net.SplitHostPort(a); e != nil {
				return nil, fmt.Errorf("invalid sentinel address")
			}
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{MasterName: c.RedisMasterName, SentinelAddrs: c.SentinelAddrs, SentinelPassword: c.SentinelPassword, Password: c.RedisPassword, PoolSize: 100, PoolTimeout: 3 * time.Second, DialTimeout: time.Second, ReadTimeout: read, WriteTimeout: time.Second, MaxRetries: -1, ContextTimeoutEnabled: true}), nil
	}
	retries := 3
	if c.RedisWaitReplicas > 0 {
		retries = -1
	}
	return redis.NewClient(&redis.Options{Addr: c.RedisAddr, Password: c.RedisPassword, PoolSize: 100, PoolTimeout: 30 * time.Second, ReadTimeout: read, MaxRetries: retries, ContextTimeoutEnabled: true}), nil
}

type AdmissionError struct{ Outcome, EventID, Message string }

func (e *AdmissionError) Error() string { return e.Message }

type RedisRuntime struct {
	mu                                              sync.Mutex
	master                                          string
	masterAddress                                   string
	epoch                                           float64
	verified                                        map[string]string
	available                                       bool
	observed                                        bool
	Up, Replicas, WaitAcks, Writable                prometheus.Gauge
	Role, Lag, OffsetGap                            *prometheus.GaugeVec
	Switches, Reconnects, Connections, Incarnations prometheus.Counter
	Epoch                                           prometheus.Gauge
	Errors, Admissions, Waits                       *prometheus.CounterVec
	WaitDuration                                    prometheus.Histogram
}

func newRedisRuntime(s *Service) *RedisRuntime {
	r := &RedisRuntime{verified: map[string]string{}}
	r.Up = prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_redis_available", Help: "Last successful master probe (not a consistency guarantee)."})
	r.Writable = prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_redis_writable", Help: "Sampled master role and min-replica readiness; not an admission guarantee."})
	r.Connections = prometheus.NewCounter(prometheus.CounterOpts{Name: "flashcoupon_redis_connections_total", Help: "Successful main data-client TCP dials, excluding separate observers and Sentinel sockets."})
	r.Replicas = prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_redis_connected_replicas", Help: "Connected replicas from current master INFO."})
	r.WaitAcks = prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_redis_wait_last_acks", Help: "Last WAIT replica count, not disk durability."})
	r.Role = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "flashcoupon_redis_node_role", Help: "Observed data-node role, bounded node slot."}, []string{"node", "redis_role"})
	r.Lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "flashcoupon_redis_replica_io_age_seconds", Help: "Replica seconds since last master IO, not exact replication latency."}, []string{"node"})
	r.OffsetGap = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "flashcoupon_redis_replica_offset_gap_bytes", Help: "Approximate offset gap from sampled current master; invalid across replication histories."}, []string{"node"})
	r.Switches = prometheus.NewCounter(prometheus.CounterOpts{Name: "flashcoupon_redis_master_switches_total", Help: "Sentinel master address transitions at nondecreasing config epoch; per observer process."})
	r.Incarnations = prometheus.NewCounter(prometheus.CounterOpts{Name: "flashcoupon_redis_master_incarnations_total", Help: "Master run-id changes, including restart without promotion."})
	r.Epoch = prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_redis_sentinel_config_epoch", Help: "Last observed Sentinel config epoch; may be stale during outage."})
	r.Reconnects = prometheus.NewCounter(prometheus.CounterOpts{Name: "flashcoupon_redis_reconnections_total", Help: "Successful probe after failed probe; not TCP pool connection count."})
	r.Errors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_redis_errors_total", Help: "Bounded Redis error class."}, []string{"kind"})
	r.Admissions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_redis_admission_total", Help: "Redis admission outcomes; unknown is not rollback."}, []string{"outcome"})
	r.Waits = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_redis_wait_total", Help: "WAIT outcomes."}, []string{"outcome"})
	r.WaitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "flashcoupon_redis_wait_seconds", Help: "Barrier and WAIT duration on reservation socket.", Buckets: prometheus.ExponentialBuckets(.0005, 2, 16)})
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"role": s.Config.Role}, s.Metrics.Registry)
	reg.MustRegister(r.Up, r.Writable, r.Connections, r.Replicas, r.WaitAcks, r.Role, r.Lag, r.OffsetGap, r.Switches, r.Incarnations, r.Epoch, r.Reconnects, r.Errors, r.Admissions, r.Waits, r.WaitDuration)
	if s.R != nil {
		s.R.AddHook(redisConnectionHook{r})
	}
	return r
}
func redisFields(info string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			m[k] = v
		}
	}
	return m
}
func redisNumber(raw string) float64 { var f float64; fmt.Sscan(raw, &f); return f }
func (r *RedisRuntime) observeMaster(run string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run != "" && run != r.master {
		if r.master != "" {
			r.Incarnations.Inc()
		}
		r.master = run
		r.verified = map[string]string{}
	}
}
func (s *Service) redisFailure(outcome, id string, err error) error {
	kind := "other"
	msg := err.Error()
	switch {
	case strings.Contains(msg, "NOREPLICAS"):
		kind = "insufficient_replicas"
	case strings.Contains(msg, "READONLY"):
		kind = "readonly"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		kind = "timeout"
	case strings.Contains(msg, "EOF") || strings.Contains(msg, "connect") || strings.Contains(msg, "closed"):
		kind = "connection"
	}
	s.RedisHA.Errors.WithLabelValues(kind).Inc()
	s.RedisHA.Admissions.WithLabelValues(outcome).Inc()
	return &AdmissionError{outcome, id, "Redis admission " + outcome + "; retry the identical request_id; no automatic rollback"}
}
func (s *Service) guardMaster(ctx context.Context, c *redis.Conn, t Template) error {
	ctx, end := s.stage(ctx, "redis.ha_guard")
	var result error
	defer func() { end(result) }()
	raw, err := c.Info(ctx, "server", "replication").Result()
	if err != nil {
		result = err
		return err
	}
	fields := redisFields(raw)
	if fields["role"] != "master" {
		result = fmt.Errorf("READONLY master required")
		return result
	}
	run := fields["run_id"]
	if run == "" {
		result = fmt.Errorf("missing master identity")
		return result
	}
	s.RedisHA.observeMaster(run)
	// Full existing audit only once per template/master incarnation. An incomplete
	// ledger is never rebuilt from SQL remaining. Balanced lost history is unknowable.
	r := s.RedisHA
	r.mu.Lock()
	defer r.mu.Unlock()
	key := prefix(t.MerchantID, t.ID)
	if r.verified[key] != run {
		audits, e := s.ReconcileFor(ctx, false, formatInt(t.MerchantID), t.ID)
		if e != nil {
			result = e
			return e
		}
		if len(audits) != 1 || !audits[0].Consistent {
			result = fmt.Errorf("HA ledger audit incomplete; restore verified AOF")
			return result
		}
		if len(r.verified) > 4096 {
			r.verified = map[string]string{}
		}
		r.verified[key] = run
	}
	return nil
}
func (s *Service) reserveRedis(ctx context.Context, t Template, keys []string, args []any) ([]interface{}, error) {
	id := args[0].(string)
	if s.Config.RedisMode != "sentinel" && s.Config.RedisWaitReplicas == 0 {
		return claimScript.Run(ctx, s.R, keys, args...).Slice()
	}
	c := s.R.Conn()
	defer c.Close()
	if s.Config.RedisMode == "sentinel" {
		if err := s.guardMaster(ctx, c, t); err != nil {
			return nil, s.redisFailure("safe_rejected", id, err)
		}
	}
	return s.reserveOnConnection(ctx, c, keys, args)
}
func (s *Service) reserveOnConnection(ctx context.Context, c *redis.Conn, keys []string, args []any) ([]interface{}, error) {
	id := args[0].(string)
	result, err := c.Eval(ctx, claimLua, keys, args...).Slice()
	if err != nil {
		outcome := "unknown"
		if strings.Contains(err.Error(), "NOREPLICAS") || strings.Contains(err.Error(), "READONLY") {
			outcome = "safe_rejected"
		}
		return nil, s.redisFailure(outcome, id, err)
	}
	if result[0].(int64) != 0 {
		s.RedisHA.Admissions.WithLabelValues("safe_rejected").Inc()
		return result, nil
	}
	faultPoint(ctx, "after_redis_eval")
	if s.Config.RedisWaitReplicas > 0 {
		ctx, end := s.stage(ctx, "redis.wait")
		started := time.Now()
		// A fresh write is required even for idempotent Lua replay: WAIT on a fresh
		// connection would otherwise confirm offset zero, not the stored reservation.
		pipe := c.Pipeline()
		pipe.Set(ctx, "fc:ha:wait-barrier", idToken(), 0)
		wait := redis.NewIntCmd(ctx, "WAIT", s.Config.RedisWaitReplicas, s.Config.RedisWaitTimeoutMS)
		_ = pipe.Process(ctx, wait)
		_, err = pipe.Exec(ctx)
		acks := wait.Val()
		s.RedisHA.WaitAcks.Set(float64(acks))
		s.RedisHA.WaitDuration.Observe(time.Since(started).Seconds())
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("redis.wait.requested", s.Config.RedisWaitReplicas), attribute.Int64("redis.wait.acks", acks))
		outcome := "confirmed"
		if err != nil {
			outcome = "error"
		} else if acks < int64(s.Config.RedisWaitReplicas) {
			outcome = "timeout"
			err = fmt.Errorf("WAIT replica acknowledgement timeout")
		}
		s.RedisHA.Waits.WithLabelValues(outcome).Inc()
		end(err)
		if err != nil {
			return nil, s.redisFailure("unknown", id, err)
		}
	}
	s.RedisHA.Admissions.WithLabelValues("accepted").Inc()
	return result, nil
}
func (s *Service) observeRedis(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	clients := make([]*redis.Client, len(s.Config.RedisObserveAddrs))
	var sentinels []*redis.SentinelClient
	if s.Config.RedisMode == "sentinel" {
		for _, addr := range s.Config.SentinelAddrs {
			c := redis.NewSentinelClient(&redis.Options{Addr: addr, Password: s.Config.SentinelPassword, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, MaxRetries: -1, ContextTimeoutEnabled: true})
			sentinels = append(sentinels, c)
			defer c.Close()
		}
	}
	for i, addr := range s.Config.RedisObserveAddrs {
		clients[i] = redis.NewClient(&redis.Options{Addr: addr, Password: s.Config.RedisPassword, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: -1, ContextTimeoutEnabled: true})
		defer clients[i].Close()
	}
	for {
		job, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
		raw, err := s.R.Info(job, "server", "replication").Result()
		f := redisFields(raw)
		up := err == nil && f["role"] == "master"
		r := s.RedisHA
		r.mu.Lock()
		if up && r.observed && !r.available {
			r.Reconnects.Inc()
		}
		r.available = up
		r.observed = true
		r.mu.Unlock()
		if up {
			r.Up.Set(1)
			writable := s.Config.RedisMode != "sentinel" || redisNumber(f["min_slaves_good_slaves"]) >= 1
			if writable {
				r.Writable.Set(1)
			} else {
				r.Writable.Set(0)
			}
			r.Replicas.Set(redisNumber(f["connected_slaves"]))
			r.observeMaster(f["run_id"])
		} else {
			r.Writable.Set(0)
			r.Up.Set(0)
			r.Replicas.Set(0)
			r.Errors.WithLabelValues("probe").Inc()
		}
		for i, client := range clients {
			node := fmt.Sprintf("node_%d", i)
			raw, e := client.Info(job, "replication").Result()
			n := redisFields(raw)
			for _, role := range []string{"master", "slave", "unavailable"} {
				value := 0.
				if (e != nil && role == "unavailable") || (e == nil && n["role"] == role) {
					value = 1
				}
				r.Role.WithLabelValues(node, role).Set(value)
			}
			lag := -1.
			gap := -1.
			if e == nil && n["role"] == "slave" {
				lag = redisNumber(n["master_last_io_seconds_ago"])
				gap = redisNumber(f["master_repl_offset"]) - redisNumber(n["slave_repl_offset"])
				if !up || gap < 0 {
					gap = -1
				}
			}
			r.Lag.WithLabelValues(node).Set(lag)
			r.OffsetGap.WithLabelValues(node).Set(gap)
		}
		for _, sentinel := range sentinels {
			master, e := sentinel.Master(job, s.Config.RedisMasterName).Result()
			if e != nil {
				continue
			}
			address := net.JoinHostPort(master["ip"], master["port"])
			epoch := redisNumber(master["config-epoch"])
			r.mu.Lock()
			if epoch >= r.epoch {
				if r.masterAddress != "" && r.masterAddress != address {
					r.Switches.Inc()
				}
				r.masterAddress = address
				r.epoch = epoch
				r.Epoch.Set(epoch)
			}
			r.mu.Unlock()
			break
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
