package app

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type Metrics struct {
	Forward           *prometheus.CounterVec
	Cleanup           *prometheus.CounterVec
	CleanupTime       prometheus.Histogram
	OutboxEntries     prometheus.Gauge
	OutboxBytes       prometheus.Gauge
	Registry          *prometheus.Registry
	HTTP              *prometheus.CounterVec
	Latency           *prometheus.HistogramVec
	Claims            *prometheus.CounterVec
	Attempts          *prometheus.CounterVec
	Retry             *prometheus.CounterVec
	Stage             *prometheus.HistogramVec
	Ledger            *prometheus.GaugeVec
	Projected         prometheus.Gauge
	Backlog           *prometheus.GaugeVec
	Errors            *prometheus.CounterVec
	ReconcileMismatch prometheus.Gauge
	ReconcileTime     prometheus.Gauge
	SnapshotTime      prometheus.Gauge
}

func NewMetrics(role string) *Metrics {
	r := prometheus.NewRegistry()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"role": role}, r)
	m := &Metrics{Registry: r,
		Forward:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_relay_forward_total", Help: "MQ send attempts by bounded outcome; confirmed includes duplicates."}, []string{"outcome"}),
		Cleanup:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_outbox_cleanup_total", Help: "Bounded cleanup scanned/deleted entries, blocked calls and errors."}, []string{"outcome"}),
		CleanupTime:       prometheus.NewHistogram(prometheus.HistogramOpts{Name: "flashcoupon_outbox_cleanup_seconds", Help: "Cleanup round duration including Redis wait.", Buckets: prometheus.DefBuckets}),
		OutboxEntries:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_outbox_entries", Help: "Physical entries including ACKed entries awaiting cleanup."}),
		OutboxBytes:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_outbox_memory_bytes", Help: "Redis MEMORY USAGE of Outbox, excluding reservation ledger."}),
		HTTP:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_http_requests_total", Help: "HTTP responses by bounded route/method/status."}, []string{"route", "method", "status"}),
		Latency:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "flashcoupon_http_duration_seconds", Help: "HTTP latency; use histogram_quantile for P95/P99.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}}, []string{"route", "method"}),
		Claims:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_claim_responses_total", Help: "HTTP claim responses, including idempotent replay; not unique issuance."}, []string{"mode", "outcome"}),
		Attempts:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_delivery_attempts_total", Help: "Delivery attempts, including replay."}, []string{"mode", "outcome"}),
		Retry:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_message_retries_total", Help: "Failed send/consume/ack attempts eligible for retry."}, []string{"stage"}),
		Stage:             prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "flashcoupon_delivery_stage_seconds", Help: "Delivery stage latency including database lock wait.", Buckets: prometheus.ExponentialBuckets(.001, 2, 16)}, []string{"stage"}),
		Ledger:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "flashcoupon_grants", Help: "Durable unique MySQL grant facts by terminal state; worker only, survives process restart."}, []string{"state"}),
		Projected:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_projected_coupons", Help: "Unique coupons persisted in user shards; worker only."}),
		Backlog:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "flashcoupon_message_backlog", Help: "Redis consumer-group pending/unread counts and last reconciliation end-to-end pending."}, []string{"stage", "kind"}),
		Errors:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "flashcoupon_background_errors_total", Help: "Background errors by bounded operation."}, []string{"operation"}),
		ReconcileMismatch: prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_reconcile_mismatches", Help: "Last reconciliation snapshot mismatches; may be transient under concurrent writes."}),
		ReconcileTime:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_reconcile_last_success_timestamp_seconds", Help: "Last completed reconciliation timestamp."}),
		SnapshotTime:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "flashcoupon_metrics_last_success_timestamp_seconds", Help: "Last successful backlog/ledger collection; use to detect stale gauges."}),
	}
	reg.MustRegister(m.Forward, m.Cleanup, m.CleanupTime, m.OutboxEntries, m.OutboxBytes, m.HTTP, m.Latency, m.Claims, m.Attempts, m.Retry, m.Stage, m.Ledger, m.Projected, m.Backlog, m.Errors, m.ReconcileMismatch, m.ReconcileTime, m.SnapshotTime, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	for _, state := range []string{"issued", "failed"} {
		m.Ledger.WithLabelValues(state).Set(0)
	}
	for _, stage := range []string{"relay", "worker", "batch", "ack"} {
		m.Retry.WithLabelValues(stage).Add(0)
	}
	return m
}
func (m *Metrics) middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "/metrics" {
			return
		}
		if route == "" {
			route = "unmatched"
		}
		method := c.Request.Method
		switch method {
		case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS":
		default:
			method = "OTHER"
		}
		status := c.Writer.Status()
		m.HTTP.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
		m.Latency.WithLabelValues(route, method).Observe(time.Since(start).Seconds())
		if route == "/v1/claims" || route == "/v1/claims/direct" {
			mode := "async"
			if route == "/v1/claims/direct" {
				mode = "direct"
			}
			outcome := "error"
			if status == 200 || status == 202 {
				outcome = "accepted"
			} else if status == 409 {
				outcome = "rejected"
			} else if status == 429 {
				outcome = "limited"
			}
			m.Claims.WithLabelValues(mode, outcome).Inc()
		}
	}
}
func (s *Service) observe(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		job, cancel := context.WithTimeout(ctx, 4*time.Second)
		err := s.collect(job)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.Metrics.Errors.WithLabelValues("metrics_collect").Inc()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (s *Service) collect(ctx context.Context) error {
	length, err := s.R.XLen(ctx, stream).Result()
	if err != nil {
		return err
	}
	size, err := s.R.MemoryUsage(ctx, stream).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	s.Metrics.OutboxEntries.Set(float64(length))
	s.Metrics.OutboxBytes.Set(float64(size))
	for _, stage := range []struct{ key, group, label string }{{stream, "relay", "outbox"}, {batchStream, "batch", "batch"}} {
		groups, err := s.R.XInfoGroups(ctx, stage.key).Result()
		if err != nil {
			return err
		}
		for _, g := range groups {
			if g.Name == stage.group {
				s.Metrics.Backlog.WithLabelValues(stage.label, "pending").Set(float64(g.Pending))
				s.Metrics.Backlog.WithLabelValues(stage.label, "unread").Set(float64(g.Lag))
			}
		}
	}
	if s.Config.Role == "worker" || s.Config.Role == "all" {
		rows, err := s.Store.DB.QueryContext(ctx, "SELECT state,COUNT(*) FROM grants GROUP BY state")
		if err != nil {
			return err
		}
		defer rows.Close()
		counts := map[string]int64{"issued": 0, "failed": 0}
		for rows.Next() {
			var state string
			var n int64
			if err = rows.Scan(&state, &n); err != nil {
				return err
			}
			counts[state] = n
		}
		if err = rows.Err(); err != nil {
			return err
		}
		for _, state := range []string{"issued", "failed"} {
			s.Metrics.Ledger.WithLabelValues(state).Set(float64(counts[state]))
		}
	}
	if s.Config.Role == "worker" || s.Config.Role == "all" {
		var projected int64
		if err := s.Store.Coupons.QueryRowContext(ctx, "SELECT COUNT(*) FROM coupons").Scan(&projected); err != nil {
			return err
		}
		s.Metrics.Projected.Set(float64(projected))
	}
	s.Metrics.SnapshotTime.SetToCurrentTime()
	return nil
}
func (s *Service) metricsHandler() gin.HandlerFunc {
	return gin.WrapH(promhttp.HandlerFor(s.Metrics.Registry, promhttp.HandlerOpts{Timeout: 3 * time.Second}))
}
