package app

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

func workerConcurrency() int {
	n, e := strconv.Atoi(env("WORKER_CONCURRENCY", "20"))
	if e != nil || n < 1 || n > 128 {
		return 20
	}
	return n
}
func (s *Service) registerDBMetrics() {
	for name, db := range map[string]*sql.DB{"control": s.Store.DB, "coupons": s.Store.Coupons} {
		labels := prometheus.Labels{"role": s.Config.Role, "pool": name}
		for metric, read := range map[string]func() float64{
			"open":   func() float64 { return float64(db.Stats().OpenConnections) },
			"in_use": func() float64 { return float64(db.Stats().InUse) },
			"idle":   func() float64 { return float64(db.Stats().Idle) },
		} {
			s.Metrics.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "flashcoupon_db_connections_" + metric, Help: "database/sql pool connections.", ConstLabels: labels}, read))
		}
		s.Metrics.Registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "flashcoupon_db_pool_wait_total", Help: "database/sql waits for a connection.", ConstLabels: labels}, func() float64 { return float64(db.Stats().WaitCount) }))
		s.Metrics.Registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "flashcoupon_db_pool_wait_seconds_total", Help: "Cumulative database/sql pool wait seconds, not server row lock time.", ConstLabels: labels}, func() float64 { return db.Stats().WaitDuration.Seconds() }))
	}
}
