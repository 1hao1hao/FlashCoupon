package main

import (
	"context"
	"flashcoupon/internal/app"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := app.LoadConfig()
	s, e := app.NewService(cfg)
	if e != nil {
		slog.Error("database startup", "error", e)
		os.Exit(1)
	}
	defer s.Close()
	if e = s.Init(ctx); e != nil {
		slog.Error("initialize", "error", e)
		os.Exit(1)
	}
	m, e := s.StartMessaging(ctx)
	if e != nil {
		slog.Error("messaging startup", "error", e)
		os.Exit(1)
	}
	defer m.Close()
	if cfg.Role == "all" || cfg.Role == "worker" {
		go s.Maintenance(ctx)
	}
	server := &http.Server{Addr: cfg.Addr, Handler: s.Router(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		slog.Info("listening", "addr", cfg.Addr, "role", cfg.Role)
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			slog.Error("HTTP", "error", e)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	server.Shutdown(shutdown)
}
