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
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		c := http.Client{Timeout: 3 * time.Second}
		r, e := c.Get("http://127.0.0.1" + app.LoadConfig().Addr + "/health/ready")
		if e != nil {
			os.Exit(1)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
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
	if e = server.Shutdown(shutdown); e != nil {
		slog.Error("HTTP drain deadline", "error", e)
		server.Close()
	}
}
