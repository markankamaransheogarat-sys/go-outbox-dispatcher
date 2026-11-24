package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/markankamaransheogarat-sys/go-outbox-dispatcher/internal/metrics"
	"github.com/markankamaransheogarat-sys/go-outbox-dispatcher/internal/outbox"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := envOr("DATABASE_URL", "postgres://paygate:paygate@localhost:5432/paygate_rails_acquiring_development?sslmode=disable")
	addr := envOr("METRICS_ADDR", ":9091")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("pgxpool init failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	reg := &metrics.Registry{}
	cfg := outbox.DefaultConfig()
	if w, err := strconv.Atoi(os.Getenv("WORKERS")); err == nil && w > 0 {
		cfg.Workers = w
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", reg.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "db_unhealthy", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	go func() {
		logger.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "err", err)
		}
	}()

	dispatcher := outbox.NewDispatcher(outbox.NewPgStore(pool), reg, cfg)
	logger.Info("go-outbox-dispatcher started", "workers", cfg.Workers, "batch_size", cfg.BatchSize)
	_ = dispatcher.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("dispatcher stopped cleanly")
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
