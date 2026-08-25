// Command server runs the Yuktee lead claim service: claim/release backed
// by a Redis fencing-token lease, lead state in PostgreSQL, and a
// /leads/{id}/notify endpoint that calls the flaky vendor stub.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"yuktee-assignment/internal/api"
	"yuktee-assignment/internal/lease"
	"yuktee-assignment/internal/store"
	"yuktee-assignment/internal/vendorclient"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	pgURL := getenv("DATABASE_URL", "postgres://yuktee:yuktee@localhost:5432/yuktee?sslmode=disable")
	vendorURL := getenv("VENDOR_URL", "http://localhost:9000")
	listenAddr := getenv("LISTEN_ADDR", ":8080")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("cannot reach redis", "addr", redisAddr, "error", err)
		os.Exit(1)
	}
	cancel()

	pool, err := pgxpool.New(context.Background(), pgURL)
	if err != nil {
		log.Error("cannot create postgres pool", "error", err)
		os.Exit(1)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(ctx); err != nil {
		log.Error("cannot reach postgres", "url", pgURL, "error", err)
		os.Exit(1)
	}
	cancel()

	vendorCfg := vendorclient.DefaultConfig()
	vendorCfg.BaseURL = vendorURL

	h := &api.Handler{
		Lease:  lease.NewManager(rdb),
		Store:  store.New(pool),
		Vendor: vendorclient.New(vendorCfg),
		Log:    log,
	}

	mux := http.NewServeMux()
	h.Routes(mux)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", listenAddr, "vendor_url", vendorURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Info("shutting down")
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	pool.Close()
	_ = rdb.Close()
}
