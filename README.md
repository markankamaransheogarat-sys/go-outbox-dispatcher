# go-outbox-dispatcher

High-concurrency transactional webhook outbox dispatcher written in **Go 1.26 (`pgx/v5`)**, built as the Go worker companion to [`paygate-rails-acquiring`](https://github.com/markankamaransheogarat-sys/paygate-rails-acquiring).

## Architecture
- **Lock-Free Claiming (`FOR UPDATE SKIP LOCKED`)**: Multiple Go dispatcher instances claim batches from PostgreSQL `webhook_deliveries` concurrently without lock contention.
- **Bounded Worker Pool + Connection Pooling**: Concurrent HTTP delivery with reusable keep-alive connections and strict per-request timeouts.
- **Per-Host Circuit Breaker**: Isolates degraded merchant endpoints so one slow host cannot stall delivery to healthy merchants.
- **Jittered Exponential Backoff & Dead-Lettering**: Retries transient failures up to `MaxAttempts` before transitioning to `dead`.
- **Stuck Row Reclaimer & Graceful Shutdown**: Reclaims crashed `processing` rows and drains in-flight workers on `SIGINT`/`SIGTERM`.
- **Prometheus `/metrics` & `/healthz`**: Lock-free `atomic.Int64` telemetry counters.

## Run
```bash
go test -race -v ./...
DATABASE_URL="postgres://paygate:paygate@localhost:5432/paygate_rails_acquiring_development?sslmode=disable" go run ./cmd/dispatcher
```
