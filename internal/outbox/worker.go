package outbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/markankamaransheogarat-sys/go-outbox-dispatcher/internal/metrics"
)

type Config struct {
	Workers      int
	BatchSize    int
	MaxAttempts  int
	BaseBackoff  time.Duration
	PollInterval time.Duration
	HTTPTimeout  time.Duration
}

func DefaultConfig() Config {
	return Config{
		Workers:      16,
		BatchSize:    50,
		MaxAttempts:  5,
		BaseBackoff:  15 * time.Second,
		PollInterval: 500 * time.Millisecond,
		HTTPTimeout:  5 * time.Second,
	}
}

type Dispatcher struct {
	store   Store
	client  *http.Client
	breaker *HostBreaker
	metrics *metrics.Registry
	cfg     Config
}

func NewDispatcher(store Store, reg *metrics.Registry, cfg Config) *Dispatcher {
	transport := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     60 * time.Second,
	}
	return &Dispatcher{
		store:   store,
		client:  &http.Client{Transport: transport, Timeout: cfg.HTTPTimeout},
		breaker: NewHostBreaker(5, 30*time.Second),
		metrics: reg,
		cfg:     cfg,
	}
}

// Run polls PostgreSQL with FOR UPDATE SKIP LOCKED and fans out deliveries across a bounded worker pool.
func (d *Dispatcher) Run(ctx context.Context) error {
	reclaimTicker := time.NewTicker(30 * time.Second)
	defer reclaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reclaimTicker.C:
			if n, err := d.store.ReclaimStuck(ctx, 2*time.Minute); err == nil && n > 0 {
				d.metrics.Reclaimed.Add(n)
			}
		default:
		}

		processed, err := d.ProcessBatch(ctx)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}

		if processed == 0 {
			timer := time.NewTimer(d.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func (d *Dispatcher) ProcessBatch(ctx context.Context) (int, error) {
	items, err := d.store.ClaimDue(ctx, d.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	d.metrics.Claimed.Add(int64(len(items)))

	jobs := make(chan Delivery, len(items))
	var wg sync.WaitGroup
	workers := min(d.cfg.Workers, len(items))

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				d.deliverOne(ctx, item)
			}
		}()
	}

	for _, item := range items {
		jobs <- item
	}
	close(jobs)
	wg.Wait()

	return len(items), nil
}

func (d *Dispatcher) deliverOne(ctx context.Context, item Delivery) {
	if !d.breaker.Allow(item.TargetURL) {
		d.metrics.CircuitSkip.Add(1)
		nextAt := time.Now().Add(15 * time.Second)
		_, _ = d.store.ScheduleRetryOrDead(ctx, item.ID, item.Attempts, d.cfg.MaxAttempts, 0, "circuit_open", nextAt)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, item.TargetURL, bytes.NewReader(item.Payload))
	if err != nil {
		d.handleFailure(ctx, item, 0, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Paygate-Event", item.EventType)
	req.Header.Set("X-Paygate-Signature", item.Signature)

	resp, err := d.client.Do(req)
	if err != nil {
		d.breaker.RecordFailure(item.TargetURL)
		d.handleFailure(ctx, item, 0, err.Error())
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		d.breaker.RecordSuccess(item.TargetURL)
		if err := d.store.MarkDelivered(ctx, item.ID, resp.StatusCode); err == nil {
			d.metrics.Delivered.Add(1)
		}
		return
	}

	d.breaker.RecordFailure(item.TargetURL)
	d.handleFailure(ctx, item, resp.StatusCode, fmt.Sprintf("HTTP %d", resp.StatusCode))
}

func (d *Dispatcher) handleFailure(ctx context.Context, item Delivery, httpStatus int, errMsg string) {
	backoff := computeBackoff(d.cfg.BaseBackoff, item.Attempts)
	dead, err := d.store.ScheduleRetryOrDead(ctx, item.ID, item.Attempts, d.cfg.MaxAttempts, httpStatus, errMsg, time.Now().Add(backoff))
	if err != nil {
		return
	}
	if dead {
		d.metrics.Dead.Add(1)
	} else {
		d.metrics.Retried.Add(1)
	}
}

func computeBackoff(base time.Duration, attempt int) time.Duration {
	exp := math.Pow(2, float64(max(attempt-1, 0)))
	raw := time.Duration(float64(base) * exp)
	jitter := time.Duration(rand.Int64N(int64(base/4 + 1)))
	return min(raw+jitter, 30*time.Minute)
}
