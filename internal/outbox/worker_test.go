package outbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/markankamaransheogarat-sys/go-outbox-dispatcher/internal/metrics"
)

type memStore struct {
	mu        sync.Mutex
	items     []Delivery
	delivered map[int64]int
	retried   map[int64]string
	dead      map[int64]bool
}

func newMemStore(items ...Delivery) *memStore {
	return &memStore{
		items:     items,
		delivered: make(map[int64]int),
		retried:   make(map[int64]string),
		dead:      make(map[int64]bool),
	}
}

func (m *memStore) ClaimDue(_ context.Context, limit int) ([]Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.items) == 0 {
		return nil, nil
	}
	n := min(limit, len(m.items))
	batch := append([]Delivery(nil), m.items[:n]...)
	m.items = m.items[n:]
	return batch, nil
}

func (m *memStore) MarkDelivered(_ context.Context, id int64, httpStatus int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered[id] = httpStatus
	return nil
}

func (m *memStore) ScheduleRetryOrDead(_ context.Context, id int64, attempts, maxAttempts, _ int, errMsg string, _ time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if attempts >= maxAttempts {
		m.dead[id] = true
		return true, nil
	}
	m.retried[id] = errMsg
	return false, nil
}

func (m *memStore) ReclaimStuck(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func TestDispatcherWorkerPoolAndDeadLetter(t *testing.T) {
	var okHits atomic.Int64
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Paygate-Event") != "payment.captured" || r.Header.Get("X-Paygate-Signature") != "sig_1" {
			http.Error(w, "bad headers", http.StatusBadRequest)
			return
		}
		okHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	store := newMemStore(
		Delivery{ID: 1, EventType: "payment.captured", TargetURL: okSrv.URL, Payload: json.RawMessage(`{"ok":true}`), Signature: "sig_1", Attempts: 1},
		Delivery{ID: 2, EventType: "payment.failed", TargetURL: failSrv.URL, Payload: json.RawMessage(`{"ok":false}`), Signature: "sig_2", Attempts: 2},
		Delivery{ID: 3, EventType: "payment.failed", TargetURL: failSrv.URL, Payload: json.RawMessage(`{"ok":false}`), Signature: "sig_3", Attempts: 5},
	)

	reg := &metrics.Registry{}
	cfg := DefaultConfig()
	cfg.Workers = 4
	d := NewDispatcher(store, reg, cfg)

	n, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch error: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 processed, got %d", n)
	}
	if okHits.Load() != 1 || reg.Delivered.Load() != 1 || reg.Retried.Load() != 1 || reg.Dead.Load() != 1 {
		t.Fatalf("unexpected metrics: delivered=%d retried=%d dead=%d", reg.Delivered.Load(), reg.Retried.Load(), reg.Dead.Load())
	}
}

func TestHostBreakerOpensAndRecovers(t *testing.T) {
	now := time.Now()
	b := NewHostBreaker(2, 30*time.Second)
	b.nowFn = func() time.Time { return now }

	target := "https://merchant.example/hook"
	if !b.Allow(target) {
		t.Fatal("expected initial Allow=true")
	}
	b.RecordFailure(target)
	if !b.Allow(target) {
		t.Fatal("expected Allow=true after 1 failure")
	}
	b.RecordFailure(target)
	if b.Allow(target) {
		t.Fatal("expected Allow=false after 2 failures")
	}
	now = now.Add(31 * time.Second)
	if !b.Allow(target) {
		t.Fatal("expected Allow=true after cooldown")
	}
}

func TestPgStoreSkipLockedIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool failed: %v", err)
	}
	defer pool.Close()

	// Verify ClaimDue runs cleanly against real PostgreSQL schema
	store := NewPgStore(pool)
	_, err = store.ClaimDue(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimDue failed: %v", err)
	}
}
