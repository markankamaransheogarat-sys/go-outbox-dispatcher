package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Registry stores lock-free atomic counters for Prometheus exposition.
type Registry struct {
	Claimed     atomic.Int64
	Delivered   atomic.Int64
	Retried     atomic.Int64
	Dead        atomic.Int64
	CircuitSkip atomic.Int64
	Reclaimed   atomic.Int64
}

func (r *Registry) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w,
			"# TYPE outbox_claimed_total counter\noutbox_claimed_total %d\n"+
				"# TYPE outbox_delivered_total counter\noutbox_delivered_total %d\n"+
				"# TYPE outbox_retried_total counter\noutbox_retried_total %d\n"+
				"# TYPE outbox_dead_total counter\noutbox_dead_total %d\n"+
				"# TYPE outbox_circuit_skipped_total counter\noutbox_circuit_skipped_total %d\n"+
				"# TYPE outbox_reclaimed_total counter\noutbox_reclaimed_total %d\n",
			r.Claimed.Load(),
			r.Delivered.Load(),
			r.Retried.Load(),
			r.Dead.Load(),
			r.CircuitSkip.Load(),
			r.Reclaimed.Load(),
		)
	}
}
