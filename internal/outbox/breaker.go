package outbox

import (
	"net/url"
	"sync"
	"time"
)

type hostState struct {
	failures  int
	openUntil time.Time
}

// HostBreaker tracks consecutive failures per destination host to isolate
// degraded merchant webhook endpoints from the shared worker pool.
type HostBreaker struct {
	mu           sync.Mutex
	hosts        map[string]*hostState
	maxFailures  int
	openDuration time.Duration
	nowFn        func() time.Time
}

func NewHostBreaker(maxFailures int, openDuration time.Duration) *HostBreaker {
	return &HostBreaker{
		hosts:        make(map[string]*hostState),
		maxFailures:  maxFailures,
		openDuration: openDuration,
		nowFn:        time.Now,
	}
}

func (b *HostBreaker) Allow(rawURL string) bool {
	host := extractHost(rawURL)
	if host == "" {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.hosts[host]
	if !ok {
		return true
	}
	return !b.nowFn().Before(st.openUntil)
}

func (b *HostBreaker) RecordSuccess(rawURL string) {
	host := extractHost(rawURL)
	if host == "" {
		return
	}

	b.mu.Lock()
	delete(b.hosts, host)
	b.mu.Unlock()
}

func (b *HostBreaker) RecordFailure(rawURL string) {
	host := extractHost(rawURL)
	if host == "" {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.hosts[host]
	if !ok {
		st = &hostState{}
		b.hosts[host] = st
	}
	st.failures++
	if st.failures >= b.maxFailures {
		st.openUntil = b.nowFn().Add(b.openDuration)
	}
}

func extractHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}
