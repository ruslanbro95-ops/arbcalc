package sources

import (
	"context"
	"sync"
	"time"
)

// limiter is a token bucket sized in requests per minute.
//
// It is hand-rolled rather than pulled from golang.org/x/time/rate so the whole
// service builds with no external dependencies: the only thing a person needs
// to run this is a Go toolchain.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration // time to regenerate one token
	burst    float64
	tokens   float64
	last     time.Time
	// now is swappable so the tests do not have to sleep.
	now func() time.Time
}

func newLimiter(perMinute, burst int) *limiter {
	if perMinute < 1 {
		perMinute = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &limiter{
		interval: time.Minute / time.Duration(perMinute),
		burst:    float64(burst),
		tokens:   float64(burst),
		now:      time.Now,
	}
}

// wait blocks until a request may proceed, or until ctx is done.
func (l *limiter) wait(ctx context.Context) error {
	for {
		delay, ok := l.reserve()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// reserve takes a token if one is available, otherwise reports how long to wait
// before trying again.
func (l *limiter) reserve() (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	if l.last.IsZero() {
		l.last = now
	}
	// Refill by however many token-intervals have elapsed, capped at the burst.
	l.tokens += float64(now.Sub(l.last)) / float64(l.interval)
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.last = now

	if l.tokens >= 1 {
		l.tokens--
		return 0, true
	}
	return time.Duration((1 - l.tokens) * float64(l.interval)), false
}
