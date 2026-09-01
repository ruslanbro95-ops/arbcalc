package sources

import (
	"context"
	"testing"
	"time"
)

func TestLimiterAllowsBurstThenThrottles(t *testing.T) {
	l := newLimiter(60, 6) // one token per second, six in reserve
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return base }

	for i := 0; i < 6; i++ {
		if _, ok := l.reserve(); !ok {
			t.Fatalf("burst request %d should have been allowed", i+1)
		}
	}
	delay, ok := l.reserve()
	if ok {
		t.Fatal("the seventh request must be throttled")
	}
	if delay <= 0 || delay > time.Second {
		t.Fatalf("delay = %v, want a wait of up to one second", delay)
	}
}

func TestLimiterRefillsOverTime(t *testing.T) {
	l := newLimiter(60, 2)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }

	l.reserve()
	l.reserve()
	if _, ok := l.reserve(); ok {
		t.Fatal("bucket should be empty")
	}
	now = now.Add(2 * time.Second)
	if _, ok := l.reserve(); !ok {
		t.Fatal("two seconds should have regenerated a token")
	}
}

func TestLimiterRefillCapsAtBurst(t *testing.T) {
	// An idle hour must not bank an hour's worth of requests, or the next tick
	// would dump the whole backlog at the provider at once.
	l := newLimiter(60, 3)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	l.reserve()

	now = now.Add(time.Hour)
	for i := 0; i < 3; i++ {
		if _, ok := l.reserve(); !ok {
			t.Fatalf("request %d within the burst should pass", i+1)
		}
	}
	if _, ok := l.reserve(); ok {
		t.Fatal("refill must be capped at the burst size")
	}
}

func TestLimiterRespectsContext(t *testing.T) {
	l := newLimiter(1, 1)
	l.reserve()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.wait(ctx); err == nil {
		t.Fatal("a cancelled context must abort the wait")
	}
}
