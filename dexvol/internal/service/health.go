package service

import (
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// healthTracker records, per chain and per calendar minute, whether ingestion
// was trustworthy.
//
// This is what decides between a real zero and a MISSING minute, so it errs
// toward MISSING. A minute nobody reported on is not healthy: silence is
// absence of evidence, and treating it as a confirmed zero is exactly how an
// outage turns into a fake spike on recovery.
type healthTracker struct {
	mu   sync.Mutex
	seen map[domain.Chain]map[int64]bool // minute -> healthy so far
}

func newHealthTracker() *healthTracker {
	return &healthTracker{seen: map[domain.Chain]map[int64]bool{}}
}

func minuteKey(t time.Time) int64 { return t.UTC().Truncate(time.Minute).Unix() }

// record marks the outcome of one poll that finished at `at`.
//
// A failure taints the previous minute as well as the current one: a poll
// covers everything since the last successful call, so when it fails the gap it
// was meant to close may well start before the minute boundary.
func (h *healthTracker) record(chain domain.Chain, at time.Time, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.seen[chain] == nil {
		h.seen[chain] = map[int64]bool{}
	}
	cur := minuteKey(at)
	if ok {
		// Never upgrade a minute that some other poll already failed in.
		if _, exists := h.seen[chain][cur]; !exists {
			h.seen[chain][cur] = true
		}
		return
	}
	h.seen[chain][cur] = false
	h.seen[chain][minuteKey(at.Add(-time.Minute))] = false
}

// healthy reports whether the chain can be trusted for that minute.
func (h *healthTracker) healthy(chain domain.Chain, minute time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	byMinute := h.seen[chain]
	if byMinute == nil {
		return false
	}
	ok, seen := byMinute[minuteKey(minute)]
	return seen && ok
}

// prune drops records older than the cutoff.
func (h *healthTracker) prune(before time.Time) {
	cutoff := minuteKey(before)
	h.mu.Lock()
	defer h.mu.Unlock()
	for chain, byMinute := range h.seen {
		for k := range byMinute {
			if k < cutoff {
				delete(byMinute, k)
			}
		}
		if len(byMinute) == 0 {
			delete(h.seen, chain)
		}
	}
}
