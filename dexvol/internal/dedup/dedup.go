// Package dedup keeps the same on-chain swap from being counted twice.
//
// The architecture deliberately runs several free sources side by side to reach
// the coverage the spec asks for, which means one swap can arrive through the
// RPC adapter and again through an aggregator cross-check. Without a shared
// dedup key that overlap would inflate every volume number.
package dedup

import (
	"sync"
	"time"
)

// Set is a time-bounded set of trade keys.
//
// Entries expire rather than living forever: the engine only cares about the
// last 24h of trades, and an unbounded set would grow without limit on a busy
// token.
type Set struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
	// lastPrune keeps the sweep off the hot path — it runs at most once a
	// minute instead of on every insert.
	lastPrune time.Time
}

func New(ttl time.Duration) *Set {
	return &Set{seen: make(map[string]time.Time), ttl: ttl}
}

// Seen reports whether key was already recorded, and records it if not.
// It returns true for a duplicate, which the caller should drop.
func (s *Set) Seen(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if now.Sub(s.lastPrune) >= time.Minute {
		s.pruneLocked(now)
		s.lastPrune = now
	}
	if _, dup := s.seen[key]; dup {
		return true
	}
	s.seen[key] = now
	return false
}

func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

func (s *Set) pruneLocked(now time.Time) {
	cutoff := now.Add(-s.ttl)
	for k, ts := range s.seen {
		if ts.Before(cutoff) {
			delete(s.seen, k)
		}
	}
}
