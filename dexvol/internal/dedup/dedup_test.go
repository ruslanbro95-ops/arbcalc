package dedup

import (
	"testing"
	"time"
)

func TestSeenDetectsDuplicate(t *testing.T) {
	s := New(time.Hour)
	now := time.Now()
	if s.Seen("ethereum|0xabc|3", now) {
		t.Fatal("first sighting must not be a duplicate")
	}
	if !s.Seen("ethereum|0xabc|3", now) {
		t.Fatal("second sighting must be a duplicate")
	}
}

func TestSameTxDifferentLogIndexAreDistinct(t *testing.T) {
	// One transaction can contain several swaps; they are different trades.
	s := New(time.Hour)
	now := time.Now()
	s.Seen("ethereum|0xabc|0", now)
	if s.Seen("ethereum|0xabc|1", now) {
		t.Fatal("a different log index is a different trade")
	}
}

func TestEntriesExpire(t *testing.T) {
	s := New(10 * time.Minute)
	t0 := time.Now()
	s.Seen("k", t0)
	// Well past the TTL, and past the prune interval, the key is forgotten.
	if s.Seen("k", t0.Add(30*time.Minute)) {
		t.Fatal("entry should have expired")
	}
}
