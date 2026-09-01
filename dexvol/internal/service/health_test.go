package service

import (
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

func tAt(min int) time.Time {
	return time.Date(2026, 9, 1, 12, min, 30, 0, time.UTC)
}

func TestUnobservedMinuteIsNotHealthy(t *testing.T) {
	// No poll reported on this minute at all. Calling it healthy would let the
	// engine record a confirmed zero it has no evidence for.
	h := newHealthTracker()
	if h.healthy(domain.ChainBase, tAt(5)) {
		t.Fatal("a minute with no observation must not count as healthy")
	}
}

func TestSuccessfulPollMarksMinuteHealthy(t *testing.T) {
	h := newHealthTracker()
	h.record(domain.ChainBase, tAt(5), true)
	if !h.healthy(domain.ChainBase, tAt(5)) {
		t.Fatal("expected healthy")
	}
}

func TestFailureTaintsCurrentAndPreviousMinute(t *testing.T) {
	// A poll covers everything since the last successful call, so a failure at
	// 12:05:30 may have lost data that belongs to 12:04.
	h := newHealthTracker()
	h.record(domain.ChainBase, tAt(4), true)
	h.record(domain.ChainBase, tAt(5), false)

	if h.healthy(domain.ChainBase, tAt(5)) {
		t.Fatal("the failing minute must be unhealthy")
	}
	if h.healthy(domain.ChainBase, tAt(4)) {
		t.Fatal("the preceding minute must be tainted too")
	}
}

func TestSuccessDoesNotOverwriteAFailure(t *testing.T) {
	// Several polls land in one minute; one failure is enough to distrust it.
	h := newHealthTracker()
	h.record(domain.ChainBase, tAt(5), false)
	h.record(domain.ChainBase, tAt(5), true)
	if h.healthy(domain.ChainBase, tAt(5)) {
		t.Fatal("a later success must not launder an earlier failure in the same minute")
	}
}

func TestChainsAreIndependent(t *testing.T) {
	h := newHealthTracker()
	h.record(domain.ChainBase, tAt(5), true)
	h.record(domain.ChainSolana, tAt(5), false)

	if !h.healthy(domain.ChainBase, tAt(5)) {
		t.Fatal("base should be unaffected")
	}
	if h.healthy(domain.ChainSolana, tAt(5)) {
		t.Fatal("solana should be unhealthy")
	}
}

func TestPruneDropsOldMinutes(t *testing.T) {
	h := newHealthTracker()
	h.record(domain.ChainBase, tAt(1), true)
	h.record(domain.ChainBase, tAt(50), true)

	h.prune(tAt(40))
	if h.healthy(domain.ChainBase, tAt(1)) {
		t.Fatal("the old minute should have been pruned")
	}
	if !h.healthy(domain.ChainBase, tAt(50)) {
		t.Fatal("the recent minute must survive")
	}
}
