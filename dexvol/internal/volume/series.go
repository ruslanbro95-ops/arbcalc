package volume

import (
	"fmt"
	"sync"
	"time"
)

// Quality marks how much a minute bucket can be trusted.
//
// The distinction matters more than it looks: a minute with no trades because
// nobody traded is a real data point worth 0, while a minute with no trades
// because the source was unreachable is not a data point at all. Folding the
// second case into 0 would drag every median down and make the next healthy
// minute look like a spike.
type Quality string

const (
	// QualityOK means the source was healthy for the whole minute, so the
	// recorded volume — zero included — reflects reality.
	QualityOK Quality = "OK"
	// QualityMissing means the source could not be trusted for that minute.
	// Missing buckets are excluded from every baseline.
	QualityMissing Quality = "MISSING"
)

// Bucket is the volume of one calendar minute for one token.
type Bucket struct {
	Minute  time.Time
	Buy     float64
	Sell    float64
	Total   float64
	Trades  int
	Quality Quality
	// Sealed is set once the minute is complete and can no longer receive
	// trades. Only sealed buckets feed baselines.
	Sealed bool
}

// Window sizes, in minutes, for the baselines the spec requires.
const (
	Window10m = 10
	Window30m = 30
	Window60m = 60
	Window24h = 24 * 60
)

// retentionMinutes keeps a full 24h window plus a small margin for late trades.
const retentionMinutes = Window24h + 5

// MinSamples is how many healthy buckets a window needs before its median is
// trusted. Below this the baseline is reported as unusable, which keeps a
// freshly started service from alerting against a two-sample "median".
var MinSamples = map[int]int{
	Window10m: 5,
	Window30m: 12,
	Window60m: 20,
	Window24h: 60,
}

// Baseline is one window's median plus the evidence behind it.
type Baseline struct {
	WindowMinutes int
	Median        float64
	Samples       int
	// Usable is false when there were not enough healthy samples, or the
	// median came out at zero. Callers must not raise alerts on it.
	Usable bool
}

// Series holds the per-minute history of a single token.
//
// Storage is a map keyed by unix-minute rather than a ring buffer: trades can
// arrive slightly out of order (block timestamps, reorgs, a lagging source),
// and a map lets a late trade land in the minute it belongs to instead of the
// minute it arrived in.
type Series struct {
	mu      sync.RWMutex
	buckets map[int64]*Bucket
}

func NewSeries() *Series {
	return &Series{buckets: make(map[int64]*Bucket, retentionMinutes+16)}
}

func minuteKey(t time.Time) int64 { return t.UTC().Truncate(time.Minute).Unix() }

// Add records one trade into its calendar minute. A trade is itself proof the
// source was alive, so the bucket starts out healthy.
//
// Trades landing in an already-sealed minute are dropped: the baselines derived
// from that minute have already been published, and silently rewriting history
// would make an alert unreproducible. The caller gets false so it can count the
// loss as a data-quality signal.
func (s *Series) Add(minute time.Time, buy bool, usd float64) bool {
	k := minuteKey(minute)
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.buckets[k]
	if b == nil {
		b = &Bucket{Minute: minute.UTC().Truncate(time.Minute), Quality: QualityOK}
		s.buckets[k] = b
	}
	if b.Sealed {
		return false
	}
	if buy {
		b.Buy += usd
	} else {
		b.Sell += usd
	}
	b.Total += usd
	b.Trades++
	b.Quality = QualityOK
	return true
}

// Seal closes a minute and fixes its quality. The engine calls this once the
// wall clock has moved past the minute, passing whether the source stayed
// healthy for its entire span.
//
// Sealing also materializes minutes that saw no trades at all — that is how a
// genuinely quiet minute becomes a real 0 in the medians.
func (s *Series) Seal(minute time.Time, healthy bool) {
	k := minuteKey(minute)
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.buckets[k]
	if b == nil {
		b = &Bucket{Minute: minute.UTC().Truncate(time.Minute)}
		s.buckets[k] = b
	}
	if b.Sealed {
		return
	}
	if healthy {
		b.Quality = QualityOK
	} else {
		b.Quality = QualityMissing
	}
	b.Sealed = true
	s.pruneLocked(minute)
}

// pruneLocked drops buckets older than the retention window.
func (s *Series) pruneLocked(now time.Time) {
	cutoff := now.UTC().Truncate(time.Minute).Add(-retentionMinutes * time.Minute).Unix()
	for k := range s.buckets {
		if k < cutoff {
			delete(s.buckets, k)
		}
	}
}

// Get returns a copy of one minute's bucket.
func (s *Series) Get(minute time.Time) (Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.buckets[minuteKey(minute)]
	if b == nil {
		return Bucket{}, false
	}
	return *b, true
}

// BaselineFor computes the median over the `window` minutes ending just before
// `endExclusive` — the minute under test is never part of its own baseline.
//
// Only sealed, healthy buckets count. Missing minutes are skipped rather than
// treated as zero, so an outage narrows the sample set instead of poisoning it.
func (s *Series) BaselineFor(endExclusive time.Time, window int) Baseline {
	end := endExclusive.UTC().Truncate(time.Minute)
	out := Baseline{WindowMinutes: window}

	s.mu.RLock()
	vals := make([]float64, 0, window)
	for i := 1; i <= window; i++ {
		b := s.buckets[minuteKey(end.Add(-time.Duration(i)*time.Minute))]
		if b == nil || !b.Sealed || b.Quality != QualityOK {
			continue
		}
		vals = append(vals, b.Total)
	}
	s.mu.RUnlock()

	out.Samples = len(vals)
	out.Median = Median(vals)
	out.Usable = out.Samples >= MinSamples[window] && out.Median > 0
	return out
}

// Health summarizes recent data quality: how many of the last `window` minutes
// were sealed healthy. The Telegram bot surfaces this so a quiet feed is never
// mistaken for a quiet market.
func (s *Series) Health(endExclusive time.Time, window int) (healthy, total int) {
	end := endExclusive.UTC().Truncate(time.Minute)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := 1; i <= window; i++ {
		b := s.buckets[minuteKey(end.Add(-time.Duration(i)*time.Minute))]
		if b == nil || !b.Sealed {
			continue
		}
		total++
		if b.Quality == QualityOK {
			healthy++
		}
	}
	return healthy, total
}

// restore inserts an already-sealed bucket, used when replaying persisted
// state at startup. It refuses to overwrite a bucket the live feed has already
// produced, so a slow restore cannot clobber fresher data.
func (s *Series) restore(b Bucket) error {
	if !b.Sealed {
		return fmt.Errorf("restore: bucket for %s is not sealed", b.Minute)
	}
	k := minuteKey(b.Minute)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[k]; exists {
		return nil
	}
	cp := b
	cp.Minute = b.Minute.UTC().Truncate(time.Minute)
	s.buckets[k] = &cp
	return nil
}

// Sum totals the sealed, healthy minutes in [from, to).
//
// It reports the healthy and total counts alongside the sum so a caller can
// tell a genuinely quiet hour from an hour the sources only half covered — the
// difference that decides whether a coverage number means anything.
func (s *Series) Sum(from, to time.Time) (total float64, healthy, sealed int) {
	from = from.UTC().Truncate(time.Minute)
	to = to.UTC().Truncate(time.Minute)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for m := from; m.Before(to); m = m.Add(time.Minute) {
		b := s.buckets[minuteKey(m)]
		if b == nil || !b.Sealed {
			continue
		}
		sealed++
		if b.Quality != QualityOK {
			continue
		}
		healthy++
		total += b.Total
	}
	return total, healthy, sealed
}
