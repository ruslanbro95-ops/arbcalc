package volume

import (
	"testing"
	"time"
)

func at(min int) time.Time {
	return time.Date(2026, 9, 1, 12, min, 0, 0, time.UTC)
}

func TestAddAggregatesByCalendarMinute(t *testing.T) {
	s := NewSeries()
	base := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)
	// The worked example from the spec: four trades inside 12:30 sum to $5,000.
	s.Add(base.Add(1*time.Second), true, 1000)
	s.Add(base.Add(8*time.Second), false, 500)
	s.Add(base.Add(14*time.Second), true, 2000)
	s.Add(base.Add(45*time.Second), false, 1500)
	// A trade one second later belongs to the next minute, not this one.
	s.Add(base.Add(60*time.Second), true, 999)

	b, ok := s.Get(base)
	if !ok {
		t.Fatal("bucket missing")
	}
	if b.Total != 5000 {
		t.Fatalf("total = %v, want 5000", b.Total)
	}
	if b.Buy != 3000 || b.Sell != 2000 {
		t.Fatalf("buy/sell = %v/%v, want 3000/2000", b.Buy, b.Sell)
	}
	if b.Trades != 4 {
		t.Fatalf("trades = %d, want 4", b.Trades)
	}
}

func TestMissingMinutesAreExcludedNotZeroed(t *testing.T) {
	// Ten minutes of $100 volume, then five minutes where the source was down.
	// If the outage were recorded as zeros, the median would collapse toward 0
	// and the next normal minute would read as a huge spike.
	s := NewSeries()
	for i := 1; i <= 10; i++ {
		s.Add(at(i), true, 100)
		s.Seal(at(i), true)
	}
	for i := 11; i <= 15; i++ {
		s.Seal(at(i), false) // outage: no trades, and not trustworthy
	}

	bl := s.BaselineFor(at(16), Window10m)
	if !bl.Usable {
		t.Fatalf("baseline unusable: %+v", bl)
	}
	if bl.Median != 100 {
		t.Fatalf("median = %v, want 100 (outage must not count as zero)", bl.Median)
	}
	if bl.Samples != 5 {
		t.Fatalf("samples = %d, want 5 healthy minutes inside the window", bl.Samples)
	}
}

func TestQuietMinutesCountAsRealZeros(t *testing.T) {
	// A healthy source with no trades is a real zero and must lower the median.
	s := NewSeries()
	for i := 1; i <= 5; i++ {
		s.Add(at(i), true, 100)
		s.Seal(at(i), true)
	}
	for i := 6; i <= 10; i++ {
		s.Seal(at(i), true) // healthy, genuinely no trades
	}
	bl := s.BaselineFor(at(11), Window10m)
	if bl.Samples != 10 {
		t.Fatalf("samples = %d, want 10", bl.Samples)
	}
	if bl.Median != 50 {
		t.Fatalf("median = %v, want 50", bl.Median)
	}
}

func TestBaselineExcludesTheMinuteUnderTest(t *testing.T) {
	s := NewSeries()
	for i := 1; i <= 10; i++ {
		s.Add(at(i), true, 100)
		s.Seal(at(i), true)
	}
	// The spike minute itself must not pull up the baseline it is judged by.
	s.Add(at(11), true, 999999)
	s.Seal(at(11), true)

	bl := s.BaselineFor(at(11), Window10m)
	if bl.Median != 100 {
		t.Fatalf("median = %v, want 100", bl.Median)
	}
}

func TestBaselineUnusableBeforeEnoughSamples(t *testing.T) {
	s := NewSeries()
	for i := 1; i <= 3; i++ {
		s.Add(at(i), true, 100)
		s.Seal(at(i), true)
	}
	if bl := s.BaselineFor(at(4), Window10m); bl.Usable {
		t.Fatalf("baseline should stay unusable with %d samples", bl.Samples)
	}
}

func TestSealedMinuteRejectsLateTrades(t *testing.T) {
	s := NewSeries()
	s.Add(at(1), true, 100)
	s.Seal(at(1), true)
	if s.Add(at(1), true, 50) {
		t.Fatal("a sealed minute must not accept more volume")
	}
	b, _ := s.Get(at(1))
	if b.Total != 100 {
		t.Fatalf("total = %v, want 100", b.Total)
	}
}

func TestHealthCountsSealedMinutes(t *testing.T) {
	s := NewSeries()
	for i := 1; i <= 6; i++ {
		s.Seal(at(i), i%2 == 0)
	}
	healthy, total := s.Health(at(7), 6)
	if total != 6 || healthy != 3 {
		t.Fatalf("health = %d/%d, want 3/6", healthy, total)
	}
}
