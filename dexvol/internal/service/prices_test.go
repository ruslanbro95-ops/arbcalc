package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

type stubProvider struct {
	out  map[string]float64
	err  error
	seen []domain.Token
}

func (s *stubProvider) Name() string { return "stub" }
func (s *stubProvider) Prices(_ context.Context, toks []domain.Token) (map[string]float64, error) {
	s.seen = toks
	return s.out, s.err
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPriceCacheRoundTrip(t *testing.T) {
	p := &stubProvider{out: map[string]float64{"base:0xaa": 2.5}}
	c := NewPriceCache(p, discardLog())

	if err := c.Refresh(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}}); err != nil {
		t.Fatal(err)
	}
	// Lookup must not care about address casing.
	got, ok := c.PriceUSD(domain.ChainBase, "0xAa")
	if !ok || got != 2.5 {
		t.Fatalf("got %v ok=%v", got, ok)
	}
}

func TestStalePriceIsReportedUnknown(t *testing.T) {
	// Valuing this minute's swaps at a twenty-minute-old price would rewrite
	// the volume in a way that looks like real market movement.
	p := &stubProvider{out: map[string]float64{"base:0xaa": 2.5}}
	c := NewPriceCache(p, discardLog())

	base := time.Now()
	c.now = func() time.Time { return base }
	c.Refresh(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}})

	c.now = func() time.Time { return base.Add(maxPriceAge + time.Second) }
	if _, ok := c.PriceUSD(domain.ChainBase, "0xAA"); ok {
		t.Fatal("a price past its age must be reported as unknown")
	}
}

func TestQuoteAssetsAreIncludedInRefresh(t *testing.T) {
	p := &stubProvider{out: map[string]float64{}}
	c := NewPriceCache(p, discardLog())
	c.TrackQuoteAssets([]domain.Token{{Chain: domain.ChainBase, Address: "0xWETH"}})

	c.Refresh(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}})
	if len(p.seen) != 2 {
		t.Fatalf("refresh asked for %d tokens, want the tracked one plus its quote asset", len(p.seen))
	}
}

func TestTrackedTokenIsNotRequestedTwice(t *testing.T) {
	p := &stubProvider{out: map[string]float64{}}
	c := NewPriceCache(p, discardLog())
	c.TrackQuoteAssets([]domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}})

	c.Refresh(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}})
	if len(p.seen) != 1 {
		t.Fatalf("asked for %d tokens, want 1", len(p.seen))
	}
}

func TestPartialResultIsKeptOnError(t *testing.T) {
	// The provider answered for one chain and then failed on another; throwing
	// the good half away would cost coverage for no reason.
	p := &stubProvider{out: map[string]float64{"base:0xaa": 2.5}, err: errors.New("bsc failed")}
	c := NewPriceCache(p, discardLog())

	if err := c.Refresh(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}}); err == nil {
		t.Fatal("the error must still surface")
	}
	if _, ok := c.PriceUSD(domain.ChainBase, "0xAA"); !ok {
		t.Fatal("the price that did arrive should have been kept")
	}
}
