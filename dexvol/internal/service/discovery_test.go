package service

import (
	"context"
	"errors"
	"testing"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

type stubDiscoverer struct {
	name  string
	pools []domain.Pool
	err   error
}

func (s *stubDiscoverer) Name() string { return s.name }
func (s *stubDiscoverer) DiscoverPools(context.Context, domain.Token) ([]domain.Pool, error) {
	return s.pools, s.err
}

var tok = domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN", Enabled: true}

func TestMergeDeduplicatesAndFlagsExclusivePools(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xShared", LiquidityUSD: 100, Source: "a"},
		{Chain: domain.ChainBase, Address: "0xOnlyA", LiquidityUSD: 900, Source: "a"},
	}}
	b := &stubDiscoverer{name: "b", pools: []domain.Pool{
		// Same pool, different casing: it is one pool, not two.
		{Chain: domain.ChainBase, Address: "0xSHARED", LiquidityUSD: 250, DEX: "uniswap", Source: "b"},
	}}

	res := NewDiscovery(discardLog(), a, b).Run(t.Context(), []domain.Token{tok})

	pools := res.ByChain[domain.ChainBase]
	if len(pools) != 2 {
		t.Fatalf("got %d pools, want 2 after dedup: %+v", len(pools), pools)
	}
	// A pool only one provider found is the coverage signal worth surfacing.
	if got := res.ExclusiveTo["a"]; len(got) != 1 || got[0] != "0xOnlyA" {
		t.Fatalf("exclusive to a = %v, want [0xOnlyA]", got)
	}
	if len(res.ExclusiveTo["b"]) != 0 {
		t.Fatalf("b found nothing exclusive, got %v", res.ExclusiveTo["b"])
	}
}

func TestMergeTakesTheRicherView(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xP", LiquidityUSD: 100, Source: "a"},
	}}
	b := &stubDiscoverer{name: "b", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xP", LiquidityUSD: 250, DEX: "uniswap", QuoteAddr: "0xWETH", Source: "b"},
	}}

	res := NewDiscovery(discardLog(), a, b).Run(t.Context(), []domain.Token{tok})
	got := res.ByChain[domain.ChainBase][0]

	if got.DEX != "uniswap" {
		t.Fatalf("dex = %q, want the value the second provider supplied", got.DEX)
	}
	// A zero almost always means "not reported", not "empty pool".
	if got.LiquidityUSD != 250 {
		t.Fatalf("liquidity = %v, want 250", got.LiquidityUSD)
	}
}

func TestPoolsAreSortedByLiquidity(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xSmall", LiquidityUSD: 10},
		{Chain: domain.ChainBase, Address: "0xBig", LiquidityUSD: 9000},
		{Chain: domain.ChainBase, Address: "0xMid", LiquidityUSD: 500},
	}}
	res := NewDiscovery(discardLog(), a).Run(t.Context(), []domain.Token{tok})

	got := res.ByChain[domain.ChainBase]
	if got[0].Address != "0xBig" || got[2].Address != "0xSmall" {
		t.Fatalf("unexpected order: %v %v %v", got[0].Address, got[1].Address, got[2].Address)
	}
}

func TestQuoteAssetsAreCollected(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xP1", QuoteAddr: "0xWETH", QuoteSymbol: "WETH"},
		{Chain: domain.ChainBase, Address: "0xP2", QuoteAddr: "0xUSDC", QuoteSymbol: "USDC"},
		// The tracked token on the quote side is not a counterparty.
		{Chain: domain.ChainBase, Address: "0xP3", QuoteAddr: "0xTOKEN"},
	}}
	res := NewDiscovery(discardLog(), a).Run(t.Context(), []domain.Token{tok})

	if len(res.QuoteAssets) != 2 {
		t.Fatalf("got %d quote assets, want 2: %+v", len(res.QuoteAssets), res.QuoteAssets)
	}
}

func TestSymbolIsResolvedFromPoolMetadata(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xP", BaseAddr: "0xTOKEN", BaseSymbol: "ABC"},
	}}
	res := NewDiscovery(discardLog(), a).Run(t.Context(), []domain.Token{tok})

	if res.Symbols[tok.Key()] != "ABC" {
		t.Fatalf("symbols = %v", res.Symbols)
	}
}

func TestFailingProviderDegradesRatherThanStops(t *testing.T) {
	// One aggregator being down should cost coverage, not halt ingestion.
	broken := &stubDiscoverer{name: "broken", err: errors.New("503")}
	ok := &stubDiscoverer{name: "ok", pools: []domain.Pool{
		{Chain: domain.ChainBase, Address: "0xP", LiquidityUSD: 1},
	}}

	res := NewDiscovery(discardLog(), broken, ok).Run(t.Context(), []domain.Token{tok})
	if len(res.ByChain[domain.ChainBase]) != 1 {
		t.Fatalf("the working provider's pools should still come through: %+v", res.ByChain)
	}
}

func TestDisabledTokenIsSkipped(t *testing.T) {
	a := &stubDiscoverer{name: "a", pools: []domain.Pool{{Chain: domain.ChainBase, Address: "0xP"}}}
	paused := domain.Token{Chain: domain.ChainBase, Address: "0xT", Enabled: false}

	res := NewDiscovery(discardLog(), a).Run(t.Context(), []domain.Token{paused})
	if len(res.ByChain) != 0 {
		t.Fatalf("a paused token must not be discovered: %+v", res.ByChain)
	}
}
