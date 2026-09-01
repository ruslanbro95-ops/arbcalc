package service

import (
	"context"
	"log/slog"
	"sort"
	"strings"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// Discoverer is one pool-discovery provider.
type Discoverer interface {
	Name() string
	DiscoverPools(ctx context.Context, tok domain.Token) ([]domain.Pool, error)
}

// DiscoveryResult is one discovery pass.
type DiscoveryResult struct {
	// ByChain is the merged pool set, ready to hand to each trade source.
	ByChain map[domain.Chain][]domain.Pool
	// ByToken is the same pools grouped per watch-list entry. Backfill needs
	// this split: history is fetched per pool, and only the pools belonging to
	// the token being filled may contribute to its minutes.
	ByToken map[string][]domain.Pool
	// QuoteAssets are the counterparties that need a price so a swap can be
	// valued from the other side when the tracked token has no quote.
	QuoteAssets []domain.Token
	// Symbols carries symbols resolved from pool metadata, filling in tokens
	// the owner added by address alone.
	Symbols map[string]string
	// ExclusiveTo names, per provider, the pools only that provider found.
	//
	// This is the coverage signal that justifies running two discoverers: a
	// non-empty list means a single provider would have missed real liquidity,
	// and it is the first thing to look at when the coverage test comes in low.
	ExclusiveTo map[string][]string
}

// Discovery merges the pool views of several providers.
//
// Running more than one is the point. The spec warns not to confuse "percent of
// pools found" with "percent of volume covered": missing one deep pool can cost
// most of a token's volume while barely denting a pool count. A second opinion
// is the cheapest defence, and discovery runs on a slow timer, so it costs
// almost nothing against the rate limits.
type Discovery struct {
	providers []Discoverer
	log       *slog.Logger
}

func NewDiscovery(log *slog.Logger, providers ...Discoverer) *Discovery {
	return &Discovery{providers: providers, log: log.With("component", "discovery")}
}

// Run queries every provider for every token and merges the results.
//
// A provider that fails is logged and skipped rather than failing the pass: one
// aggregator being down should degrade coverage, not stop ingestion.
func (d *Discovery) Run(ctx context.Context, tokens []domain.Token) DiscoveryResult {
	res := DiscoveryResult{
		ByChain:     map[domain.Chain][]domain.Pool{},
		ByToken:     map[string][]domain.Pool{},
		Symbols:     map[string]string{},
		ExclusiveTo: map[string][]string{},
	}
	// perToken keeps pool keys per token while merging, so the same pool seen
	// through both providers lands once.
	perToken := map[string]map[string]bool{}

	merged := map[string]domain.Pool{}
	sawIt := map[string]map[string]bool{} // pool key -> providers that found it
	quotes := map[string]domain.Token{}

	for _, tok := range tokens {
		if !tok.Enabled {
			continue
		}
		for _, p := range d.providers {
			pools, err := p.DiscoverPools(ctx, tok)
			if err != nil {
				d.log.Warn("discovery provider failed",
					"provider", p.Name(), "token", tok.Key(), "err", err)
				continue
			}
			for _, pool := range pools {
				key := pool.Key()
				if sawIt[key] == nil {
					sawIt[key] = map[string]bool{}
				}
				sawIt[key][p.Name()] = true

				existing, ok := merged[key]
				if !ok {
					merged[key] = pool
				} else {
					merged[key] = enrich(existing, pool)
				}
				if perToken[tok.Key()] == nil {
					perToken[tok.Key()] = map[string]bool{}
				}
				perToken[tok.Key()][key] = true

				// Remember the counterparty so it can be priced.
				if pool.QuoteAddr != "" && !strings.EqualFold(pool.QuoteAddr, tok.Address) {
					qk := priceKey(pool.Chain, pool.QuoteAddr)
					quotes[qk] = domain.Token{
						Chain:   pool.Chain,
						Address: pool.QuoteAddr,
						Symbol:  pool.QuoteSymbol,
					}
				}
				if tok.Symbol == "" && pool.BaseSymbol != "" &&
					strings.EqualFold(pool.BaseAddr, tok.Address) {
					res.Symbols[tok.Key()] = pool.BaseSymbol
				}
			}
		}
	}

	for tokenKey, keys := range perToken {
		for key := range keys {
			res.ByToken[tokenKey] = append(res.ByToken[tokenKey], merged[key])
		}
		sortByVolume(res.ByToken[tokenKey])
	}

	for key, pool := range merged {
		res.ByChain[pool.Chain] = append(res.ByChain[pool.Chain], pool)
		if len(sawIt[key]) == 1 && len(d.providers) > 1 {
			for name := range sawIt[key] {
				res.ExclusiveTo[name] = append(res.ExclusiveTo[name], pool.Address)
			}
		}
	}
	// Deepest pools first, so a truncated view still holds the liquidity that
	// matters most.
	for chain := range res.ByChain {
		pools := res.ByChain[chain]
		sort.Slice(pools, func(i, j int) bool {
			if pools[i].LiquidityUSD != pools[j].LiquidityUSD {
				return pools[i].LiquidityUSD > pools[j].LiquidityUSD
			}
			return pools[i].Address < pools[j].Address
		})
		res.ByChain[chain] = pools
	}

	for _, q := range quotes {
		res.QuoteAssets = append(res.QuoteAssets, q)
	}
	sort.Slice(res.QuoteAssets, func(i, j int) bool {
		return res.QuoteAssets[i].Address < res.QuoteAssets[j].Address
	})
	for name := range res.ExclusiveTo {
		sort.Strings(res.ExclusiveTo[name])
	}
	return res
}

// sortByVolume orders pools by reported 24h volume, deepest first.
//
// Volume rather than liquidity, because that is what backfill truncates on: the
// spec is explicit that "percent of pools covered" and "percent of volume
// covered" are different numbers, and only the second one matters.
func sortByVolume(pools []domain.Pool) {
	sort.Slice(pools, func(i, j int) bool {
		if pools[i].Volume24hUSD != pools[j].Volume24hUSD {
			return pools[i].Volume24hUSD > pools[j].Volume24hUSD
		}
		return pools[i].Address < pools[j].Address
	})
}

// enrich fills gaps in a pool record from a second provider's view of it.
// Providers disagree on which fields they populate, so the union is strictly
// more useful than whichever one happened to be asked first.
func enrich(dst, src domain.Pool) domain.Pool {
	if dst.DEX == "" {
		dst.DEX = src.DEX
	}
	if dst.BaseAddr == "" {
		dst.BaseAddr = src.BaseAddr
	}
	if dst.BaseSymbol == "" {
		dst.BaseSymbol = src.BaseSymbol
	}
	if dst.QuoteAddr == "" {
		dst.QuoteAddr = src.QuoteAddr
	}
	if dst.QuoteSymbol == "" {
		dst.QuoteSymbol = src.QuoteSymbol
	}
	// Liquidity and volume come from whichever provider reports more, since a
	// zero almost always means "not reported" rather than "empty pool".
	if src.LiquidityUSD > dst.LiquidityUSD {
		dst.LiquidityUSD = src.LiquidityUSD
	}
	if src.Volume24hUSD > dst.Volume24hUSD {
		dst.Volume24hUSD = src.Volume24hUSD
	}
	if !strings.Contains(dst.Source, src.Source) {
		dst.Source = dst.Source + "+" + src.Source
	}
	return dst
}
