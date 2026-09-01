// Command coverage measures how much of a token's real traded volume this
// service's pipeline actually reconstructs.
//
// It exists because the spec forbids claiming a coverage figure without
// evidence, and because the figure cannot be produced anywhere except a machine
// with open network access. Run it, then paste its markdown table into
// docs/RESEARCH.md §6.
//
// Two modes:
//
//	-discovery-only   compare pool sets between providers, seconds to run
//	(default)         collect for -duration, then compare volume against each
//	                  provider's own aggregate for the same window
//
// A word on what the numbers mean. There is no absolute reference: DEX Screener
// and GeckoTerminal are indexers with their own gaps, so a ratio above 100% is
// not a bug, it means the on-chain pipeline saw pools that indexer missed. Read
// the three figures as a corridor, not as a graded exam.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/service"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/dexscreener"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/evmrpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/geckoterminal"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/solanarpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		tokensFlag = flag.String("tokens", "",
			"comma-separated chain:address[:SYMBOL] entries, e.g. base:0xabc...:ABC,solana:So111...:SOL")
		duration      = flag.Duration("duration", time.Hour, "how long to collect before comparing")
		discoveryOnly = flag.Bool("discovery-only", false, "compare pool sets and exit without collecting")
		pollInterval  = flag.Duration("poll", 12*time.Second, "how often to poll for trades")
		verbose       = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	tokens, err := parseTokens(*tokensFlag)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return fmt.Errorf("no tokens given; pass -tokens chain:address[:SYMBOL],...")
	}

	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ds := dexscreener.New()
	gt := geckoterminal.New()
	discovery := service.NewDiscovery(log, ds, gt)

	fmt.Println("## Pool discovery")
	fmt.Println()
	res := discovery.Run(ctx, tokens)
	reportDiscovery(res)

	if *discoveryOnly {
		return nil
	}

	prices := service.NewPriceCache(ds, log)
	prices.TrackQuoteAssets(res.QuoteAssets)
	if err := prices.Refresh(ctx, tokens); err != nil {
		log.Warn("initial price refresh failed", "err", err)
	}

	engine := volume.NewEngine()
	srcs := buildSources(res, tokens, prices, log)
	if len(srcs) == 0 {
		return fmt.Errorf("no chain in the token list has an ingestion adapter")
	}

	start := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	fmt.Printf("\nCollecting from %s UTC for %s. Ctrl-C stops early and still reports.\n\n",
		start.Format("15:04"), *duration)

	collect(ctx, srcs, engine, prices, tokens, start, *duration, *pollInterval, log)

	end := time.Now().UTC().Truncate(time.Minute)
	fmt.Println("\n## Coverage")
	fmt.Println()
	return reportCoverage(ctx, engine, tokens, res, ds, gt, start, end)
}

// parseTokens reads the -tokens flag.
func parseTokens(s string) ([]domain.Token, error) {
	var out []domain.Token
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("token %q must look like chain:address[:SYMBOL]", entry)
		}
		chain, err := parseChain(parts[0])
		if err != nil {
			return nil, err
		}
		tok := domain.Token{Chain: chain, Address: parts[1], Enabled: true}
		if len(parts) >= 3 {
			tok.Symbol = strings.ToUpper(parts[2])
		}
		out = append(out, tok)
	}
	return out, nil
}

func parseChain(s string) (domain.Chain, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "eth", "ethereum":
		return domain.ChainEthereum, nil
	case "bsc", "bnb":
		return domain.ChainBNB, nil
	case "sol", "solana":
		return domain.ChainSolana, nil
	case "base":
		return domain.ChainBase, nil
	case "rh", "robinhood":
		return domain.ChainRobinhood, nil
	}
	return "", fmt.Errorf("unknown chain %q", s)
}

func buildSources(res service.DiscoveryResult, tokens []domain.Token, prices *service.PriceCache, log *slog.Logger) map[domain.Chain]service.TradeSource {
	endpoints := map[domain.Chain]string{
		domain.ChainEthereum:  env("RPC_ETHEREUM", "https://ethereum-rpc.publicnode.com"),
		domain.ChainBNB:       env("RPC_BSC", "https://bsc-rpc.publicnode.com"),
		domain.ChainBase:      env("RPC_BASE", "https://base-rpc.publicnode.com"),
		domain.ChainRobinhood: env("RPC_ROBINHOOD", "https://rpc.mainnet.chain.robinhood.com"),
		domain.ChainSolana:    env("RPC_SOLANA", "https://api.mainnet-beta.solana.com"),
	}

	needed := map[domain.Chain]bool{}
	for _, t := range tokens {
		needed[t.Chain] = true
	}

	out := map[domain.Chain]service.TradeSource{}
	for chain := range needed {
		url := endpoints[chain]
		if url == "" {
			continue
		}
		var src service.TradeSource
		if chain == domain.ChainSolana {
			src = solanarpc.NewSource(solanarpc.NewRPC(url, 240), prices, solanarpc.DefaultOptions(), log)
		} else if chain.IsEVM() {
			src = evmrpc.NewSource(chain, evmrpc.NewRPC(string(chain), url, 120), prices, evmrpc.DefaultOptions(), log)
		} else {
			continue
		}
		src.SetPools(res.ByChain[chain])
		src.SetTokens(tokens)
		out[chain] = src
	}
	return out
}

// collect polls the sources and seals minutes for the measurement window.
func collect(
	ctx context.Context,
	srcs map[domain.Chain]service.TradeSource,
	engine *volume.Engine,
	prices *service.PriceCache,
	tokens []domain.Token,
	start time.Time,
	duration, poll time.Duration,
	log *slog.Logger,
) {
	trades := make(chan domain.Trade, 4096)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case tr := <-trades:
				engine.Ingest(tr)
			}
		}
	}()

	// A chain's health for a minute mirrors the running service: a failed poll
	// makes the minute MISSING so it never counts toward the coverage sum.
	healthy := map[domain.Chain]map[int64]bool{}
	markHealth := func(chain domain.Chain, at time.Time, ok bool) {
		if healthy[chain] == nil {
			healthy[chain] = map[int64]bool{}
		}
		k := at.UTC().Truncate(time.Minute).Unix()
		if !ok {
			healthy[chain][k] = false
			return
		}
		if _, seen := healthy[chain][k]; !seen {
			healthy[chain][k] = true
		}
	}

	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	sealTicker := time.NewTicker(10 * time.Second)
	defer sealTicker.Stop()

	lastSealed := start.Add(-time.Minute)
	seal := func(now time.Time) {
		limit := now.Add(-20 * time.Second).Truncate(time.Minute)
		for m := lastSealed.Add(time.Minute); !m.After(limit); m = m.Add(time.Minute) {
			for _, tok := range tokens {
				ok := false
				if byMinute, has := healthy[tok.Chain]; has {
					ok = byMinute[m.Unix()]
				}
				engine.Seal(tok.Key(), m, ok)
			}
			lastSealed = m
		}
	}

	for {
		select {
		case <-ctx.Done():
			seal(time.Now().UTC())
			return
		case <-ticker.C:
			if time.Now().After(deadline) {
				seal(time.Now().UTC())
				return
			}
			for _, src := range srcs {
				err := src.Poll(ctx, trades)
				markHealth(src.Chain(), time.Now().UTC(), err == nil && src.Healthy())
				if err != nil && ctx.Err() == nil {
					log.Warn("poll failed", "chain", src.Chain(), "err", err)
				}
			}
			if err := prices.Refresh(ctx, tokens); err != nil && ctx.Err() == nil {
				log.Warn("price refresh failed", "err", err)
			}
			fmt.Printf("\rcollecting… %s remaining ", time.Until(deadline).Round(time.Second))
		case now := <-sealTicker.C:
			seal(now.UTC())
		}
	}
}

func reportDiscovery(res service.DiscoveryResult) {
	chains := make([]domain.Chain, 0, len(res.ByChain))
	for c := range res.ByChain {
		chains = append(chains, c)
	}
	sort.Slice(chains, func(i, j int) bool { return chains[i] < chains[j] })

	fmt.Println("| Chain | Pools | Liquidity (USD) |")
	fmt.Println("|---|---|---|")
	for _, c := range chains {
		var liq float64
		for _, p := range res.ByChain[c] {
			liq += p.LiquidityUSD
		}
		fmt.Printf("| %s | %d | %s |\n", c, len(res.ByChain[c]), alert.FormatUSD(liq))
	}

	if len(res.ExclusiveTo) == 0 {
		fmt.Println("\nBoth providers found the same pools.")
		return
	}
	// This is the real payoff of running two discoverers: any pool listed here
	// would have been invisible to a single-provider setup.
	fmt.Println("\nPools found by only one provider — a single-provider setup would have missed these:")
	names := make([]string, 0, len(res.ExclusiveTo))
	for name := range res.ExclusiveTo {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("\n- **%s** (%d):\n", name, len(res.ExclusiveTo[name]))
		for _, addr := range res.ExclusiveTo[name] {
			fmt.Printf("  - `%s`\n", addr)
		}
	}
}

func reportCoverage(
	ctx context.Context,
	engine *volume.Engine,
	tokens []domain.Token,
	res service.DiscoveryResult,
	ds *dexscreener.Client,
	gt *geckoterminal.Client,
	start, end time.Time,
) error {
	window := end.Sub(start)
	fmt.Printf("Window: %s → %s UTC (%s)\n\n", start.Format("15:04"), end.Format("15:04"), window.Round(time.Minute))
	fmt.Println("| Token | Chain | Ours | Minutes | DexScreener | vs DS | GeckoTerminal | vs GT |")
	fmt.Println("|---|---|---|---|---|---|---|---|")

	for _, tok := range tokens {
		ours, healthy, sealed := engine.Sum(tok.Key(), start, end)

		dsRef := scaled(fetch(ctx, ds, tok), window)
		gtRef := 0.0
		if gt.Supports(tok.Chain) {
			gtRef = scaled(fetch(ctx, gt, tok), window)
		}

		fmt.Printf("| %s | %s | %s | %d/%d | %s | %s | %s | %s |\n",
			label(tok), tok.Chain,
			alert.FormatUSD(ours), healthy, sealed,
			usdOrDash(dsRef), ratio(ours, dsRef),
			usdOrDash(gtRef), ratio(ours, gtRef))
	}

	fmt.Println()
	if len(tokens) > 0 {
		_, healthy, sealed := engine.Sum(tokens[0].Key(), start, end)
		if sealed > 0 && healthy < sealed {
			fmt.Printf("⚠ %d of %d minutes were MISSING and are excluded from the sums above. "+
				"A coverage ratio measured over a partially covered window understates the pipeline "+
				"rather than the sources — rerun once ingestion is stable.\n\n", sealed-healthy, sealed)
		}
	}
	fmt.Println("Neither reference is ground truth: both are indexers with their own gaps, so a")
	fmt.Println("ratio above 100% means the on-chain pipeline saw pools that indexer did not.")
	return nil
}

func fetch(ctx context.Context, src sources.ReferenceVolume, tok domain.Token) float64 {
	ref, err := src.Volume(ctx, tok)
	if err != nil {
		return 0
	}
	return ref.H1USD
}

// scaled converts a provider's 1-hour aggregate to the measurement window, so a
// 20-minute run is not compared against a full hour of reference volume.
func scaled(h1 float64, window time.Duration) float64 {
	if h1 <= 0 || window <= 0 {
		return 0
	}
	return h1 * (window.Minutes() / 60.0)
}

func ratio(ours, reference float64) string {
	if reference <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.1f%%", ours/reference*100)
}

func usdOrDash(v float64) string {
	if v <= 0 {
		return "—"
	}
	return alert.FormatUSD(v)
}

func label(t domain.Token) string {
	if t.Symbol != "" {
		return t.Symbol
	}
	if len(t.Address) > 10 {
		return t.Address[:10] + "…"
	}
	return t.Address
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
