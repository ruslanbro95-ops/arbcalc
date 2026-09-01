// Command monitor runs the DEX volume anomaly monitor.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/service"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/dexscreener"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/evmrpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/geckoterminal"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/solanarpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/store"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/telegram"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	static, err := config.LoadStatic()
	if err != nil {
		return err
	}
	log := newLogger(static.LogLevel)

	settings := config.NewStore(static.StatePath)
	if err := settings.Load(); err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	db, err := store.Open(static.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer db.Close()

	engine := volume.NewEngine()

	// Replaying persisted minutes is what makes a restart cheap: without it a
	// 24h baseline would need a full day before it could judge anything.
	restored, err := db.Restore(engine, settings.Get().Tokens, time.Now().UTC())
	if err != nil {
		log.Warn("could not fully restore history", "err", err)
	}
	log.Info("restored persisted minutes", "rows", restored)

	ds := dexscreener.New()
	gt := geckoterminal.New()

	prices := service.NewPriceCache(ds, log)
	discovery := service.NewDiscovery(log, ds, gt)
	alerts := alert.NewManager()

	sources, err := buildSources(static, prices, log)
	if err != nil {
		return err
	}

	svc := service.New(static, settings, db, engine, discovery, prices, alerts, sources, log)

	client := telegram.NewClient(static.TelegramToken)
	bot := telegram.NewBot(client, static.OwnerID, settings, alerts, svc, log)
	svc.SetNotifier(bot)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 2)
	go func() { errs <- svc.Run(ctx) }()
	go func() { errs <- bot.Run(ctx) }()

	log.Info("monitor started",
		"tokens", len(settings.Get().Tokens),
		"poll_interval", static.PollInterval,
		"chains", len(sources))

	// The first error wins; the shared context then unwinds the other loop.
	err = <-errs
	stop()
	<-errs

	if ctx.Err() != nil {
		log.Info("shutting down")
		return nil
	}
	return err
}

// buildSources creates one trade source per configured chain.
func buildSources(static config.Static, prices *service.PriceCache, log *slog.Logger) (map[domain.Chain]service.TradeSource, error) {
	out := map[domain.Chain]service.TradeSource{}

	for chain, url := range static.RPCEndpoints {
		if url == "" {
			log.Warn("no RPC endpoint configured; this chain will not be ingested", "chain", chain)
			continue
		}
		if chain == domain.ChainSolana {
			out[chain] = solanarpc.NewSource(
				solanarpc.NewRPC(url, rpcBudget(chain)),
				prices, solanarpc.DefaultOptions(), log)
			continue
		}
		if !chain.IsEVM() {
			log.Warn("no ingestion adapter for chain", "chain", chain)
			continue
		}
		out[chain] = evmrpc.NewSource(chain,
			evmrpc.NewRPC(string(chain), url, rpcBudget(chain)),
			prices, evmrpc.DefaultOptions(), log)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no chains could be ingested; check the RPC_* environment variables")
	}
	return out, nil
}

// rpcBudget is the per-minute request allowance assumed for an endpoint.
//
// Solana gets the larger share because its ingestion cannot batch across pools
// the way eth_getLogs does; see docs/RESEARCH.md §3.7. Point the RPC_* variables
// at your own endpoints and these numbers can go up.
func rpcBudget(chain domain.Chain) int {
	if chain == domain.ChainSolana {
		return 240
	}
	return 120
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
