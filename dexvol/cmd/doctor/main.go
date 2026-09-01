// Command doctor runs every check that has to pass before the monitor can do
// its job, and prints them all at once.
//
// It exists because this project was built where outbound network access was
// blocked, so nothing in it had ever touched a live API. Several things are
// therefore assumptions until a real machine confirms them: the GeckoTerminal
// network ids, the OKX link pattern, whether the public RPC endpoints answer
// at all. Rather than leaving those to be discovered one confusing symptom at
// a time, this command probes them and says what to fix.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/dexscreener"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/evmrpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/geckoterminal"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources/solanarpc"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/telegram"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fmt.Println("DEX Volume Anomaly Monitor — preflight")

	r := &report{}
	// Read the same env file the service reads, so preflight checks the
	// configuration that will actually be used rather than the shell's.
	if err := config.LoadEnvFile(""); err != nil {
		r.fail("Config", "env file", err.Error(), "fix the file or delete it and set the environment directly")
	}
	checkConfig(ctx, r)
	checkStorage(r)
	checkAggregators(ctx, r)
	checkRPC(ctx, r)
	checkLinks(ctx, r)

	if r.print() > 0 {
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func checkConfig(ctx context.Context, r *report) {
	const group = "Telegram"

	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		r.fail(group, "bot token", "TELEGRAM_BOT_TOKEN is not set",
			"create a bot with @BotFather and export the token it gives you")
	}

	raw := strings.TrimSpace(os.Getenv("TELEGRAM_OWNER_ID"))
	switch {
	case raw == "":
		r.fail(group, "owner id", "TELEGRAM_OWNER_ID is not set",
			"ask @userinfobot for your numeric id; without it the bot would take orders from anyone")
	default:
		if id, err := strconv.ParseInt(raw, 10, 64); err != nil {
			r.fail(group, "owner id", fmt.Sprintf("%q is not a number", raw),
				"TELEGRAM_OWNER_ID must be the numeric id, not the @username")
		} else {
			r.ok(group, "owner id", fmt.Sprintf("%d — the only account the bot will answer", id))
		}
	}

	if token == "" {
		return
	}
	// getMe both validates the token and proves Telegram is reachable. The
	// token itself is never printed: this output is the kind of thing people
	// paste into a chat when asking for help.
	me, err := telegram.NewClient(token).GetMe(ctx)
	if err != nil {
		r.fail(group, "bot token", "Telegram rejected it or is unreachable: "+redact(err.Error(), token),
			"check the token, and that outbound HTTPS to api.telegram.org is allowed")
		return
	}
	name := me.Username
	if name == "" {
		name = me.FirstName
	}
	r.ok(group, "bot token", "valid — @"+name)
}

// redact keeps a bot token from leaking through an error string.
func redact(s, secret string) string {
	if secret == "" {
		return s
	}
	return strings.ReplaceAll(s, secret, "<token>")
}

func checkStorage(r *report) {
	const group = "Storage"

	statePath := env("STATE_PATH", "state.json")
	if err := writable(filepath.Dir(statePath)); err != nil {
		r.fail(group, "state file", fmt.Sprintf("cannot write near %s: %v", statePath, err),
			"point STATE_PATH somewhere writable; settings changed from the bot are saved there")
	} else {
		r.ok(group, "state file", statePath)
	}

	dbPath := env("DB_PATH", "dexvol-data")
	if err := os.MkdirAll(dbPath, 0o700); err != nil {
		r.fail(group, "data directory", fmt.Sprintf("cannot create %s: %v", dbPath, err),
			"point DB_PATH somewhere writable; minute volumes and raw trades live there")
		return
	}
	if err := writable(dbPath); err != nil {
		r.fail(group, "data directory", fmt.Sprintf("cannot write in %s: %v", dbPath, err), "check permissions")
		return
	}
	r.ok(group, "data directory", dbPath)
}

func writable(dir string) error {
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, ".dexvol-preflight-*")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}

func checkAggregators(ctx context.Context, r *report) {
	const group = "Aggregators"

	ds := dexscreener.New()
	probed := 0
	for _, info := range domain.Chains() {
		addr, ok := probeToken[info.Chain]
		if !ok || info.DexScreenerID == "" {
			continue
		}
		pools, err := ds.DiscoverPools(ctx, domain.Token{Chain: info.Chain, Address: addr})
		if err != nil {
			// If the provider itself is unreachable every chain fails the same
			// way, and eight identical lines only bury the rest of the report.
			// Say it once and move on.
			r.fail(group, "dexscreener", err.Error(),
				"check outbound HTTPS to api.dexscreener.com; discovery and pricing both depend on it")
			probed = -1
			break
		}
		probed++
		if len(pools) == 0 {
			r.warn(group, "dexscreener "+string(info.Chain),
				"answered, but found no pools for the probe token",
				"either the probe address is wrong or this chain is not indexed — check one of your own tokens with `coverage -discovery-only`")
			continue
		}
		r.ok(group, "dexscreener "+string(info.Chain), fmt.Sprintf("%d pools for the probe token", len(pools)))
	}
	if probed == 0 {
		r.fail(group, "dexscreener", "no chain could be probed", "see the failures above")
	}
	if probed < 0 {
		// The aggregators share a transport; if one is refused the other will
		// be too, and its error adds nothing.
		return
	}

	gt := geckoterminal.New()
	networks, err := gt.Networks(ctx, 20)
	if err != nil {
		r.fail(group, "geckoterminal", err.Error(),
			"check outbound HTTPS to api.geckoterminal.com; the second discovery opinion and the 24h history backfill both depend on it")
		return
	}
	known := make(map[string]string, len(networks))
	for _, n := range networks {
		known[n.ID] = n.Name
	}
	r.ok(group, "geckoterminal", fmt.Sprintf("%d networks listed", len(networks)))

	// A wrong network id is the quietest failure in the project: requests 404,
	// the adapter reports "no pools", and the chain silently loses its second
	// discovery opinion and its history backfill while nothing looks broken.
	for _, info := range domain.Chains() {
		name := "geckoterminal id " + string(info.Chain)
		switch {
		case info.GeckoTerminalID == "":
			r.warn(group, name, "not configured",
				"this chain runs on one discovery provider and cannot backfill history; run `coverage -list-networks "+string(info.Chain)+"` to see whether the provider indexes it under another name")
		case known[info.GeckoTerminalID] != "":
			r.ok(group, name, fmt.Sprintf("%s — %s", info.GeckoTerminalID, known[info.GeckoTerminalID]))
		default:
			r.fail(group, name, fmt.Sprintf("%q is not in the provider's list", info.GeckoTerminalID),
				"run `coverage -verify-networks` for the full list and correct chainRegistry in internal/domain/chains.go")
		}
	}
}

func checkRPC(ctx context.Context, r *report) {
	const group = "RPC endpoints"

	for _, info := range domain.Chains() {
		url := env(info.RPCEnv, info.DefaultRPC)
		name := string(info.Chain)
		if url == "" {
			r.warn(group, name, "no endpoint configured", "set "+info.RPCEnv+" or this chain will not be ingested")
			continue
		}

		if info.Chain == domain.ChainSolana {
			slot, err := solanarpc.NewRPC(url, "", 240).Slot(ctx)
			if err != nil {
				r.fail(group, name, err.Error(), "set "+info.RPCEnv+" to a reachable endpoint")
				continue
			}
			r.ok(group, name, fmt.Sprintf("slot %d", slot))
			continue
		}
		if !info.EVM {
			continue
		}

		rpc := evmrpc.NewRPC(name, url, 120)
		got, err := rpc.ChainID(ctx)
		if err != nil {
			r.fail(group, name, err.Error(), "set "+info.RPCEnv+" to a reachable endpoint")
			continue
		}
		// The mistake nothing downstream would catch: a healthy node for the
		// wrong network. The service would poll it forever and report that
		// every token on that chain has no trades.
		if got != info.ChainID {
			r.fail(group, name, fmt.Sprintf("endpoint reports chain id %d, expected %d", got, info.ChainID),
				"the URL in "+info.RPCEnv+" points at a different network")
			continue
		}
		head, err := rpc.BlockNumber(ctx)
		if err != nil {
			r.warn(group, name, "chain id matches but the head block is unreadable: "+err.Error(),
				"the endpoint may be rate limiting; consider your own RPC")
			continue
		}
		r.ok(group, name, fmt.Sprintf("chain id %d, head %d", got, head))
	}
}

func checkLinks(ctx context.Context, r *report) {
	const group = "Alert buttons"

	client := &http.Client{Timeout: 15 * time.Second}
	// Once the network refuses one of these it will refuse them all, so stop
	// after the first transport failure rather than printing a dozen copies.
	for _, info := range domain.Chains() {
		addr, ok := probeToken[info.Chain]
		if !ok {
			continue
		}
		tok := domain.Token{Chain: info.Chain, Address: addr}

		for _, l := range alert.Links(tok) {
			name := strings.ToLower(l.Text) + " " + string(info.Chain)
			status, err := probeURL(ctx, client, l.URL)
			switch {
			case err != nil:
				r.warn(group, "alert links", "unreachable: "+err.Error(),
					"the buttons could not be checked from here; open one by hand to confirm the pattern")
				return
			case status >= 200 && status < 400:
				r.ok(group, name, fmt.Sprintf("http %d", status))
			case status == http.StatusForbidden || status == http.StatusTooManyRequests:
				// Bot protection, not a broken link. Sites like GMGN sit
				// behind a challenge that a plain HTTP client cannot pass,
				// and the same URL opens perfectly in a browser.
				r.warn(group, name, fmt.Sprintf("http %d — looks like bot protection", status),
					"open one of these in a browser once to confirm the pattern; a 403 here is expected and does not affect the buttons")
			case l.Text == "OKX":
				// This pattern is the one that could not be confirmed against
				// a live page while the project was built.
				r.warn(group, name, fmt.Sprintf("http %d", status),
					"open "+l.URL+"; if the real page differs, set OKX_URL_TEMPLATE with {chain} and {address}, or set it empty to drop the button")
			default:
				r.warn(group, name, fmt.Sprintf("http %d", status),
					"open "+l.URL+" by hand; the site may simply be refusing automated requests")
			}
		}
	}
}

func probeURL(ctx context.Context, client *http.Client, url string) (int, error) {
	// GET rather than HEAD: several of these sites answer HEAD with 405 while
	// serving the page perfectly well.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "dexvol-monitor/1.0 preflight")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}
