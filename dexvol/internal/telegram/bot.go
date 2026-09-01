package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// Controller is the slice of the running service the bot is allowed to touch.
// Keeping it narrow means a bot bug cannot reach into ingestion.
type Controller interface {
	// Snapshot returns the latest sealed minute for a token.
	Snapshot(tok domain.Token) (volume.Snapshot, bool)
	// Stats exposes ingestion counters for the /status view.
	Stats() volume.Stats
	// TokensChanged tells the service the watch list was edited, so pool
	// discovery runs for a newly added token instead of waiting for the next
	// refresh tick.
	TokensChanged()
}

// Bot is the owner's control panel and the alert channel.
type Bot struct {
	client  *Client
	ownerID int64
	store   *config.Store
	alerts  *alert.Manager
	ctrl    Controller
	log     *slog.Logger
}

func NewBot(c *Client, ownerID int64, store *config.Store, alerts *alert.Manager, ctrl Controller, log *slog.Logger) *Bot {
	return &Bot{client: c, ownerID: ownerID, store: store, alerts: alerts, ctrl: ctrl, log: log}
}

// Notify delivers an alert to the owner. Alerts go to the owner's private chat
// and nowhere else — the same id that gates the commands.
func (b *Bot) Notify(ctx context.Context, m alert.Message) error {
	buttons := make([]InlineButton, 0, len(m.Links))
	for _, l := range m.Links {
		buttons = append(buttons, InlineButton{Text: l.Text, URL: l.URL})
	}
	return b.client.Send(ctx, b.ownerID, m.Text, buttons)
}

// Run long-polls for commands until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) error {
	if err := b.client.DeleteWebhook(ctx); err != nil {
		b.log.Warn("could not clear webhook", "err", err)
	}
	me, err := b.client.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("telegram auth failed: %w", err)
	}
	b.log.Info("telegram bot ready", "bot", me.Username, "owner_id", b.ownerID)

	offset := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		updates, err := b.client.GetUpdates(ctx, offset, 30)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			b.log.Warn("getUpdates failed", "err", err)
			// Back off briefly so a persistent failure does not spin.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
			continue
		}
		for _, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			b.handle(ctx, u)
		}
	}
}

// handle enforces the ownership check before anything else.
//
// Unauthorized updates are dropped without a reply: this bot holds the watch
// list and the alerting configuration, and answering a stranger would confirm
// the bot exists and is live.
func (b *Bot) handle(ctx context.Context, u Update) {
	if u.Message == nil || u.Message.From == nil {
		return
	}
	if u.Message.From.ID != b.ownerID {
		b.log.Warn("rejected message from a non-owner",
			"from_id", u.Message.From.ID, "username", u.Message.From.Username)
		return
	}

	cmd, args := splitCommand(u.Message.Text)
	reply, err := b.dispatch(cmd, args)
	if err != nil {
		reply = "⚠ " + err.Error()
	}
	if reply == "" {
		return
	}
	if err := b.client.Send(ctx, u.Message.Chat.ID, reply, nil); err != nil {
		b.log.Error("reply failed", "err", err)
	}
}

func (b *Bot) dispatch(cmd string, args []string) (string, error) {
	switch cmd {
	case "/start", "/help":
		return helpText, nil
	case "/add":
		return b.cmdAdd(args)
	case "/remove", "/rm":
		return b.cmdRemove(args)
	case "/list":
		return b.cmdList()
	case "/chains":
		return b.cmdChains()
	case "/threshold":
		return b.cmdThreshold(args)
	case "/cooldown":
		return b.cmdCooldown(args)
	case "/escalation":
		return b.cmdEscalation(args)
	case "/windows":
		return b.cmdWindows(args)
	case "/on":
		return b.cmdMonitoring(true)
	case "/off":
		return b.cmdMonitoring(false)
	case "/settings":
		return b.cmdSettings()
	case "/status":
		return b.cmdStatus()
	case "/vol":
		return b.cmdVol(args)
	case "":
		return "", nil
	default:
		return "", fmt.Errorf("unknown command %s — send /help", cmd)
	}
}

const helpText = `DEX Volume Anomaly Monitor

Tokens
/add <chain> <address> [SYMBOL]
/remove <symbol|address>
/list

Alerting
/threshold [percent]      trigger level, e.g. /threshold 30
/cooldown [minutes]       repeat suppression
/escalation [factor]      how much stronger an anomaly must be to break cooldown
/windows [10|30|60|24h on|off]

Service
/on  /off                 monitoring switch
/settings                 current configuration
/status                   ingestion and data quality
/vol <symbol>             volume right now
/chains                   supported networks`

var (
	evmAddr    = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	solanaAddr = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]{32,44}$`)
)

// validateAddress catches a mistyped address at entry instead of letting it
// become a token that silently never produces data.
func validateAddress(chain domain.Chain, addr string) error {
	if chain == domain.ChainSolana {
		if !solanaAddr.MatchString(addr) {
			return fmt.Errorf("%q is not a valid Solana address", addr)
		}
		return nil
	}
	if !evmAddr.MatchString(addr) {
		return fmt.Errorf("%q is not a valid EVM address (expected 0x + 40 hex characters)", addr)
	}
	return nil
}

func (b *Bot) cmdAdd(args []string) (string, error) {
	if len(args) < 2 {
		return "", errors.New("usage: /add <chain> <address> [SYMBOL]")
	}
	chain, err := domain.ParseChain(args[0])
	if err != nil {
		return "", err
	}
	addr := args[1]
	if err := validateAddress(chain, addr); err != nil {
		return "", err
	}

	symbol := ""
	if len(args) >= 3 {
		symbol = strings.ToUpper(args[2])
	}
	tok := domain.Token{Symbol: symbol, Chain: chain, Address: addr, Enabled: true}

	var dup bool
	err = b.store.Update(func(rt *config.Runtime) {
		for _, t := range rt.Tokens {
			if t.Key() == tok.Key() {
				dup = true
				return
			}
		}
		rt.Tokens = append(rt.Tokens, tok)
	})
	if err != nil {
		return "", err
	}
	if dup {
		return "already tracked: " + tok.Key(), nil
	}

	b.ctrl.TokensChanged()
	if symbol == "" {
		return fmt.Sprintf("added %s\nsymbol will be resolved by pool discovery", tok.Key()), nil
	}
	return fmt.Sprintf("added %s (%s)", symbol, tok.Key()), nil
}

func (b *Bot) cmdRemove(args []string) (string, error) {
	if len(args) < 1 {
		return "", errors.New("usage: /remove <symbol|address>")
	}
	needle := strings.ToLower(args[0])

	var removed []domain.Token
	err := b.store.Update(func(rt *config.Runtime) {
		kept := rt.Tokens[:0]
		for _, t := range rt.Tokens {
			if strings.ToLower(t.Symbol) == needle || strings.ToLower(t.Address) == needle {
				removed = append(removed, t)
				continue
			}
			kept = append(kept, t)
		}
		rt.Tokens = kept
	})
	if err != nil {
		return "", err
	}
	if len(removed) == 0 {
		return "no token matched " + args[0], nil
	}
	// Drop the cooldown state too, so re-adding the token alerts immediately
	// instead of inheriting a suppression window it cannot see.
	for _, t := range removed {
		b.alerts.Reset(t.Key())
	}
	b.ctrl.TokensChanged()

	var sb strings.Builder
	sb.WriteString("removed:")
	for _, t := range removed {
		fmt.Fprintf(&sb, "\n%s", t.Key())
	}
	return sb.String(), nil
}

// cmdChains lists what the registry supports and how well each network is
// covered, so the owner can see before adding a token whether it will get one
// discovery provider or two, and whether history can be backfilled.
func (b *Bot) cmdChains() (string, error) {
	var sb strings.Builder
	sb.WriteString("supported networks:")
	for _, info := range domain.Chains() {
		providers := 0
		if info.DexScreenerID != "" {
			providers++
		}
		if info.GeckoTerminalID != "" {
			providers++
		}
		note := ""
		if info.GeckoTerminalID == "" {
			// Backfill and the second discovery opinion both ride on
			// GeckoTerminal, so its absence is worth stating up front.
			note = " · 1 provider, no history backfill"
		}
		fmt.Fprintf(&sb, "\n%s%s", info.Chain, note)
		_ = providers
	}
	return sb.String(), nil
}

func (b *Bot) cmdList() (string, error) {
	rt := b.store.Get()
	if len(rt.Tokens) == 0 {
		return "watch list is empty — /add <chain> <address>", nil
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "tracking %d token(s):", len(rt.Tokens))
	for _, t := range rt.Tokens {
		symbol := t.Symbol
		if symbol == "" {
			symbol = "?"
		}
		state := ""
		if !t.Enabled {
			state = " [paused]"
		}
		fmt.Fprintf(&sb, "\n%s · %s · %s%s", symbol, t.Chain, t.Address, state)
	}
	return sb.String(), nil
}

func (b *Bot) cmdThreshold(args []string) (string, error) {
	if len(args) == 0 {
		return fmt.Sprintf("threshold: +%.0f%%", b.store.Get().ThresholdPct), nil
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(args[0], "%"), 64)
	if err != nil {
		return "", fmt.Errorf("%q is not a number", args[0])
	}
	if v <= 0 || v > 100000 {
		return "", errors.New("threshold must be between 0 and 100000 percent")
	}
	if err := b.store.Update(func(rt *config.Runtime) { rt.ThresholdPct = v }); err != nil {
		return "", err
	}
	return fmt.Sprintf("threshold set to +%.0f%%", v), nil
}

func (b *Bot) cmdCooldown(args []string) (string, error) {
	if len(args) == 0 {
		return fmt.Sprintf("cooldown: %d min", b.store.Get().CooldownMinutes), nil
	}
	v, err := strconv.Atoi(args[0])
	if err != nil {
		return "", fmt.Errorf("%q is not a whole number of minutes", args[0])
	}
	if v < 0 || v > 1440 {
		return "", errors.New("cooldown must be between 0 and 1440 minutes")
	}
	if err := b.store.Update(func(rt *config.Runtime) { rt.CooldownMinutes = v }); err != nil {
		return "", err
	}
	return fmt.Sprintf("cooldown set to %d min", v), nil
}

func (b *Bot) cmdEscalation(args []string) (string, error) {
	if len(args) == 0 {
		return fmt.Sprintf("escalation factor: x%.2f", b.store.Get().EscalationFactor), nil
	}
	v, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return "", fmt.Errorf("%q is not a number", args[0])
	}
	if v <= 1 {
		return "", errors.New("factor must be greater than 1, otherwise every minute would break the cooldown")
	}
	if err := b.store.Update(func(rt *config.Runtime) { rt.EscalationFactor = v }); err != nil {
		return "", err
	}
	return fmt.Sprintf("escalation factor set to x%.2f", v), nil
}

var windowAliases = map[string]int{
	"10": volume.Window10m, "10m": volume.Window10m,
	"30": volume.Window30m, "30m": volume.Window30m,
	"60": volume.Window60m, "60m": volume.Window60m, "1h": volume.Window60m,
	"24h": volume.Window24h, "1440": volume.Window24h, "24": volume.Window24h,
}

func (b *Bot) cmdWindows(args []string) (string, error) {
	rt := b.store.Get()
	if len(args) == 0 {
		var sb strings.Builder
		sb.WriteString("baseline windows:")
		for _, w := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
			state := "off"
			if rt.Windows[w] {
				state = "on"
			}
			fmt.Fprintf(&sb, "\n%s %s", alert.FormatWindow(w), state)
		}
		return sb.String(), nil
	}
	if len(args) != 2 {
		return "", errors.New("usage: /windows <10|30|60|24h> <on|off>")
	}
	w, ok := windowAliases[strings.ToLower(args[0])]
	if !ok {
		return "", fmt.Errorf("unknown window %q — use 10, 30, 60 or 24h", args[0])
	}
	var on bool
	switch strings.ToLower(args[1]) {
	case "on", "true", "1":
		on = true
	case "off", "false", "0":
		on = false
	default:
		return "", fmt.Errorf("expected on or off, got %q", args[1])
	}

	// Turning off the last window would leave nothing to compare against, so
	// the detector could never fire again — refuse instead of silently going
	// deaf.
	if !on {
		remaining := 0
		for _, other := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
			if other != w && rt.Windows[other] {
				remaining++
			}
		}
		if remaining == 0 {
			return "", errors.New("at least one window must stay enabled, or no alert could ever fire")
		}
	}

	if err := b.store.Update(func(rt *config.Runtime) { rt.Windows[w] = on }); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s", alert.FormatWindow(w), args[1]), nil
}

func (b *Bot) cmdMonitoring(on bool) (string, error) {
	if err := b.store.Update(func(rt *config.Runtime) { rt.Monitoring = on }); err != nil {
		return "", err
	}
	if on {
		return "monitoring ON", nil
	}
	return "monitoring OFF — data collection continues, alerts are held", nil
}

func (b *Bot) cmdSettings() (string, error) {
	rt := b.store.Get()
	state := "ON"
	if !rt.Monitoring {
		state = "OFF"
	}
	var windows []string
	for _, w := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
		if rt.Windows[w] {
			windows = append(windows, alert.FormatWindow(w))
		}
	}
	return fmt.Sprintf(
		"monitoring: %s\nthreshold: +%.0f%%\ncooldown: %d min\nescalation: x%.2f\nwindows: %s\ntokens: %d",
		state, rt.ThresholdPct, rt.CooldownMinutes, rt.EscalationFactor,
		strings.Join(windows, " "), len(rt.Tokens)), nil
}

func (b *Bot) cmdStatus() (string, error) {
	rt := b.store.Get()
	st := b.ctrl.Stats()

	var sb strings.Builder
	fmt.Fprintf(&sb, "trades: %d accepted · %d duplicate · %d late\n",
		st.Accepted, st.Duplicate, st.TooLate)

	if len(rt.Tokens) == 0 {
		sb.WriteString("no tokens tracked")
		return sb.String(), nil
	}
	sb.WriteString("data quality (last 60m):")
	for _, t := range rt.Tokens {
		snap, ok := b.ctrl.Snapshot(t)
		if !ok || snap.TotalMinutes == 0 {
			fmt.Fprintf(&sb, "\n%s · warming up", labelOf(t))
			continue
		}
		day := snap.Baselines[volume.Window24h]
		history := "no 24h baseline yet"
		if day.Usable {
			history = fmt.Sprintf("24h baseline %d samples", day.Samples)
			if day.Backfilled > 0 {
				history += fmt.Sprintf(" (%d backfilled)", day.Backfilled)
			}
		}
		fmt.Fprintf(&sb, "\n%s · %d/%dm healthy · %s",
			labelOf(t), snap.HealthyMinutes, snap.TotalMinutes, history)
	}
	return sb.String(), nil
}

func (b *Bot) cmdVol(args []string) (string, error) {
	if len(args) < 1 {
		return "", errors.New("usage: /vol <symbol>")
	}
	needle := strings.ToLower(args[0])
	rt := b.store.Get()

	for _, t := range rt.Tokens {
		if strings.ToLower(t.Symbol) != needle && strings.ToLower(t.Address) != needle {
			continue
		}
		snap, ok := b.ctrl.Snapshot(t)
		if !ok {
			return labelOf(t) + " · no data yet", nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "%s\n%s at %s UTC",
			labelOf(t), alert.FormatUSD(snap.Current.Total), snap.Minute.Format("15:04"))
		if snap.Current.Quality != volume.QualityOK {
			fmt.Fprintf(&sb, "\n⚠ minute quality: %s", snap.Current.Quality)
		}
		fmt.Fprintf(&sb, "\nbuy %s · sell %s · %d trades",
			alert.FormatUSD(snap.Current.Buy), alert.FormatUSD(snap.Current.Sell), snap.Current.Trades)

		for _, w := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
			bl := snap.Baselines[w]
			if !bl.Usable {
				fmt.Fprintf(&sb, "\n%s median: warming up (%d samples)", alert.FormatWindow(w), bl.Samples)
				continue
			}
			pct, _ := volume.PercentChange(snap.Current.Total, bl.Median)
			// Showing the historical share makes it obvious when a baseline is
			// still the aggregator's view rather than this pipeline's.
			origin := ""
			if bl.Backfilled > 0 {
				origin = fmt.Sprintf(" · %d/%d from history", bl.Backfilled, bl.Samples)
			}
			fmt.Fprintf(&sb, "\n%s median %s · %s%s",
				alert.FormatWindow(w), alert.FormatUSD(bl.Median), alert.FormatPct(pct), origin)
		}
		return sb.String(), nil
	}
	return "not tracking " + args[0], nil
}

func labelOf(t domain.Token) string {
	if t.Symbol == "" {
		return t.Address[:min(10, len(t.Address))] + "… · " + string(t.Chain)
	}
	return t.Symbol + " · " + string(t.Chain)
}
