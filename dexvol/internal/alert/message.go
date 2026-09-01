package alert

import (
	"fmt"
	"strings"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
)

// Message is a rendered alert ready to hand to the Telegram client.
type Message struct {
	Text  string
	Links []Link
}

// Render builds the compact alert body.
//
// Messages are plain text with no parse_mode: a token symbol is arbitrary
// user-supplied text and can easily contain characters that would break
// MarkdownV2 or HTML parsing, and a malformed message is a message Telegram
// refuses to deliver.
//
// Example:
//
//	#ABC · base
//	$150K
//	Volume +50%
//	10m +50%
//	30m +30%
//	60m +25%
//	24h +15%
//	Median: $100K
//
// A message that arrives inside an active cooldown carries a "new:" line
// naming the baselines that crossed for the first time, so a repeat always
// says what changed rather than looking like the bot spamming.
func Render(res detect.Result, d Decision) Message {
	var b strings.Builder

	// The chain is part of the identity: the same symbol can be listed on
	// several networks, and the owner needs to know which one moved.
	fmt.Fprintf(&b, "#%s · %s\n", sanitizeSymbol(res.Token.Symbol), res.Token.Chain)
	fmt.Fprintf(&b, "%s\n", FormatUSD(res.Volume))
	fmt.Fprintf(&b, "Volume %s\n", FormatPct(res.Primary.Pct))

	for _, ch := range res.Changes {
		// A window without enough healthy history has no honest percentage to
		// show, so it is left out rather than printed as a misleading 0%.
		if !ch.Usable {
			continue
		}
		fmt.Fprintf(&b, "%s %s\n", FormatWindow(ch.Window), FormatPct(ch.Pct))
	}

	fmt.Fprintf(&b, "Median: %s", FormatUSD(res.Primary.Median))

	// A repeat inside an active cooldown has to justify itself, or it reads as
	// the bot spamming. Each of the two ways through the cooldown names what
	// let it through.
	switch {
	case d.Reason == ReasonNewTrigger && len(d.NewWindows) > 0:
		fmt.Fprintf(&b, "\nnew: %s", windowLabels(d.NewWindows))
	case d.Reason == ReasonEscalation && len(d.EscalatedWindows) > 0:
		fmt.Fprintf(&b, "\nescalated: %s", windowLabels(d.EscalatedWindows))
	}

	// Only mention data quality when it is imperfect — a clean feed should not
	// spend a line saying so.
	if res.Snap.TotalMinutes > 0 && res.Snap.HealthyMinutes < res.Snap.TotalMinutes {
		fmt.Fprintf(&b, "\n⚠ data %d/%dm", res.Snap.HealthyMinutes, res.Snap.TotalMinutes)
	}

	// A baseline drawn mostly from an aggregator's history is not measured by
	// the same ruler as the live minute above it. Early after a start, or right
	// after a token is added, that is most alerts — and it is exactly when a
	// reader should weigh the number a little more carefully.
	if res.Primary.Samples > 0 && res.Primary.Backfilled*2 > res.Primary.Samples {
		fmt.Fprintf(&b, "\n⚠ baseline mostly from history")
	}

	return Message{Text: b.String(), Links: Links(res.Token)}
}

func windowLabels(windows []int) string {
	labels := make([]string, 0, len(windows))
	for _, w := range windows {
		labels = append(labels, FormatWindow(w))
	}
	return strings.Join(labels, " ")
}

// sanitizeSymbol keeps a hashtag from being broken by whitespace or a '#' in a
// hostile token name.
func sanitizeSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "UNKNOWN"
	}
	repl := strings.NewReplacer(" ", "_", "#", "", "\n", "", "\t", "")
	return repl.Replace(s)
}
