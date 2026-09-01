// Package alert decides when to notify and renders what the owner sees.
package alert

import (
	"fmt"
	"strings"
)

// FormatUSD renders a dollar amount the way the spec's examples do:
//
//	$950  ·  $12.5K  ·  $125.4K  ·  $1.24M  ·  $12.7M
//
// Precision drops as the magnitude grows, and a trailing ".0" is trimmed, so
// $150,000 reads as "$150K" rather than "$150.0K".
func FormatUSD(v float64) string {
	neg := v < 0
	if neg {
		v = -v
	}

	var s string
	switch {
	case v < 1_000:
		s = fmt.Sprintf("%.0f", v)
	case v < 1_000_000:
		s = trimZeros(fmt.Sprintf("%.1f", v/1_000)) + "K"
	case v < 1_000_000_000:
		s = trimZeros(scaled(v/1_000_000)) + "M"
	default:
		s = trimZeros(scaled(v/1_000_000_000)) + "B"
	}

	if neg {
		return "-$" + s
	}
	return "$" + s
}

// scaled keeps three significant digits below 10 and drops to one decimal
// above it, which is what makes $1.24M and $12.7M both look right.
func scaled(v float64) string {
	if v < 10 {
		return fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func trimZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// FormatPct renders a percentage change with an explicit sign: "+37%", "-12%".
func FormatPct(p float64) string {
	return fmt.Sprintf("%+.0f%%", p)
}

// FormatWindow turns a window size in minutes into the label used in messages.
func FormatWindow(minutes int) string {
	switch minutes {
	case 1440:
		return "24h"
	case 60:
		return "60m"
	default:
		return fmt.Sprintf("%dm", minutes)
	}
}
