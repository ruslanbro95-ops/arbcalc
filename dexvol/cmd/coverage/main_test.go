package main

import (
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

func TestParseTokens(t *testing.T) {
	got, err := parseTokens("base:0xAA:abc, solana:So111, eth:0xBB")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d tokens", len(got))
	}
	if got[0].Chain != domain.ChainBase || got[0].Symbol != "ABC" {
		t.Fatalf("unexpected first token: %+v", got[0])
	}
	if got[1].Chain != domain.ChainSolana || got[1].Symbol != "" {
		t.Fatalf("unexpected second token: %+v", got[1])
	}
	if !got[2].Enabled {
		t.Fatal("tokens must be enabled for discovery to pick them up")
	}
}

func TestParseTokensRejectsGarbage(t *testing.T) {
	if _, err := parseTokens("base"); err == nil {
		t.Fatal("an entry without an address must be rejected")
	}
	if _, err := parseTokens("polygon:0xAA"); err == nil {
		t.Fatal("an unsupported chain must be rejected")
	}
}

func TestScaleReferenceToWindow(t *testing.T) {
	// A twenty-minute run must not be compared against a full hour of
	// reference volume, or coverage would read as a third of the truth.
	if got := scaled(600, 20*time.Minute); got != 200 {
		t.Fatalf("got %v, want 200", got)
	}
	if got := scaled(600, time.Hour); got != 600 {
		t.Fatalf("got %v, want 600", got)
	}
	if got := scaled(0, time.Hour); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

func TestRatioHandlesMissingReference(t *testing.T) {
	if got := ratio(100, 0); got != "—" {
		t.Fatalf("got %q, want a dash when there is no reference", got)
	}
	if got := ratio(95, 100); got != "95.0%" {
		t.Fatalf("got %q", got)
	}
	// Above 100% is a legitimate outcome, not an error to clamp away.
	if got := ratio(120, 100); got != "120.0%" {
		t.Fatalf("got %q", got)
	}
}
