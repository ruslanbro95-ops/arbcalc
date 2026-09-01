package alert

import (
	"testing"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

func TestGMGNLinkPerChain(t *testing.T) {
	cases := map[domain.Chain]string{
		domain.ChainSolana:   "https://gmgn.ai/sol/token/ADDR",
		domain.ChainEthereum: "https://gmgn.ai/eth/token/ADDR",
		domain.ChainBNB:      "https://gmgn.ai/bsc/token/ADDR",
		domain.ChainBase:     "https://gmgn.ai/base/token/ADDR",
	}
	for chain, want := range cases {
		l, ok := GMGNLink(domain.Token{Chain: chain, Address: "ADDR"})
		if !ok || l.URL != want {
			t.Errorf("%s: got %q ok=%v, want %q", chain, l.URL, ok, want)
		}
	}
}

func TestGMGNButtonOmittedWhereUnsupported(t *testing.T) {
	if _, ok := GMGNLink(domain.Token{Chain: domain.ChainRobinhood, Address: "ADDR"}); ok {
		t.Fatal("GMGN does not list Robinhood Chain; the button must be omitted")
	}
}

func TestOKXTemplateOverride(t *testing.T) {
	t.Setenv("OKX_URL_TEMPLATE", "https://example.test/{chain}/{address}")
	l, ok := OKXLink(domain.Token{Chain: domain.ChainBase, Address: "0xAA"})
	if !ok || l.URL != "https://example.test/base/0xAA" {
		t.Fatalf("got %q ok=%v", l.URL, ok)
	}
}

func TestOKXCanBeDisabled(t *testing.T) {
	t.Setenv("OKX_URL_TEMPLATE", "")
	if _, ok := OKXLink(domain.Token{Chain: domain.ChainBase, Address: "0xAA"}); ok {
		t.Fatal("an empty template must disable the OKX button")
	}
}

func TestLinksOrderAndOmission(t *testing.T) {
	got := Links(domain.Token{Chain: domain.ChainSolana, Address: "A"})
	if len(got) != 2 || got[0].Text != "GMGN" || got[1].Text != "OKX" {
		t.Fatalf("unexpected buttons: %+v", got)
	}
	// Robinhood has neither provider, so no buttons at all.
	if got := Links(domain.Token{Chain: domain.ChainRobinhood, Address: "A"}); len(got) != 0 {
		t.Fatalf("expected no buttons, got %+v", got)
	}
}

func TestOKXArbitrumUsesItsOwnSlug(t *testing.T) {
	// OKX names this chain arbitrum-one, unlike every other one here where the
	// slug matches the common name. Preflight found it: "arbitrum" answered
	// 404 while the other seven chains answered 200.
	l, ok := OKXLink(domain.Token{Chain: domain.ChainArbitrum, Address: "0xAA"})
	if !ok {
		t.Fatal("expected a link")
	}
	if l.URL != "https://web3.okx.com/token/arbitrum-one/0xAA" {
		t.Fatalf("got %q", l.URL)
	}
}
