package alert

import (
	"os"
	"strings"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// gmgnSlug maps a chain to GMGN's path segment. GMGN covers Solana, Ethereum,
// BSC and Base; a chain absent from this map simply gets no GMGN button, which
// is what the spec asks for when a link does not exist.
var gmgnSlug = map[domain.Chain]string{
	domain.ChainSolana:   "sol",
	domain.ChainEthereum: "eth",
	domain.ChainBNB:      "bsc",
	domain.ChainBase:     "base",
	// Robinhood Chain is intentionally absent: GMGN does not list it.
}

// okxSlug maps a chain to the path segment used by the OKX web3 token page.
//
// Unlike the GMGN pattern, this one could not be confirmed against a live page
// while the project was built (see docs/RESEARCH.md §0 — egress was blocked),
// so it is overridable. Set OKX_URL_TEMPLATE to a string containing {chain} and
// {address} if OKX changes its layout, or set it to an empty value to turn the
// OKX button off entirely.
var okxSlug = map[domain.Chain]string{
	domain.ChainEthereum: "ethereum",
	domain.ChainBNB:      "bsc",
	domain.ChainBase:     "base",
	domain.ChainSolana:   "solana",
}

const defaultOKXTemplate = "https://web3.okx.com/token/{chain}/{address}"

// Link is one inline button.
type Link struct {
	Text string
	URL  string
}

// GMGNLink builds the GMGN token page URL. ok is false when GMGN has no page
// for that chain, and the caller must then omit the button rather than render
// a dead one.
func GMGNLink(t domain.Token) (Link, bool) {
	slug, ok := gmgnSlug[t.Chain]
	if !ok || t.Address == "" {
		return Link{}, false
	}
	return Link{Text: "GMGN", URL: "https://gmgn.ai/" + slug + "/token/" + t.Address}, true
}

// OKXLink builds the OKX token page URL, honouring the OKX_URL_TEMPLATE
// override.
func OKXLink(t domain.Token) (Link, bool) {
	tmpl, set := os.LookupEnv("OKX_URL_TEMPLATE")
	if set && strings.TrimSpace(tmpl) == "" {
		return Link{}, false // explicitly disabled
	}
	if !set {
		tmpl = defaultOKXTemplate
	}

	slug, ok := okxSlug[t.Chain]
	if !ok || t.Address == "" {
		return Link{}, false
	}
	url := strings.ReplaceAll(tmpl, "{chain}", slug)
	url = strings.ReplaceAll(url, "{address}", t.Address)
	return Link{Text: "OKX", URL: url}, true
}

// Links returns the buttons available for a token, in the order the spec shows
// them. An empty result means the message carries no buttons at all.
func Links(t domain.Token) []Link {
	var out []Link
	if l, ok := GMGNLink(t); ok {
		out = append(out, l)
	}
	if l, ok := OKXLink(t); ok {
		out = append(out, l)
	}
	return out
}
