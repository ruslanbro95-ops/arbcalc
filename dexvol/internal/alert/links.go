package alert

import (
	"os"
	"strings"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// Button targets come from the shared chain registry, so a network added there
// gets its links automatically and one with no listing gets no button — which
// is what the spec asks for when a link does not exist.

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
	info, ok := domain.Info(t.Chain)
	if !ok || info.GMGNSlug == "" || t.Address == "" {
		return Link{}, false
	}
	return Link{Text: "GMGN", URL: "https://gmgn.ai/" + info.GMGNSlug + "/token/" + t.Address}, true
}

// OKXLink builds the OKX token page URL, honouring the OKX_URL_TEMPLATE
// override.
//
// Unlike the GMGN pattern, this one could not be confirmed against a live page
// while the project was built (see docs/RESEARCH.md §0 — egress was blocked),
// so it is overridable. Set OKX_URL_TEMPLATE to a string containing {chain} and
// {address} if OKX changes its layout, or set it to an empty value to turn the
// OKX button off entirely.
func OKXLink(t domain.Token) (Link, bool) {
	tmpl, set := os.LookupEnv("OKX_URL_TEMPLATE")
	if set && strings.TrimSpace(tmpl) == "" {
		return Link{}, false // explicitly disabled
	}
	if !set {
		tmpl = defaultOKXTemplate
	}

	info, ok := domain.Info(t.Chain)
	if !ok || info.OKXSlug == "" || t.Address == "" {
		return Link{}, false
	}
	url := strings.ReplaceAll(tmpl, "{chain}", info.OKXSlug)
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
