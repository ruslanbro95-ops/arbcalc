package alert

import "testing"

func TestFormatUSDMatchesSpecExamples(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{950, "$950"},
		{12_500, "$12.5K"},
		{125_430, "$125.4K"},
		{1_240_000, "$1.24M"},
		{12_700_000, "$12.7M"},
		// The section 29 example prints round numbers without a ".0" tail.
		{150_000, "$150K"},
		{100_000, "$100K"},
		{91_500, "$91.5K"},
		{0, "$0"},
		{-2_500, "-$2.5K"},
		{4_500_000_000, "$4.5B"},
	}
	for _, c := range cases {
		if got := FormatUSD(c.in); got != c.want {
			t.Errorf("FormatUSD(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatPct(t *testing.T) {
	cases := map[float64]string{37: "+37%", 30.4348: "+30%", 0: "+0%", -12.6: "-13%"}
	for in, want := range cases {
		if got := FormatPct(in); got != want {
			t.Errorf("FormatPct(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatWindow(t *testing.T) {
	cases := map[int]string{10: "10m", 30: "30m", 60: "60m", 1440: "24h"}
	for in, want := range cases {
		if got := FormatWindow(in); got != want {
			t.Errorf("FormatWindow(%d) = %q, want %q", in, got, want)
		}
	}
}
