package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeEnv(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dexvol.env")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadEnvFile(t *testing.T) {
	path := writeEnv(t, `
# the bot, from @BotFather
TELEGRAM_BOT_TOKEN=123:ABC

export TELEGRAM_OWNER_ID=42
POLL_INTERVAL = 15s
QUOTED="a value"
SINGLE='another'
not a pair
`)
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	os.Unsetenv("TELEGRAM_BOT_TOKEN")

	if err := LoadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"TELEGRAM_BOT_TOKEN": "123:ABC",
		"TELEGRAM_OWNER_ID":  "42",
		"POLL_INTERVAL":      "15s",
		"QUOTED":             "a value",
		"SINGLE":             "another",
	}
	for k, want := range cases {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
		os.Unsetenv(k)
	}
}

func TestRealEnvironmentWinsOverTheFile(t *testing.T) {
	// A one-off override on the command line has to beat the file, or nobody
	// could test a change without editing it.
	path := writeEnv(t, "TELEGRAM_OWNER_ID=1\n")
	t.Setenv("TELEGRAM_OWNER_ID", "999")

	if err := LoadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("TELEGRAM_OWNER_ID"); got != "999" {
		t.Fatalf("got %q, want the environment to win", got)
	}
}

func TestMissingFileIsNotAnError(t *testing.T) {
	// Setting the environment directly is a perfectly normal way to run.
	if err := LoadEnvFile(filepath.Join(t.TempDir(), "absent.env")); err != nil {
		t.Fatalf("a missing file must not stop startup: %v", err)
	}
}

func TestParseEnvLine(t *testing.T) {
	cases := []struct {
		in         string
		key, value string
		ok         bool
	}{
		{"A=1", "A", "1", true},
		{"  A = 1  ", "A", "1", true},
		{"export A=1", "A", "1", true},
		{`A="x y"`, "A", "x y", true},
		{"A=", "A", "", true},
		{"# comment", "", "", false},
		{"", "", "", false},
		{"nonsense", "", "", false},
	}
	for _, c := range cases {
		k, v, ok := parseEnvLine(c.in)
		if ok != c.ok || k != c.key || v != c.value {
			t.Errorf("parseEnvLine(%q) = %q,%q,%v; want %q,%q,%v", c.in, k, v, ok, c.key, c.value, c.ok)
		}
	}
}

func TestLoadFillsInMissingWindows(t *testing.T) {
	// The detector treats an absent window as enabled while the bot prints it
	// as off, so a partial map would make /settings report the opposite of
	// what the service does.
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"threshold_pct":30,"windows":{"10":false}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	s := NewStore(path)
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	rt := s.Get()

	if on, set := rt.Windows[10]; !set || on {
		t.Fatalf("the file's own value must survive: %v %v", on, set)
	}
	for _, w := range []int{30, 60, 1440} {
		if on, set := rt.Windows[w]; !set || !on {
			t.Errorf("window %d = %v (set %v), want it filled in as enabled", w, on, set)
		}
	}
}
