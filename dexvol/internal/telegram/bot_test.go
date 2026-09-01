package telegram

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

type fakeCtrl struct {
	changed int
	snap    volume.Snapshot
	has     bool
}

func (f *fakeCtrl) Snapshot(domain.Token) (volume.Snapshot, bool) { return f.snap, f.has }
func (f *fakeCtrl) Stats() volume.Stats                           { return volume.Stats{Accepted: 7} }
func (f *fakeCtrl) TokensChanged()                                { f.changed++ }

func newTestBot(t *testing.T) (*Bot, *fakeCtrl, *config.Store) {
	b, ctrl, store, _ := newTestBotAt(t)
	return b, ctrl, store
}

func newTestBotAt(t *testing.T) (*Bot, *fakeCtrl, *config.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	store := config.NewStore(path)
	ctrl := &fakeCtrl{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewBot(nil, 42, 0, store, alert.NewManager(), ctrl, log), ctrl, store, path
}

func TestNonOwnerIsIgnoredEntirely(t *testing.T) {
	// The bot is the control panel, so a stranger must not even learn it is
	// alive: no reply, and no state change.
	b, ctrl, store := newTestBot(t)
	b.handle(t.Context(), Update{Message: &Message{
		From: &User{ID: 999, Username: "stranger"},
		Chat: Chat{ID: 999},
		Text: "/add base 0x1111111111111111111111111111111111111111 EVIL",
	}})

	if len(store.Get().Tokens) != 0 {
		t.Fatal("a non-owner must not be able to change the watch list")
	}
	if ctrl.changed != 0 {
		t.Fatal("a non-owner must not be able to poke the service")
	}
}

func TestAddValidatesChainAndAddress(t *testing.T) {
	b, ctrl, store := newTestBot(t)

	// Sui appears in the spec as a future network but has no adapter, so it
	// must be refused rather than accepted into a watch list that will never
	// produce data for it.
	if _, err := b.dispatch("/add", []string{"sui", "0x1111111111111111111111111111111111111111"}); err == nil {
		t.Fatal("an unsupported chain must be rejected")
	}
	if _, err := b.dispatch("/add", []string{"base", "0xdeadbeef"}); err == nil {
		t.Fatal("a malformed EVM address must be rejected")
	}
	if _, err := b.dispatch("/add", []string{"solana", "0x1111111111111111111111111111111111111111"}); err == nil {
		t.Fatal("an EVM address on Solana must be rejected")
	}

	if _, err := b.dispatch("/add", []string{"base", "0x1111111111111111111111111111111111111111", "abc"}); err != nil {
		t.Fatalf("valid add failed: %v", err)
	}
	tokens := store.Get().Tokens
	if len(tokens) != 1 || tokens[0].Symbol != "ABC" || tokens[0].Chain != domain.ChainBase {
		t.Fatalf("unexpected token stored: %+v", tokens)
	}
	if ctrl.changed != 1 {
		t.Fatalf("pool discovery should be nudged on add, changed = %d", ctrl.changed)
	}
}

func TestExpansionChainsCanBeAdded(t *testing.T) {
	b, _, store := newTestBot(t)
	for i, chain := range []string{"arbitrum", "avax", "polygon", "op"} {
		addr := "0x" + string(rune('1'+i)) + "111111111111111111111111111111111111111"
		if _, err := b.dispatch("/add", []string{chain, addr[:42], "T" + chain}); err != nil {
			t.Errorf("%s: %v", chain, err)
		}
	}
	if got := len(store.Get().Tokens); got != 4 {
		t.Fatalf("stored %d tokens, want 4", got)
	}
}

func TestChainsCommandListsRegistry(t *testing.T) {
	b, _, _ := newTestBot(t)
	out, err := b.dispatch("/chains", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range domain.Chains() {
		if !strings.Contains(out, string(info.Chain)) {
			t.Errorf("/chains output missing %q:\n%s", info.Chain, out)
		}
	}
	// The warning is what tells the owner, before adding a token, that a
	// network gets one discovery opinion and no history to start from. It has
	// to appear for exactly the networks GeckoTerminal does not cover — which
	// is none of them since Robinhood Chain's id was confirmed.
	want := 0
	for _, info := range domain.Chains() {
		if info.GeckoTerminalID == "" {
			want++
		}
	}
	if got := strings.Count(out, "no history backfill"); got != want {
		t.Errorf("/chains flagged %d networks, want %d:\n%s", got, want, out)
	}
}

func TestAddIsIdempotent(t *testing.T) {
	b, _, store := newTestBot(t)
	args := []string{"base", "0x1111111111111111111111111111111111111111", "ABC"}
	b.dispatch("/add", args)
	reply, err := b.dispatch("/add", args)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reply, "already tracked") {
		t.Fatalf("reply = %q", reply)
	}
	if len(store.Get().Tokens) != 1 {
		t.Fatal("the same token must not be stored twice")
	}
}

func TestRemoveBySymbolAndAddress(t *testing.T) {
	b, _, store := newTestBot(t)
	b.dispatch("/add", []string{"base", "0x1111111111111111111111111111111111111111", "ABC"})
	b.dispatch("/add", []string{"eth", "0x2222222222222222222222222222222222222222", "XYZ"})

	if _, err := b.dispatch("/remove", []string{"abc"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dispatch("/remove", []string{"0x2222222222222222222222222222222222222222"}); err != nil {
		t.Fatal(err)
	}
	if got := len(store.Get().Tokens); got != 0 {
		t.Fatalf("expected an empty list, got %d", got)
	}
}

func TestThresholdRoundTripsThroughDisk(t *testing.T) {
	// The spec requires changing the threshold without editing code or
	// restarting the service, so the change must also survive a restart.
	b, _, store, path := newTestBotAt(t)
	if _, err := b.dispatch("/threshold", []string{"30"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dispatch("/cooldown", []string{"9"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dispatch("/add", []string{"base", "0x1111111111111111111111111111111111111111", "ABC"}); err != nil {
		t.Fatal(err)
	}
	if store.Get().ThresholdPct != 30 {
		t.Fatalf("threshold = %v", store.Get().ThresholdPct)
	}

	reloaded := config.NewStore(path)
	if err := reloaded.Load(); err != nil {
		t.Fatal(err)
	}
	rt := reloaded.Get()
	if rt.ThresholdPct != 30 || rt.CooldownMinutes != 9 || len(rt.Tokens) != 1 {
		t.Fatalf("settings did not survive a restart: %+v", rt)
	}
}

func TestThresholdRejectsGarbage(t *testing.T) {
	b, _, _ := newTestBot(t)
	for _, bad := range []string{"abc", "-5", "0"} {
		if _, err := b.dispatch("/threshold", []string{bad}); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
	// A trailing percent sign is what a person naturally types.
	if _, err := b.dispatch("/threshold", []string{"50%"}); err != nil {
		t.Errorf("50%% should be accepted: %v", err)
	}
}

func TestLastWindowCannotBeDisabled(t *testing.T) {
	b, _, _ := newTestBot(t)
	for _, w := range []string{"10", "30", "60"} {
		if _, err := b.dispatch("/windows", []string{w, "off"}); err != nil {
			t.Fatalf("disabling %s failed: %v", w, err)
		}
	}
	if _, err := b.dispatch("/windows", []string{"24h", "off"}); err == nil {
		t.Fatal("disabling the last window would make alerting impossible and must be refused")
	}
}

func TestUnknownCommand(t *testing.T) {
	b, _, _ := newTestBot(t)
	if _, err := b.dispatch("/nope", nil); err == nil {
		t.Fatal("an unknown command should explain itself")
	}
}

func TestSplitCommandStripsBotSuffix(t *testing.T) {
	cmd, args := splitCommand("/threshold@MyVolBot 30")
	if cmd != "/threshold" || len(args) != 1 || args[0] != "30" {
		t.Fatalf("cmd = %q args = %v", cmd, args)
	}
}
