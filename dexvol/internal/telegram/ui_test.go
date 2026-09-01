package telegram

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
)

// apiRecorder stands in for Telegram and remembers what the bot asked it to do.
type apiRecorder struct {
	mu    sync.Mutex
	calls []recorded
}

type recorded struct {
	method  string
	payload map[string]any
}

func (r *apiRecorder) server(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var payload map[string]any
		body, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(body, &payload)

		parts := strings.Split(req.URL.Path, "/")
		r.mu.Lock()
		r.calls = append(r.calls, recorded{method: parts[len(parts)-1], payload: payload})
		r.mu.Unlock()

		w.Write([]byte(`{"ok":true,"result":{}}`))
	}))
	t.Cleanup(srv.Close)
	return NewClientWithBase("token", srv.URL)
}

func (r *apiRecorder) called(method string) (recorded, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.calls {
		if c.method == method {
			return c, true
		}
	}
	return recorded{}, false
}

func uiBot(t *testing.T) (*Bot, *config.Store, *apiRecorder) {
	t.Helper()
	rec := &apiRecorder{}
	store := config.NewStore(filepath.Join(t.TempDir(), "state.json"))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := NewBot(rec.server(t), 42, 0, store, alert.NewManager(), &fakeCtrl{}, log)
	return b, store, rec
}

func TestAnyoneCanMuteFromTheAlertButton(t *testing.T) {
	// The alerts go to a chat that other people read, so silencing a pump
	// everyone has already seen must not require the owner to be at a
	// keyboard. This is the one control that is deliberately open.
	b, store, rec := uiBot(t)

	b.handleCallback(t.Context(), &CallbackQuery{
		ID:      "q1",
		From:    &User{ID: 999, Username: "someone-else"},
		Message: &Message{MessageID: 7, Chat: Chat{ID: -100}},
		Data:    cbMute + "base:0xabc",
	})

	until, ok := store.Get().Muted["base:0xabc"]
	if !ok {
		t.Fatal("a non-owner press must still mute the token")
	}
	if d := time.Until(until); d < 29*time.Minute || d > 31*time.Minute {
		t.Fatalf("muted for %s, want about 30 minutes", d)
	}
	if _, ok := rec.called("editMessageReplyMarkup"); !ok {
		t.Error("the button must report that it was pressed")
	}
}

func TestNonOwnerCannotTouchSettingsFromButtons(t *testing.T) {
	// Everything except the mute changes configuration, and in a group that
	// makes the difference between a shared feed and a shared control panel.
	b, store, rec := uiBot(t)
	before := store.Get().ThresholdPct

	for _, data := range []string{cbSet + "thr:+5", cbSet + "mon:off", cbNav + "settings"} {
		b.handleCallback(t.Context(), &CallbackQuery{
			ID:      "q",
			From:    &User{ID: 999},
			Message: &Message{MessageID: 1, Chat: Chat{ID: -100}},
			Data:    data,
		})
	}

	if got := store.Get().ThresholdPct; got != before {
		t.Fatalf("threshold moved to %v; a stranger must not change settings", got)
	}
	if !store.Get().Monitoring {
		t.Fatal("a stranger must not be able to pause alerting")
	}
	// Silence, not a refusal: an error message would confirm the bot is here.
	if _, ok := rec.called("answerCallbackQuery"); ok {
		t.Error("a rejected press must get no answer at all")
	}
}

func TestOwnerButtonsAdjustAndRedraw(t *testing.T) {
	b, store, rec := uiBot(t)
	before := store.Get().ThresholdPct

	b.handleCallback(t.Context(), &CallbackQuery{
		ID:      "q2",
		From:    &User{ID: 42},
		Message: &Message{MessageID: 3, Chat: Chat{ID: 42}},
		Data:    cbSet + "thr:+5",
	})

	if got := store.Get().ThresholdPct; got != before+5 {
		t.Fatalf("threshold = %v, want %v", got, before+5)
	}
	// The panel is edited in place; a new message per press would turn a
	// control panel into a chat log.
	if _, ok := rec.called("editMessageText"); !ok {
		t.Error("the settings panel must be redrawn in place")
	}
	if _, ok := rec.called("sendMessage"); ok {
		t.Error("a button press must not post a new message")
	}
}

func TestAlertCarriesLinksAndAMuteButton(t *testing.T) {
	b, _, rec := uiBot(t)

	err := b.Notify(t.Context(), alert.Message{
		Text:     "#ABC · base\n$150K",
		Links:    []alert.Link{{Text: "GMGN", URL: "https://example.test/g"}},
		TokenKey: "base:0xabc",
	})
	if err != nil {
		t.Fatal(err)
	}

	call, ok := rec.called("sendMessage")
	if !ok {
		t.Fatal("no message sent")
	}
	markup, _ := json.Marshal(call.payload["reply_markup"])
	for _, want := range []string{"GMGN", cbMute + "base:0xabc"} {
		if !strings.Contains(string(markup), want) {
			t.Errorf("keyboard %s is missing %q", markup, want)
		}
	}
}

func TestAlertsGoToTheAlertChatNotTheOwner(t *testing.T) {
	// The whole point of the group setup: alerts are shared, control is not.
	rec := &apiRecorder{}
	store := config.NewStore(filepath.Join(t.TempDir(), "state.json"))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := NewBot(rec.server(t), 42, -1001234567890, store, alert.NewManager(), &fakeCtrl{}, log)

	if err := b.Notify(t.Context(), alert.Message{Text: "x", TokenKey: "base:0xabc"}); err != nil {
		t.Fatal(err)
	}
	call, _ := rec.called("sendMessage")
	if got := call.payload["chat_id"]; got != float64(-1001234567890) {
		t.Fatalf("alert went to chat %v, want the configured group", got)
	}
}

func TestMuteCallbackDataFitsTelegramsLimit(t *testing.T) {
	// Telegram silently rejects a keyboard whose callback_data exceeds 64
	// bytes, and the failure mode is an alert that arrives with no buttons.
	longest := cbMute + "robinhood:0x" + strings.Repeat("f", 40)
	if len(longest) > 64 {
		t.Fatalf("callback data is %d bytes for %q, over Telegram's 64-byte limit",
			len(longest), longest)
	}
}
