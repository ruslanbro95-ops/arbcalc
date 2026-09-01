// Package telegram is both the notification channel and the control panel for
// this service, which is why every entry point is gated on a single owner id.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a dependency-free Telegram Bot API client.
type Client struct {
	token   string
	baseURL string
	http    *http.Client
}

// DefaultAPIBase is Telegram's API root.
const DefaultAPIBase = "https://api.telegram.org"

func NewClient(token string) *Client { return NewClientWithBase(token, DefaultAPIBase) }

// NewClientWithBase lets the tests point the client at a stub server.
func NewClientWithBase(token, base string) *Client {
	return &Client{
		token:   token,
		baseURL: strings.TrimRight(base, "/"),
		// The timeout must exceed the long-poll timeout, or every poll would
		// be cancelled client-side just as it was about to return.
		http: &http.Client{Timeout: 90 * time.Second},
	}
}

// User is the sender of an update. Only the id matters for authorization.
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
}

type Chat struct {
	ID int64 `json:"id"`
}

type Message struct {
	MessageID int    `json:"message_id"`
	From      *User  `json:"from"`
	Chat      Chat   `json:"chat"`
	Text      string `json:"text"`
}

// CallbackQuery is an inline-button press.
//
// It is a separate update type from a message, and that distinction carries
// the whole access model: commands arrive as messages and are owner-only,
// while the mute button arrives here and anyone in the chat may press it.
type CallbackQuery struct {
	ID      string   `json:"id"`
	From    *User    `json:"from"`
	Message *Message `json:"message"`
	Data    string   `json:"data"`
}

type Update struct {
	UpdateID      int            `json:"update_id"`
	Message       *Message       `json:"message"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}

// InlineButton is one inline keyboard button: either a link or a callback.
type InlineButton struct {
	Text string `json:"text"`
	URL  string `json:"url,omitempty"`
	// Data is what comes back in CallbackQuery.Data when the button is
	// pressed. Telegram caps it at 64 bytes.
	Data string `json:"callback_data,omitempty"`
}

// Command is one entry in the bot's command menu.
type Command struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}
type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
}

func (c *Client) call(ctx context.Context, method string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := c.baseURL + "/bot" + url.PathEscape(c.token) + "/" + method

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}

	var ar apiResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return fmt.Errorf("telegram %s: bad response (http %d): %w", method, resp.StatusCode, err)
	}
	if !ar.OK {
		// The token is never included in the error: it would end up in logs.
		return fmt.Errorf("telegram %s: %s (code %d)", method, ar.Description, ar.ErrorCode)
	}
	if out != nil {
		return json.Unmarshal(ar.Result, out)
	}
	return nil
}

// GetUpdates long-polls for new updates starting at offset.
func (c *Client) GetUpdates(ctx context.Context, offset, timeoutSec int) ([]Update, error) {
	payload := map[string]any{
		"offset":  offset,
		"timeout": timeoutSec,
		// Button presses arrive as callback_query, not as messages.
		"allowed_updates": []string{"message", "callback_query"},
	}
	var out []Update
	if err := c.call(ctx, "getUpdates", payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Send delivers a plain-text message with an optional single row of URL
// buttons. Plain text avoids the escaping hazards of MarkdownV2 for content
// that includes arbitrary token symbols.
func (c *Client) Send(ctx context.Context, chatID int64, text string, buttons []InlineButton) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"disable_web_page_preview": true,
	}
	if len(buttons) > 0 {
		payload["reply_markup"] = map[string]any{
			"inline_keyboard": [][]InlineButton{buttons},
		}
	}
	return c.call(ctx, "sendMessage", payload, nil)
}

// DeleteWebhook clears any webhook left over from an earlier deployment.
// Telegram refuses getUpdates while a webhook is registered, so a stale one
// would make the bot look silently dead.
func (c *Client) DeleteWebhook(ctx context.Context) error {
	return c.call(ctx, "deleteWebhook", map[string]any{"drop_pending_updates": false}, nil)
}

// GetMe returns the bot's own account, used at startup to log which bot the
// token belongs to.
func (c *Client) GetMe(ctx context.Context) (User, error) {
	var u User
	err := c.call(ctx, "getMe", map[string]any{}, &u)
	return u, err
}

// splitCommand separates "/cmd@BotName arg1 arg2" into the bare command and its
// arguments. The @BotName suffix appears when the bot is used inside a group.
func splitCommand(text string) (cmd string, args []string) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return "", nil
	}
	cmd = fields[0]
	if i := strings.IndexByte(cmd, '@'); i >= 0 {
		cmd = cmd[:i]
	}
	return strings.ToLower(cmd), fields[1:]
}

// SendWith delivers a message with a full inline keyboard (one slice per row).
//
// Send is the single-row shortcut; rows matter because an alert now carries
// links and a mute control that must not be crammed onto one line.
func (c *Client) SendWith(ctx context.Context, chatID int64, text string, rows [][]InlineButton) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"disable_web_page_preview": true,
	}
	if len(rows) > 0 {
		payload["reply_markup"] = map[string]any{"inline_keyboard": rows}
	}
	return c.call(ctx, "sendMessage", payload, nil)
}

// EditText replaces a message's text and keyboard in place.
//
// Screens that replace each other rather than pile up are the difference
// between a control panel and a chat log: pressing "settings" should change
// the panel you are looking at, not append a fourteenth copy of it.
func (c *Client) EditText(ctx context.Context, chatID int64, messageID int, text string, rows [][]InlineButton) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"message_id":               messageID,
		"text":                     text,
		"disable_web_page_preview": true,
	}
	if rows != nil {
		payload["reply_markup"] = map[string]any{"inline_keyboard": rows}
	}
	return c.call(ctx, "editMessageText", payload, nil)
}

// EditMarkup replaces only a message's keyboard, leaving its text alone. An
// alert's body is a record of what happened and must not be rewritten; its
// mute button still has to show that it was pressed.
func (c *Client) EditMarkup(ctx context.Context, chatID int64, messageID int, rows [][]InlineButton) error {
	payload := map[string]any{
		"chat_id":      chatID,
		"message_id":   messageID,
		"reply_markup": map[string]any{"inline_keyboard": rows},
	}
	return c.call(ctx, "editMessageReplyMarkup", payload, nil)
}

// AnswerCallback clears the button's loading spinner and optionally shows a
// toast. Telegram leaves the button spinning until this is called, which reads
// as a hung bot.
func (c *Client) AnswerCallback(ctx context.Context, id, text string) error {
	payload := map[string]any{"callback_query_id": id}
	if text != "" {
		payload["text"] = text
	}
	return c.call(ctx, "answerCallbackQuery", payload, nil)
}

// SetCommands publishes the command menu, so typing "/" lists the commands
// with descriptions instead of requiring them to be memorized.
func (c *Client) SetCommands(ctx context.Context, cmds []Command) error {
	return c.call(ctx, "setMyCommands", map[string]any{"commands": cmds}, nil)
}
