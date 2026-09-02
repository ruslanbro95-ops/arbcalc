package telegram

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// Callback data prefixes. Telegram caps callback_data at 64 bytes, which a
// chain-qualified address fits inside with room to spare.
const (
	cbMute = "mute:" // mute:<token key> — anyone in the chat may press this
	cbNav  = "nav:"  // nav:<screen>     — owner only
	cbSet  = "set:"  // set:<field>:<±n> — owner only
	cbNoop = "noop:" // a control that only displays a value
)

// muteFor is how long the alert button silences a token.
//
// It is deliberately short and fixed. The button exists for one situation — a
// pump that is already understood and keeps firing — and a control that needs
// a submenu to answer "how long?" would not get pressed during one.
const muteFor = 30 * time.Minute

// commandMenu is what Telegram lists when someone types a slash.
//
// Every command here also works as plain text, but a bot whose commands have
// to be memorized from a help message is a bot whose commands go unused.
func commandMenu() []Command {
	return []Command{
		{Command: "menu", Description: "панель управления"},
		{Command: "list", Description: "отслеживаемые токены"},
		{Command: "add", Description: "добавить токен: /add base 0xADDR ABC"},
		{Command: "remove", Description: "убрать токен: /remove ABC"},
		{Command: "vol", Description: "объём прямо сейчас: /vol ABC"},
		{Command: "status", Description: "качество данных и ingestion"},
		{Command: "settings", Description: "текущая конфигурация"},
		{Command: "threshold", Description: "порог срабатывания в процентах"},
		{Command: "cooldown", Description: "анти-спам в минутах"},
		{Command: "minvolume", Description: "минимальный объём минуты в USD"},
		{Command: "chains", Description: "поддерживаемые сети"},
		{Command: "chatid", Description: "id текущего чата, для настройки группы"},
		{Command: "help", Description: "все команды"},
	}
}

// alertKeyboard is the control strip under an alert: links on one row, the
// mute on its own.
//
// The mute sits apart on purpose. The links are navigation and the mute
// changes what the bot does, and a control that changes behaviour should not
// be one thumb-width from one that opens a chart.
func alertKeyboard(links []InlineButton, tokenKey string) [][]InlineButton {
	var rows [][]InlineButton
	if len(links) > 0 {
		rows = append(rows, links)
	}
	rows = append(rows, []InlineButton{
		{Text: "🔇 Тихо 30 мин", Data: cbMute + tokenKey},
	})
	return rows
}

// mutedKeyboard replaces the mute control once it has been used, so the button
// reports the state it produced instead of looking untouched.
func mutedKeyboard(links []InlineButton, until time.Time) [][]InlineButton {
	var rows [][]InlineButton
	if len(links) > 0 {
		rows = append(rows, links)
	}
	rows = append(rows, []InlineButton{
		{Text: "🔇 тихо до " + until.Local().Format("15:04"), Data: cbNoop + "muted"},
	})
	return rows
}

// mainMenu is the panel the owner lands on.
func (b *Bot) mainMenu() (string, [][]InlineButton) {
	rt := b.store.Get()

	state := "🟢 алерты включены"
	toggle := InlineButton{Text: "⏸ Пауза", Data: cbSet + "mon:off"}
	if !rt.Monitoring {
		state = "🔴 алерты выключены (сбор данных идёт)"
		toggle = InlineButton{Text: "▶️ Включить", Data: cbSet + "mon:on"}
	}

	text := fmt.Sprintf("DEX Volume Monitor\n\n%s\nТокенов: %d · порог +%.0f%% · кулдаун %dм%s",
		state, len(rt.Tokens), rt.ThresholdPct, rt.CooldownMinutes, mutedSuffix(rt))

	return text, [][]InlineButton{
		{{Text: "📋 Токены", Data: cbNav + "list"}, {Text: "⚙️ Настройки", Data: cbNav + "settings"}},
		{{Text: "📊 Статус", Data: cbNav + "status"}, toggle},
	}
}

// mutedSuffix mentions active mutes on the main panel. A muted token that says
// nothing looks exactly like a quiet one, and that is the wrong thing for a
// control panel to leave ambiguous.
func mutedSuffix(rt config.Runtime) string {
	now := time.Now()
	n := 0
	for _, until := range rt.Muted {
		if now.Before(until) {
			n++
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("\n🔇 заглушено токенов: %d", n)
}

func backRow() []InlineButton {
	return []InlineButton{{Text: "◀️ Назад", Data: cbNav + "menu"}}
}

// settingsScreen renders the two numbers people actually change, with the
// steps built in. Typing the command works and always will; nobody wants to
// type it on a phone while something is pumping.
func (b *Bot) settingsScreen() (string, [][]InlineButton) {
	rt := b.store.Get()
	text, err := b.cmdSettings()
	if err != nil {
		text = "⚠ " + err.Error()
	}
	return text, [][]InlineButton{
		{
			{Text: "−5%", Data: cbSet + "thr:-5"},
			{Text: fmt.Sprintf("порог +%.0f%%", rt.ThresholdPct), Data: cbNoop + "thr"},
			{Text: "+5%", Data: cbSet + "thr:+5"},
		},
		{
			{Text: "−1м", Data: cbSet + "cd:-1"},
			{Text: fmt.Sprintf("кулдаун %dм", rt.CooldownMinutes), Data: cbNoop + "cd"},
			{Text: "+1м", Data: cbSet + "cd:+1"},
		},
		{
			{Text: "÷2", Data: cbSet + "minvol:half"},
			{Text: "мин. объём " + alert.FormatUSD(rt.MinVolumeUSD), Data: cbNoop + "minvol"},
			{Text: "×2", Data: cbSet + "minvol:double"},
		},
		backRow(),
	}
}

// screen resolves a navigation target to its text and keyboard.
func (b *Bot) screen(name string) (string, [][]InlineButton) {
	switch name {
	case "list":
		text, err := b.cmdList()
		if err != nil {
			text = "⚠ " + err.Error()
		}
		return text, [][]InlineButton{backRow()}
	case "settings":
		return b.settingsScreen()
	case "status":
		text, err := b.cmdStatus()
		if err != nil {
			text = "⚠ " + err.Error()
		}
		return text, [][]InlineButton{
			{{Text: "🔄 Обновить", Data: cbNav + "status"}},
			backRow(),
		}
	default:
		return b.mainMenu()
	}
}

// handleCallback routes a button press.
//
// The access rule is the point of this function. Mute is open to everyone in
// the chat, because the alerts are for everyone in the chat and silencing a
// pump you have already seen should not require the owner. Everything else
// edits configuration and stays owner-only; a stranger pressing it gets
// nothing back, the same silence a stranger's command gets, so the bot never
// confirms it is listening.
func (b *Bot) handleCallback(ctx context.Context, q *CallbackQuery) {
	if q.From == nil || q.Message == nil {
		return
	}
	data := q.Data

	switch {
	case strings.HasPrefix(data, cbMute):
		b.applyMute(ctx, q, strings.TrimPrefix(data, cbMute))
		return
	case strings.HasPrefix(data, cbNoop):
		b.answerNoop(ctx, q, strings.TrimPrefix(data, cbNoop))
		return
	}

	if q.From.ID != b.ownerID {
		b.log.Warn("rejected button press from a non-owner",
			"from_id", q.From.ID, "data", data)
		return
	}

	switch {
	case strings.HasPrefix(data, cbNav):
		text, rows := b.screen(strings.TrimPrefix(data, cbNav))
		_ = b.client.AnswerCallback(ctx, q.ID, "")
		b.render(ctx, q, text, rows)
	case strings.HasPrefix(data, cbSet):
		b.applySetting(ctx, q, strings.TrimPrefix(data, cbSet))
	default:
		_ = b.client.AnswerCallback(ctx, q.ID, "")
	}
}

// applyMute silences one token for everyone reading the chat.
func (b *Bot) applyMute(ctx context.Context, q *CallbackQuery, tokenKey string) {
	if tokenKey == "" {
		return
	}
	until := time.Now().Add(muteFor)
	err := b.store.Update(func(rt *config.Runtime) {
		if rt.Muted == nil {
			rt.Muted = map[string]time.Time{}
		}
		rt.Muted[tokenKey] = until
	})
	if err != nil {
		b.log.Error("could not store mute", "token", tokenKey, "err", err)
		_ = b.client.AnswerCallback(ctx, q.ID, "не удалось заглушить")
		return
	}
	b.log.Info("token muted from the alert button",
		"token", tokenKey, "by", q.From.ID, "until", until)

	_ = b.client.AnswerCallback(ctx, q.ID, "Тихо 30 минут")
	// The alert body is a record of what happened and is left alone; only its
	// control strip changes, and it keeps the links.
	links := linksFor(tokenKey)
	if err := b.client.EditMarkup(ctx, q.Message.Chat.ID, q.Message.MessageID,
		mutedKeyboard(links, until)); err != nil {
		b.log.Warn("could not update the mute button", "err", err)
	}
}

// answerNoop explains a control that only displays a value.
func (b *Bot) answerNoop(ctx context.Context, q *CallbackQuery, what string) {
	rt := b.store.Get()
	msg := ""
	switch what {
	case "thr":
		msg = fmt.Sprintf("Порог: +%.0f%%", rt.ThresholdPct)
	case "cd":
	case "minvol":
		msg = "Минимальный объём минуты: " + alert.FormatUSD(rt.MinVolumeUSD)
		msg = fmt.Sprintf("Кулдаун: %d мин", rt.CooldownMinutes)
	case "muted":
		msg = "Уже заглушено"
	}
	_ = b.client.AnswerCallback(ctx, q.ID, msg)
}

// applySetting nudges one number and redraws the panel in place.
func (b *Bot) applySetting(ctx context.Context, q *CallbackQuery, spec string) {
	field, delta, ok := strings.Cut(spec, ":")
	if !ok {
		return
	}

	var note string
	switch field {
	case "mon":
		on := delta == "on"
		if err := b.store.Update(func(rt *config.Runtime) { rt.Monitoring = on }); err != nil {
			note = "не сохранилось"
		} else if on {
			note = "алерты включены"
		} else {
			note = "алерты выключены"
		}
	case "thr":
		n, err := strconv.Atoi(strings.TrimPrefix(delta, "+"))
		if err != nil {
			return
		}
		if err := b.store.Update(func(rt *config.Runtime) {
			rt.ThresholdPct = clampFloat(rt.ThresholdPct+float64(n), 1, 100000)
		}); err != nil {
			note = "не сохранилось"
		}
	case "minvol":
		if err := b.store.Update(func(rt *config.Runtime) {
			switch delta {
			case "double":
				if rt.MinVolumeUSD <= 0 {
					rt.MinVolumeUSD = 100
				} else {
					rt.MinVolumeUSD *= 2
				}
			case "half":
				rt.MinVolumeUSD = clampFloat(rt.MinVolumeUSD/2, 0, 1e12)
				if rt.MinVolumeUSD < 50 {
					// Below this the floor stops filtering anything, so the
					// step lands on "off" rather than on a number that only
					// looks like a setting.
					rt.MinVolumeUSD = 0
				}
			}
		}); err != nil {
			note = "не сохранилось"
		}
	case "cd":
		n, err := strconv.Atoi(strings.TrimPrefix(delta, "+"))
		if err != nil {
			return
		}
		if err := b.store.Update(func(rt *config.Runtime) {
			rt.CooldownMinutes = clampInt(rt.CooldownMinutes+n, 1, 1440)
		}); err != nil {
			note = "не сохранилось"
		}
	default:
		return
	}

	_ = b.client.AnswerCallback(ctx, q.ID, note)

	// Monitoring lives on the main panel, the two numbers on the settings one.
	target := "settings"
	if field == "mon" {
		target = "menu"
	}
	text, rows := b.screen(target)
	b.render(ctx, q, text, rows)
}

// render edits the message the button belongs to, so screens replace each
// other instead of stacking up.
func (b *Bot) render(ctx context.Context, q *CallbackQuery, text string, rows [][]InlineButton) {
	if err := b.client.EditText(ctx, q.Message.Chat.ID, q.Message.MessageID, text, rows); err != nil {
		// Telegram rejects an edit that changes nothing, which is the
		// "refresh showed the same numbers" case. It is not a fault and it
		// certainly should not become a second message.
		if strings.Contains(err.Error(), "message is not modified") {
			return
		}
		b.log.Warn("could not redraw the panel", "err", err)
	}
}

func clampFloat(v, lo, hi float64) float64 { return max(lo, min(hi, v)) }

func clampInt(v, lo, hi int) int { return max(lo, min(hi, v)) }

// linksFor rebuilds an alert's link buttons from the token key.
//
// Telegram does not hand a callback back the keyboard it came from, and the
// mute edit has to resend the whole strip or the links vanish. Rebuilding from
// the key is better than parsing them out of the message anyway: the key is
// the identity, and the link templates may have been reconfigured since the
// alert was sent.
func linksFor(tokenKey string) []InlineButton {
	chain, addr, ok := strings.Cut(tokenKey, ":")
	if !ok {
		return nil
	}
	tok := domain.Token{Chain: domain.Chain(chain), Address: addr}
	var out []InlineButton
	for _, l := range alert.Links(tok) {
		out = append(out, InlineButton{Text: l.Text, URL: l.URL})
	}
	return out
}
