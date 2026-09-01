// Package config separates the two kinds of settings this service has:
// static ones that come from the environment and only change on restart, and
// runtime ones the owner edits from Telegram while the service keeps running.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// Static is read once at boot. Secrets live here and never in the state file.
type Static struct {
	// TelegramToken is the bot token from @BotFather.
	TelegramToken string
	// OwnerID is the only Telegram user allowed to talk to the bot. Every
	// other chat is refused, because the bot is also the control panel.
	OwnerID int64
	// StatePath is where runtime settings and the token list are persisted.
	StatePath string
	// DBPath is the SQLite file holding minute volumes and raw trades.
	DBPath string
	// PollInterval is how often sources are asked for new trades.
	PollInterval time.Duration
	// SealDelay is how long after a minute ends before it is sealed. It buys
	// slow sources time to deliver late trades; too short and real trades get
	// dropped as TooLate, too long and alerts lag.
	SealDelay time.Duration
	// PoolRefresh is how often pool discovery re-runs, so a token appearing on
	// a new DEX is picked up instead of being missed forever.
	PoolRefresh time.Duration
	// RawTradeRetention bounds how long raw trades are kept for debugging.
	RawTradeRetention time.Duration
	// RPCEndpoints maps a chain to its JSON-RPC URL.
	RPCEndpoints map[domain.Chain]string
	// LogLevel is "debug", "info", "warn" or "error".
	LogLevel string
}

// Runtime is everything the owner can change from Telegram without a restart.
type Runtime struct {
	// ThresholdPct is the percentage a minute must exceed a baseline by.
	ThresholdPct float64 `json:"threshold_pct"`
	// CooldownMinutes suppresses repeat alerts for one token.
	CooldownMinutes int `json:"cooldown_minutes"`
	// EscalationFactor lets a materially stronger anomaly break the cooldown:
	// a new spike must be this many times the last alerted change to get
	// through. Without it a $10k anomaly would mask a $10M one for the whole
	// cooldown window.
	EscalationFactor float64 `json:"escalation_factor"`
	// Windows enables or disables each baseline.
	Windows map[int]bool `json:"windows"`
	// Monitoring is the global on/off switch.
	Monitoring bool `json:"monitoring"`
	// Tokens is the watch list.
	Tokens []domain.Token `json:"tokens"`
}

// DefaultRuntime matches the values the spec calls standard.
func DefaultRuntime() Runtime {
	return Runtime{
		ThresholdPct:     30,
		CooldownMinutes:  5,
		EscalationFactor: 1.5,
		Windows: map[int]bool{
			10:      true,
			30:      true,
			60:      true,
			24 * 60: true,
		},
		Monitoring: true,
		Tokens:     nil,
	}
}

// LoadStatic reads the environment. It fails fast on a missing bot token or
// owner id: without an owner the bot would accept commands from anyone, and
// this bot is the control panel for the whole service.
func LoadStatic() (Static, error) {
	// A file beside the binary is how an unattended start gets its settings:
	// systemd could use EnvironmentFile, but Windows Task Scheduler has no
	// equivalent. Real environment variables still win over it.
	if err := LoadEnvFile(""); err != nil {
		return Static{}, fmt.Errorf("read env file: %w", err)
	}

	s := Static{
		TelegramToken:     os.Getenv("TELEGRAM_BOT_TOKEN"),
		StatePath:         envStr("STATE_PATH", "state.json"),
		DBPath:            envStr("DB_PATH", "dexvol.db"),
		PollInterval:      envDur("POLL_INTERVAL", 12*time.Second),
		SealDelay:         envDur("SEAL_DELAY", 20*time.Second),
		PoolRefresh:       envDur("POOL_REFRESH", 5*time.Minute),
		RawTradeRetention: envDur("RAW_TRADE_RETENTION", 48*time.Hour),
		LogLevel:          envStr("LOG_LEVEL", "info"),
		RPCEndpoints:      map[domain.Chain]string{},
	}

	if s.TelegramToken == "" {
		return s, fmt.Errorf("TELEGRAM_BOT_TOKEN is not set")
	}
	raw := os.Getenv("TELEGRAM_OWNER_ID")
	if raw == "" {
		return s, fmt.Errorf("TELEGRAM_OWNER_ID is not set; the bot refuses to run without a single authorized owner")
	}
	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return s, fmt.Errorf("TELEGRAM_OWNER_ID %q is not a number: %w", raw, err)
	}
	s.OwnerID = id

	// Endpoints come from the chain registry, so adding a network there also
	// gives it an RPC_* override with a working public default. Public
	// endpoints are rate limited; point these at your own for headroom.
	for _, info := range domain.Chains() {
		s.RPCEndpoints[info.Chain] = envStr(info.RPCEnv, info.DefaultRPC)
	}
	return s, nil
}

// Store holds the runtime settings and persists every change, so a restart
// keeps the owner's threshold, cooldown and watch list.
//
// It is guarded because it is genuinely shared: the bot goroutine writes to it
// whenever the owner sends /threshold or /windows, while the seal, price,
// discovery and backfill loops all read it. Unsynchronised, that is not merely
// a stale read — Runtime carries a map, and Go aborts the whole process on a
// concurrent map read and write. A single /windows command at the wrong
// instant would take the monitor down.
type Store struct {
	path string
	mu   sync.RWMutex
	rt   Runtime
}

func NewStore(path string) *Store {
	return &Store{path: path, rt: DefaultRuntime()}
}

// Load reads the state file. A missing file is not an error — it just means a
// first run, and the defaults stand.
func (s *Store) Load() error {
	b, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	rt := DefaultRuntime()
	if err := json.Unmarshal(b, &rt); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}
	// Fill in any window the file does not mention. The detector treats an
	// absent entry as enabled while /windows and /settings print it as off, so
	// a partial map — a hand-edited file, or a state file written before a new
	// window existed — would make the bot report the opposite of what it does.
	if rt.Windows == nil {
		rt.Windows = map[int]bool{}
	}
	for w, on := range DefaultRuntime().Windows {
		if _, set := rt.Windows[w]; !set {
			rt.Windows[w] = on
		}
	}
	if rt.EscalationFactor <= 1 {
		rt.EscalationFactor = DefaultRuntime().EscalationFactor
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.rt = rt
	return nil
}

// Save writes the current settings to disk.
func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.saveLocked()
}

// saveLocked writes through a temporary file so a crash mid-write cannot leave
// the owner's settings truncated. The caller must hold the lock.
func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.rt, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Get returns a deep copy, so a caller holding the result can neither mutate
// the store nor observe it changing underneath them mid-decision.
func (s *Store) Get() Runtime {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp := s.rt
	cp.Tokens = append([]domain.Token(nil), s.rt.Tokens...)
	cp.Windows = make(map[int]bool, len(s.rt.Windows))
	for k, v := range s.rt.Windows {
		cp.Windows[k] = v
	}
	return cp
}

// Update applies fn to the settings and persists the result atomically: no
// reader can see the change half-applied, and two concurrent updates cannot
// interleave.
func (s *Store) Update(fn func(*Runtime)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fn(&s.rt)
	return s.saveLocked()
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
