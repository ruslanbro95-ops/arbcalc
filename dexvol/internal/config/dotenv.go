package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// DefaultEnvFile is looked for in the working directory when DOTENV_PATH is
// unset.
const DefaultEnvFile = "dexvol.env"

// LoadEnvFile reads KEY=VALUE lines into the process environment.
//
// It exists mostly for how this service gets started unattended. A systemd unit
// can carry EnvironmentFile, but Windows Task Scheduler has no equivalent, and
// without a file the only options are baking secrets into a wrapper script or
// setting machine-wide variables. A file next to the binary is simpler and
// keeps the bot token in one place with one set of permissions.
//
// A real environment variable always wins over the file, so a one-off override
// on the command line behaves the way anyone would expect.
func LoadEnvFile(path string) error {
	if path == "" {
		path = envStr("DOTENV_PATH", DefaultEnvFile)
	}

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// Not having one is normal: the environment may be set directly.
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		key, value, ok := parseEnvLine(sc.Text())
		if !ok {
			continue
		}
		if key == "" {
			return fmt.Errorf("%s:%d: name is empty", path, line)
		}
		if _, set := os.LookupEnv(key); set {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("%s:%d: %w", path, line, err)
		}
	}
	return sc.Err()
}

// parseEnvLine returns the key and value of one line, or ok=false for a blank
// line or a comment.
func parseEnvLine(raw string) (key, value string, ok bool) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	// "export FOO=bar" is what people paste out of a README.
	line = strings.TrimPrefix(line, "export ")

	name, val, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimSpace(name)
	value = strings.TrimSpace(val)

	// Strip one layer of matching quotes, which is how a value with spaces or
	// a trailing comment marker has to be written.
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == '\'' && value[len(value)-1] == '\'') {
			return key, value[1 : len(value)-1], true
		}
	}
	return key, value, true
}
