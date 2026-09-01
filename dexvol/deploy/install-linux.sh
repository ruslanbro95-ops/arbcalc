#!/usr/bin/env bash
# Install the monitor as a systemd service that survives crashes and reboots.
#
# Usage:  sudo ./deploy/install-linux.sh [install-dir]
set -euo pipefail

DIR="${1:-/opt/dexvol}"
SERVICE_USER="${SUDO_USER:-$USER}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "Run with sudo: sudo $0 ${1:-}" >&2
  exit 1
fi

command -v go >/dev/null || { echo "Go is not installed." >&2; exit 1; }

echo "Building..."
( cd "$SRC" && go build -o "$SRC/monitor" ./cmd/monitor && go build -o "$SRC/doctor" ./cmd/doctor && go build -o "$SRC/coverage" ./cmd/coverage )

install -d -o "$SERVICE_USER" -m 755 "$DIR"
install -o "$SERVICE_USER" -m 755 "$SRC/monitor" "$SRC/doctor" "$SRC/coverage" "$DIR/"

if [[ ! -f "$DIR/dexvol.env" ]]; then
  install -o "$SERVICE_USER" -m 600 "$SRC/deploy/dexvol.env.example" "$DIR/dexvol.env"
  echo
  echo "Created $DIR/dexvol.env — fill in TELEGRAM_BOT_TOKEN and TELEGRAM_OWNER_ID, then rerun."
  exit 0
fi

# Refuse to install something that cannot work. Finding out from a preflight
# table beats finding out from a service that flaps in the background.
echo
echo "Running preflight..."
if ! ( cd "$DIR" && sudo -u "$SERVICE_USER" ./doctor ); then
  echo
  echo "Preflight failed — not installing. Fix the failures above and rerun." >&2
  exit 1
fi

sed -e "s|__DIR__|$DIR|g" -e "s|__USER__|$SERVICE_USER|g" \
  "$SRC/deploy/dexvol.service" > /etc/systemd/system/dexvol.service

systemctl daemon-reload
systemctl enable --now dexvol.service

echo
echo "Installed and started."
echo "  status:  systemctl status dexvol"
echo "  logs:    journalctl -u dexvol -f"
echo "  stop:    sudo systemctl stop dexvol"
