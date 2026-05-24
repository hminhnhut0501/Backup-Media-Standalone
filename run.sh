#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$ROOT_DIR"

if [ ! -d "$VENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

python -m pip install -U pip >/dev/null
python -m pip install -r requirements.txt >/dev/null

mkdir -p sessions downloads/backup_tmp runtime

echo "Starting Backup Media Standalone at http://localhost:${PORT:-8010}"
exec python app.py
