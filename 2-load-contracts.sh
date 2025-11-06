#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
CLI_DIR="$PROJECT_ROOT/cli-python"
VENV_DIR="$CLI_DIR/venv"
MAPPING_FILE="$PROJECT_ROOT/config/mappings-contracts.json"

if [ ! -f "$MAPPING_FILE" ]; then
  echo "Mapping file not found at $MAPPING_FILE" >&2
  exit 1
fi

cd "$CLI_DIR"

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if ! python3 -c "import dotenv, requests, PyPDF2, yaml" >/dev/null 2>&1; then
  echo "Installing Python dependencies..."
  pip install --upgrade pip
  pip install -r requirements.txt
fi

python3 import_contracts.py --mapping "$MAPPING_FILE" "$@"
