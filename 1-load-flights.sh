#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
PYTHON_CLI_DIR="$PROJECT_ROOT/cli-python"
RUBY_CLI_DIR="$PROJECT_ROOT/cli-ruby"
PYTHON_VENV_DIR="$PYTHON_CLI_DIR/venv"
MAPPING_FILE="$PROJECT_ROOT/config/mappings-flights.json"
CONFIG_FILE="$PROJECT_ROOT/config/elasticsearch.yml"
DATA_FILE="$PROJECT_ROOT/data/flights-2025-07.csv.gz"

if [ ! -f "$MAPPING_FILE" ]; then
  echo "Mapping file not found at $MAPPING_FILE" >&2
  exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Config file not found at $CONFIG_FILE" >&2
  exit 1
fi

if [ ! -f "$DATA_FILE" ]; then
  echo "Data file not found at $DATA_FILE" >&2
  exit 1
fi

# Randomly select between Ruby and Python clients
if [ $((RANDOM % 2)) -eq 0 ]; then
  SELECTED_CLIENT="ruby"
  CLIENT_SCRIPT="$RUBY_CLI_DIR/import_flights.rb"
else
  SELECTED_CLIENT="python"
  CLIENT_SCRIPT="$PYTHON_CLI_DIR/import_flights.py"
fi

echo "Randomly selected client: $SELECTED_CLIENT"
echo "$CLIENT_SCRIPT"

if [ "$SELECTED_CLIENT" = "ruby" ]; then
  cd "$RUBY_CLI_DIR"

  # Install Ruby dependencies if needed
  if ! bundle check >/dev/null 2>&1; then
    echo "Installing Ruby dependencies..."
    bundle install
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

  PASSTHROUGH=("$@")
  if [ ${#PASSTHROUGH[@]} -gt 0 ]; then
    for arg in "${PASSTHROUGH[@]}"; do
      case "$arg" in
      --status | --delete-index | --help | -h)
        DEFAULT_ARGS=()
        break
        ;;
      esac
    done
  fi

  # Use conditional expansion to avoid "unbound variable" error when arrays are empty
  ARGS=()
  [ ${#PASSTHROUGH[@]} -gt 0 ] && ARGS+=("${PASSTHROUGH[@]}")
  [ ${#DEFAULT_ARGS[@]} -gt 0 ] && ARGS+=("${DEFAULT_ARGS[@]}")

  bundle exec ruby import_flights.rb --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
else
  cd "$PYTHON_CLI_DIR"

  if [ ! -d "$PYTHON_VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$PYTHON_VENV_DIR"
  fi

  # shellcheck disable=SC1091
  source "$PYTHON_VENV_DIR/bin/activate"

  if ! python3 -c "import dotenv, requests, PyPDF2, yaml" >/dev/null 2>&1; then
    echo "Installing Python dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

  PASSTHROUGH=("$@")
  if [ ${#PASSTHROUGH[@]} -gt 0 ]; then
    for arg in "${PASSTHROUGH[@]}"; do
      case "$arg" in
      --status | --delete-index | --help | -h)
        DEFAULT_ARGS=()
        break
        ;;
      esac
    done
  fi

  # Use conditional expansion to avoid "unbound variable" error when arrays are empty
  ARGS=()
  [ ${#PASSTHROUGH[@]} -gt 0 ] && ARGS+=("${PASSTHROUGH[@]}")
  [ ${#DEFAULT_ARGS[@]} -gt 0 ] && ARGS+=("${DEFAULT_ARGS[@]}")

  python3 import_flights.py --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
fi
