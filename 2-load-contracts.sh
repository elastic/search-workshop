#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
PYTHON_CLI_DIR="$PROJECT_ROOT/cli-python"
RUBY_CLI_DIR="$PROJECT_ROOT/cli-ruby"
JS_CLI_DIR="$PROJECT_ROOT/cli-js"
DOTNET_CLI_DIR="$PROJECT_ROOT/cli-dotnet"
GO_CLI_DIR="$PROJECT_ROOT/cli-go"
JAVA_CLI_DIR="$PROJECT_ROOT/cli-java"
PHP_CLI_DIR="$PROJECT_ROOT/cli-php"
RUST_CLI_DIR="$PROJECT_ROOT/cli-rust"

PYTHON_VENV_DIR="$PYTHON_CLI_DIR/venv"
MAPPING_FILE="$PROJECT_ROOT/config/mappings-contracts.json"
CONFIG_FILE="$PROJECT_ROOT/config/elasticsearch.yml"
PDF_PATH="$PROJECT_ROOT/data"

if [ ! -f "$MAPPING_FILE" ]; then
  echo "Mapping file not found at $MAPPING_FILE" >&2
  exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Config file not found at $CONFIG_FILE" >&2
  exit 1
fi

if [ ! -d "$PDF_PATH" ]; then
  echo "PDF directory not found at $PDF_PATH" >&2
  exit 1
fi

# Parse command-line arguments for --client flag
SELECTED_CLIENT=""
PASSTHROUGH=()
CLIENT_SPECIFIED=false

for arg in "$@"; do
  case "$arg" in
    --client=*)
      SELECTED_CLIENT="${arg#*=}"
      CLIENT_SPECIFIED=true
      ;;
    --client)
      # Handle --client as next argument
      CLIENT_SPECIFIED=true
      ;;
    *)
      if [ "$CLIENT_SPECIFIED" = true ] && [ -z "$SELECTED_CLIENT" ]; then
        SELECTED_CLIENT="$arg"
        CLIENT_SPECIFIED=false
      else
        PASSTHROUGH+=("$arg")
      fi
      ;;
  esac
done

# Validate client if specified
if [ -n "$SELECTED_CLIENT" ]; then
  case "$SELECTED_CLIENT" in
    ruby|python|js|dotnet|go|java|php|rust)
      ;;
    *)
      echo "Error: Invalid client '$SELECTED_CLIENT'. Must be one of: ruby, python, js, dotnet, go, java, php, rust" >&2
      exit 1
      ;;
  esac
fi

# Select client (randomly if not specified)
if [ -z "$SELECTED_CLIENT" ]; then
  CLIENTS=(ruby python js dotnet go java php rust)
  RANDOM_INDEX=$((RANDOM % ${#CLIENTS[@]}))
  SELECTED_CLIENT="${CLIENTS[$RANDOM_INDEX]}"
  echo "Randomly selected client: $SELECTED_CLIENT"
else
  echo "Using specified client: $SELECTED_CLIENT"
fi

# helper to build args (keep pdf-path unless help/status requested)
build_args() {
  # emulate nameref for POSIX sh by using global temp
  local out_var="$1"
  local pdf="$2"
  local defaults=(--pdf-path "$pdf")
  ARGS_BUILT=()

  if [ ${#PASSTHROUGH[@]} -gt 0 ]; then
    for arg in "${PASSTHROUGH[@]}"; do
      case "$arg" in
      --status | --help | -h)
        defaults=()
        break
        ;;
      esac
    done
  fi

  [ ${#PASSTHROUGH[@]} -gt 0 ] && ARGS_BUILT+=("${PASSTHROUGH[@]}")
  [ ${#defaults[@]} -gt 0 ] && ARGS_BUILT+=("${defaults[@]}")

  # export to requested var name
  eval "$out_var=(\"\${ARGS_BUILT[@]}\")"
}

if [ "$SELECTED_CLIENT" = "ruby" ]; then
  cd "$RUBY_CLI_DIR"

  # Install Ruby dependencies if needed
  if ! bundle check >/dev/null 2>&1; then
    echo "Installing Ruby dependencies..."
    bundle install
  fi

  build_args ARGS "$PDF_PATH"
  bundle exec ruby import_contracts.rb --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "python" ]; then
  cd "$PYTHON_CLI_DIR"

  if [ ! -d "$PYTHON_VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$PYTHON_VENV_DIR"
  fi

  # shellcheck disable=SC1091
  source "$PYTHON_VENV_DIR/bin/activate"

  if ! python3 -c "import requests, yaml" >/dev/null 2>&1; then
    echo "Installing Python dependencies..."
    pip install --upgrade pip
    pip install requests pyyaml
  fi

  build_args ARGS "$PDF_PATH"
  python3 import_contracts.py --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "js" ]; then
  cd "$JS_CLI_DIR"
  build_args ARGS "$PDF_PATH"
  npm install >/dev/null 2>&1 || true
  node import_contracts.js --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "dotnet" ]; then
  cd "$DOTNET_CLI_DIR"
  build_args ARGS "$PDF_PATH"
  dotnet run --project ImportContracts.csproj -- --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "go" ]; then
  cd "$GO_CLI_DIR"

  # Build Go executable if needed
  if [ ! -f "import_contracts" ]; then
    echo "Building Go executable..."
    go build -tags contracts -o import_contracts
  fi

  build_args ARGS "$PDF_PATH"
  ./import_contracts --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "java" ]; then
  cd "$JAVA_CLI_DIR"
  build_args ARGS "$PDF_PATH"
  mvn -q -DskipTests package
  java -cp target/import-flights-1.0.0.jar com.elastic.ImportContracts --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "php" ]; then
  cd "$PHP_CLI_DIR"
  build_args ARGS "$PDF_PATH"
  composer install --no-interaction --quiet || true
  php import_contracts.php --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "rust" ]; then
  cd "$RUST_CLI_DIR"
  build_args ARGS "$PDF_PATH"
  cargo run --release --bin import_contracts -- --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" "${ARGS[@]}"
fi
