#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
PYTHON_CLI_DIR="$PROJECT_ROOT/cli-python"
RUBY_CLI_DIR="$PROJECT_ROOT/cli-ruby"
GO_CLI_DIR="$PROJECT_ROOT/cli-go"
RUST_CLI_DIR="$PROJECT_ROOT/cli-rust"
JS_CLI_DIR="$PROJECT_ROOT/cli-js"
PHP_CLI_DIR="$PROJECT_ROOT/cli-php"
JAVA_CLI_DIR="$PROJECT_ROOT/cli-java"
DOTNET_CLI_DIR="$PROJECT_ROOT/cli-dotnet"
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
    ruby|python|go|rust|javascript|js|php|java|dotnet|csharp)
      # Normalize "js" to "javascript"
      if [ "$SELECTED_CLIENT" = "js" ]; then
        SELECTED_CLIENT="javascript"
      fi
      # Normalize "csharp" to "dotnet"
      if [ "$SELECTED_CLIENT" = "csharp" ]; then
        SELECTED_CLIENT="dotnet"
      fi
      ;;
    *)
      echo "Error: Invalid client '$SELECTED_CLIENT'. Must be one of: ruby, python, go, rust, javascript (or js), php, java, dotnet (or csharp)" >&2
      exit 1
      ;;
  esac
fi

# Select client (randomly if not specified)
if [ -z "$SELECTED_CLIENT" ]; then
  # Randomly select between Ruby, Python, Go, Rust, JavaScript, PHP, Java, and .NET clients
  RANDOM_CHOICE=$((RANDOM % 8))
  if [ $RANDOM_CHOICE -eq 0 ]; then
    SELECTED_CLIENT="ruby"
  elif [ $RANDOM_CHOICE -eq 1 ]; then
    SELECTED_CLIENT="python"
  elif [ $RANDOM_CHOICE -eq 2 ]; then
    SELECTED_CLIENT="go"
  elif [ $RANDOM_CHOICE -eq 3 ]; then
    SELECTED_CLIENT="rust"
  elif [ $RANDOM_CHOICE -eq 4 ]; then
    SELECTED_CLIENT="javascript"
  elif [ $RANDOM_CHOICE -eq 5 ]; then
    SELECTED_CLIENT="php"
  elif [ $RANDOM_CHOICE -eq 6 ]; then
    SELECTED_CLIENT="java"
  else
    SELECTED_CLIENT="dotnet"
  fi
  echo "Randomly selected client: $SELECTED_CLIENT"
else
  echo "Using specified client: $SELECTED_CLIENT"
fi

# Set CLIENT_SCRIPT based on selected client
case "$SELECTED_CLIENT" in
  ruby)
    CLIENT_SCRIPT="$RUBY_CLI_DIR/import_flights.rb"
    ;;
  python)
    CLIENT_SCRIPT="$PYTHON_CLI_DIR/import_flights.py"
    ;;
  go)
    CLIENT_SCRIPT="$GO_CLI_DIR/import_flights"
    ;;
  rust)
    CLIENT_SCRIPT="$RUST_CLI_DIR/target/release/import_flights"
    ;;
  javascript)
    CLIENT_SCRIPT="$JS_CLI_DIR/import_flights.js"
    ;;
  php)
    CLIENT_SCRIPT="$PHP_CLI_DIR/import_flights.php"
    ;;
  java)
    CLIENT_SCRIPT="$JAVA_CLI_DIR/target/import-flights-1.0.0.jar"
    ;;
  dotnet)
    CLIENT_SCRIPT="$DOTNET_CLI_DIR"
    ;;
esac

echo "$CLIENT_SCRIPT"

if [ "$SELECTED_CLIENT" = "ruby" ]; then
  cd "$RUBY_CLI_DIR"

  # Install Ruby dependencies if needed
  if ! bundle check >/dev/null 2>&1; then
    echo "Installing Ruby dependencies..."
    bundle install
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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
elif [ "$SELECTED_CLIENT" = "python" ]; then
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
elif [ "$SELECTED_CLIENT" = "go" ]; then
  cd "$GO_CLI_DIR"

  # Build Go executable if needed
  if [ ! -f "$CLIENT_SCRIPT" ]; then
    echo "Building Go executable..."
    go build -o import_flights
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  ./import_flights --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "rust" ]; then
  cd "$RUST_CLI_DIR"

  # Build Rust executable if needed
  if [ ! -f "$CLIENT_SCRIPT" ]; then
    echo "Building Rust executable..."
    cargo build --release
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  "$CLIENT_SCRIPT" --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "javascript" ]; then
  cd "$JS_CLI_DIR"

  # Install Node.js dependencies if needed
  if [ ! -d "node_modules" ]; then
    echo "Installing JavaScript dependencies..."
    npm install
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  node import_flights.js --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "php" ]; then
  cd "$PHP_CLI_DIR"

  # Install PHP dependencies if needed
  if [ ! -f "vendor/autoload.php" ]; then
    echo "Installing PHP dependencies..."
    composer install
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  php import_flights.php --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "java" ]; then
  cd "$JAVA_CLI_DIR"

  # Build Java JAR if needed
  if [ ! -f "$CLIENT_SCRIPT" ]; then
    echo "Building Java JAR..."
    mvn clean package
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  java -jar "$CLIENT_SCRIPT" --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
elif [ "$SELECTED_CLIENT" = "dotnet" ]; then
  cd "$DOTNET_CLI_DIR"

  # Restore .NET dependencies if needed
  if [ ! -d "bin" ] || [ ! -f "bin/Debug/net8.0/ImportFlights.dll" ]; then
    echo "Restoring .NET dependencies and building..."
    dotnet restore
    dotnet build
  fi

  DEFAULT_ARGS=(--file "$DATA_FILE")

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

  dotnet run -- --config "$CONFIG_FILE" --mapping "$MAPPING_FILE" --index flights-2025-07 "${ARGS[@]}"
fi
