#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data"
DATA_URL="https://storage.googleapis.com/search-workshop/data.tar"
ARCHIVE_PATH="$DATA_DIR/data.tar"

if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is required to download data." >&2
  exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "Error: tar is required to extract data." >&2
  exit 1
fi

cleanup() {
  if [ -f "$ARCHIVE_PATH" ]; then
    rm -f "$ARCHIVE_PATH"
  fi
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo "Downloading data archive..."
curl -L "$DATA_URL" -o "$ARCHIVE_PATH"

echo "Extracting archive..."
tar -xvf "$ARCHIVE_PATH" -C "$DATA_DIR" --strip-components=1

echo "Cleaning up..."
cleanup

echo "Data download and extraction complete."
