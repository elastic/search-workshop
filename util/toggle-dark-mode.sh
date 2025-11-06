#!/usr/bin/env bash
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
VERSION="${KIBANA_VERSION:-9.2.0}"
AUTH="${KIBANA_AUTH:-elastic:elastic}"

# Read current value (default to "false" if missing/null)
CURRENT=$(
  curl -s -u "$AUTH" -H "kbn-xsrf: true" \
    "$KIBANA_URL/api/saved_objects/config/$VERSION" |
    jq -r '.attributes["theme:darkMode"] // "false"'
)

# Toggle
if [[ "$CURRENT" == "true" ]]; then NEW=false; else NEW=true; fi

# Write new value (no fragile quotes thanks to a here-doc)
curl -s -X PUT "$KIBANA_URL/api/saved_objects/config/$VERSION" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u "$AUTH" \
  --data-binary @- <<JSON
{"attributes":{"theme:darkMode":$NEW}}
JSON

echo
echo "Dark mode toggled to: $NEW"
