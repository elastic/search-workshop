#!/usr/bin/env bash
set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the project root (parent of util/)
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WEBSITE_DIR="$PROJECT_ROOT/website"

# Change to website directory
cd "$WEBSITE_DIR"

# Check if virtual environment exists, create if not
if [ -d "venv" ]; then
  echo "Activating virtual environment..."
  source venv/bin/activate
else
  echo "Virtual environment not found. Creating one..."
  python3 -m venv venv
  echo "Activating virtual environment..."
  source venv/bin/activate
fi

# Check if Flask is installed
if ! python3 -c "import flask" 2>/dev/null; then
  echo "Flask not found. Installing dependencies..."
  pip install -r requirements.txt
fi

# Start the Flask app
echo "Starting Flask app..."
echo "The app will be available at http://localhost:5000"
echo "Press Ctrl+C to stop the server"
echo ""

python3 app.py
