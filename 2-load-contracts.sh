set -a
source elastic-start-local/.env
source contracts/.env
set +a

source contracts/venv/bin/activate && python3 cli-python/import_contracts.py --pdf-path data/