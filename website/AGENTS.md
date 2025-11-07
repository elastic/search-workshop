# Repository Guidelines

## Project Structure & Module Organization
The Flask backend that serves search requests resides in `website/app.py`; it exposes `/api/search` and delivers the static front end. Client assets live under `website/static/` with `index.html`, `app.js`, and `style.css`. Elasticsearch configuration is stored one directory up in `config/`, where `elasticsearch.yml` overrides the provided `elasticsearch.sample.yml`. Data-loading scripts and helper tooling remain at the repository root (`1-load-flights.sh`, `2-load-contracts.sh`, `elastic-start-local/`) and should be kept in sync with API changes in `app.py`.

## Build, Test, and Development Commands
- Install dependencies into a virtual environment:  
  ```bash
  python3 -m venv venv && source venv/bin/activate && pip install -r website/requirements.txt
  ```
- Run the Flask server locally from `website/`:  
  ```bash
  python app.py
  ```
- Populate indices before testing UI flows:  
  ```bash
  ./1-load-flights.sh && ./2-load-contracts.sh
  ```

## Coding Style & Naming Conventions
Follow PEP 8: four-space indentation, snake_case for Python functions, and descriptive logger names. JavaScript in `static/app.js` uses ES6 modules, `const`/`let`, and four-space indentation; favor camelCase for browser variables and functions (`performSearch`, `hideError`). Keep HTML IDs lowercase with hyphens (`loadingIndicator`). Run `pip install -r requirements.txt --upgrade` when updating dependencies and commit the refreshed `requirements.txt`.

## Testing Guidelines
Automated tests are not yet in place; perform manual smoke tests after each change. Start the server, open `http://localhost:5000`, verify all three search modes, and ensure filters update results. For API spot checks, POST to `/api/search` with `curl` to validate response structure:  
```bash
curl -X POST http://localhost:5000/api/search -H "Content-Type: application/json" -d '{"query":"london","type":"bm25"}'
```
Document any regressions or Elasticsearch errors in the PR description.

## Commit & Pull Request Guidelines
Use short, imperative commit subjects mirroring existing history (`Fix website script`, `Improve python loading of flights`). Each commit should be scoped to one logical change. Pull requests must summarize the change, reference relevant workshop issues or scripts, list manual test steps, and attach screenshots or JSON snippets when UI output changes. Request review before merging and confirm the data-loading scripts still succeed against your branch.

## Security & Configuration Tips
Never commit real API keys. Keep `config/elasticsearch.yml` out of version control updates; rely on the sample file for defaults. When sharing logs, redact base URLs and credentials. If SSL verification is disabled for local work, document the rationale in the PR and confirm the flag is restored before merging.
