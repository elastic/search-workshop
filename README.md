# Search & Analytics Workshop

This repository contains the supporting material for an airline-focused Elasticsearch workshop. It bundles monthly BTS On-Time Performance extracts alongside lookup tables for airlines and airports, plus importer scripts (Ruby and Python) for loading documents into Elasticsearch.

## Requirements

- Access to an Elasticsearch cluster (local or hosted).
- `unzip` utility available on your PATH (used to stream CSVs directly from the monthly archives).
- Ruby 3.x for `bin/ruby/import_flights.rb` (uses only the standard library, so no Gemfile or Bundler setup is needed).
- Python 3.10+ for `bin/python/import_flights.py` plus its lone dependency:
  ```bash
  python3 -m pip install -r bin/python/requirements.txt
  ```

## Quick Start

1. Copy the provided config template and update it with your cluster details:
   ```bash
   cp config/elasticsearch.sample.yml config/elasticsearch.yml
   ```
   Set `endpoint`, authentication (user/password or API key), and optional headers or TLS options as required.
2. Validate connectivity:
   ```bash
   ruby bin/ruby/import_flights.rb --status
   ```
3. Choose an importer:
   - Ruby:
     ```bash
     ruby bin/ruby/import_flights.rb --all
     ```
   - Python:
     ```bash
     python3 bin/python/import_flights.py --all
     ```
   Each command streams every `.zip` and `.csv` within `./data` (override with `--data-dir PATH`). Use `--file` to target a single archive and `--batch-size` to tune bulk size.

## Data Layout

- `data/flights/*.zip` — monthly BTS On-Time Performance extracts from January 2019 onward. Each archive contains one CSV with the DOT schema. Keep them zipped; the importer reads the CSV via `unzip -p`.
- `data/airlines.csv.gz` — OpenFlights airline reference data. `\N` denotes missing values.
- `data/airports.csv.gz` — OpenFlights airport reference data with latitude/longitude coordinates.
- `sample-flight.csv` — small CSV slice for quick experimentation.
- `sample-flight.json` — same sample flights expanded to JSON for mapping or ingestion tests.

## Ruby Importer (`bin/ruby/import_flights.rb`)

- Creates the target index automatically if it does not exist, using the mapping from `mappings-flights.json`.
- Supports incremental loads with `--file PATH` and full directory loads with `--all`.
- Highlights:
  - `ruby bin/ruby/import_flights.rb --help` — view every available option.
  - `ruby bin/ruby/import_flights.rb --status` — report cluster health and exit.
  - `ruby bin/ruby/import_flights.rb --all --batch-size 500` — import all archives with smaller bulk batches.
  - `ruby bin/ruby/import_flights.rb --file sample-flight.csv --refresh` — load a single file and force refreshes between bulks.

## Python Importer (`bin/python/import_flights.py`)

- Mirrors the Ruby script’s behaviour (index creation, batch size handling, and `_bulk` API usage) while relying only on the standard library plus PyYAML for configuration parsing.
- Example commands:
  - `python3 bin/python/import_flights.py --help` — view the CLI usage summary.
  - `python3 bin/python/import_flights.py --status` — verify connectivity to the configured cluster.
  - `python3 bin/python/import_flights.py --all --batch-size 500` — import all archives with smaller bulk batches.
  - `python3 bin/python/import_flights.py --file sample-flight.csv --refresh` — load a single file and force refreshes between bulks.
- Accepts the same options as the Ruby version (`--config`, `--mapping`, `--index`, `--delete-index`, etc.), so switching between languages is as simple as changing the executable.

## Running the Importers

Execute all commands from the repository root so relative paths resolve correctly.

- Ruby workflow:
  1. Install Ruby 3.x (no gems required).
  2. Copy and edit `config/elasticsearch.yml` as described above.
  3. Run `ruby bin/ruby/import_flights.rb --status` to confirm connectivity.
  4. Load data with either `ruby bin/ruby/import_flights.rb --all` or `ruby bin/ruby/import_flights.rb --file data/flights/<file>.zip`.
- Python workflow:
  1. Install Python 3.10+ and dependencies: `python3 -m pip install -r bin/python/requirements.txt`.
  2. Confirm connectivity with `python3 bin/python/import_flights.py --status`.
  3. Ingest data with `python3 bin/python/import_flights.py --all` or `python3 bin/python/import_flights.py --file sample-flight.csv`.

## Enrichment Lookups

- Airlines: Join flights on `IATA_CODE_Reporting_Airline` (or `Reporting_Airline` when blank) to fetch names and callsigns from `data/airlines.csv.gz`.
- Airports: Use DOT `OriginAirportID` / `DestAirportID` or the IATA codes (`Origin`, `Dest`) to add airport names and geolocation fields from `data/airports.csv.gz`. Populate `OriginLocation` and `DestLocation` as `geo_point` values.

## Next Steps

- Update `mappings-flights.json` before loading if you need additional fields or custom analyzers.
- Build dashboards or notebooks on top of the `flights` index to explore delays, distance buckets, regional performance, and more.
- Extend the importer to enrich flights with airline and airport metadata as part of the document body.

Refer to `AGENTS.md` for more detailed notes about the datasets and lookup fields.
