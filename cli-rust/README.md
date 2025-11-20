# Flight Data Import Script (Rust)

This is a Rust version of the flight import script. It imports flight data from CSV files (including gzipped and zipped files) into Elasticsearch.

## Building

Build the project:

```bash
cargo build --release
```

The binary will be at `target/release/import_flights`.

## Usage

```bash
./target/release/import_flights [OPTIONS]
```

Or run directly with cargo:

```bash
cargo run -- [OPTIONS]
```

### Options

- `-c, --config <PATH>` - Path to Elasticsearch config YAML (default: `config/elasticsearch.yml`)
- `-m, --mapping <PATH>` - Path to mappings JSON (default: `config/mappings-flights.json`)
- `-d, --data-dir <PATH>` - Directory containing data files (default: `data`)
- `-f, --file <PATH>` - Only import the specified file
- `-a, --all` - Import all files found in the data directory
- `-g, --glob <PATTERN>` - Import files matching the glob pattern
- `--index <NAME>` - Override index name (default: `flights`)
- `--batch-size <N>` - Number of documents per bulk request (default: `1000`)
- `--refresh` - Request an index refresh after each bulk request
- `--status` - Test connection and print cluster health status
- `--delete-index` - Delete the target index and exit
- `--sample` - Print the first document and exit (not yet implemented)
- `--airports-file <PATH>` - Path to airports CSV file (default: `data/airports.csv.gz`)
- `--cancellations-file <PATH>` - Path to cancellations CSV file (default: `data/cancellations.csv`)

### Examples

Import all flight files:
```bash
cargo run -- --all
```

Import a specific file:
```bash
cargo run -- --file data/flights-2024.csv.gz
```

Import files matching a pattern:
```bash
cargo run -- --glob "data/flights-2024*.csv.gz"
```

Check Elasticsearch connection:
```bash
cargo run -- --status
```

## Requirements

- Rust 1.70+ (with async/await support)
- Elasticsearch cluster accessible via the configured endpoint
- Required data files (CSV, CSV.gz, or ZIP format)

## Notes

- The script automatically deletes existing indices before importing
- Large files are processed in batches to manage memory usage
- Progress is displayed during import operations
- Uses async/await for efficient I/O operations
