# Elasticsearch Flight Data Importer (Go)

This Go program imports flight data from CSV files into Elasticsearch.

## Features

- Import flight data from CSV files (supports `.csv`, `.csv.gz`, and `.zip` formats)
- Automatic index creation based on date patterns
- Bulk indexing with configurable batch sizes
- Airport coordinate lookup from airports CSV
- Cancellation reason lookup from cancellations CSV
- Progress tracking and error handling
- Support for various Elasticsearch authentication methods

## Prerequisites

- Go 1.21 or later
- Elasticsearch cluster (local or remote)
- Elasticsearch configuration file (`config/elasticsearch.yml`)

## Installation

Build the flights import executable:
```bash
cd cli-go
go mod download
go build -o import_flights
```

Build the contracts import executable:
```bash
go build -tags contracts -o import_contracts
```

Both executables share common code from `main.go` but have separate entry points:
- `main_flights.go` - Entry point for `import_flights` (built by default)
- `main_contracts.go` - Entry point for `import_contracts` (built with `-tags contracts`)

## Usage

```bash
./import_flights [options]
```

### Options

- `-c, --config PATH`: Path to Elasticsearch config YAML (default: `config/elasticsearch.yml`)
- `-m, --mapping PATH`: Path to mappings JSON (default: `config/mappings-flights.json`)
- `-d, --data-dir PATH`: Directory containing data files (default: `data`)
- `-f, --file PATH`: Only import the specified file
- `-a, --all`: Import all files found in the data directory
- `-g, --glob PATTERN`: Import files matching the glob pattern
- `--index NAME`: Override index name (default: `flights`)
- `--batch-size N`: Number of documents per bulk request (default: 500)
- `--refresh`: Request an index refresh after each bulk request
- `--status`: Test connection and print cluster health status
- `--delete-index`: Delete indices matching the index pattern and exit
- `--delete-all`: Delete all flights-* indices and exit
- `--sample`: Print the first document and exit
- `--airports-file PATH`: Path to airports CSV file (default: `data/airports.csv.gz`)
- `--cancellations-file PATH`: Path to cancellations CSV file (default: `data/cancellations.csv`)

### Examples

```bash
# Import all flight files
./import_flights --all

# Import a specific file
./import_flights --file data/flights-2024.csv.gz

# Import files matching a pattern
./import_flights --glob "data/flights-2024*.csv.gz"

# Check Elasticsearch connection status
./import_flights --status

# Sample the first document from a file
./import_flights --sample --file data/flights-2024.csv.gz

# Delete all flight indices
./import_flights --delete-all
```

## Configuration

Create a `config/elasticsearch.yml` file with your Elasticsearch connection details:

```yaml
endpoint: "https://your-elasticsearch-cluster.com"
user: "elastic"
password: "your-password"
# OR
api_key: "your-api-key"

ssl_verify: true
headers: {}
```

## Implementation Notes

- Uses the official Elasticsearch Go client (`github.com/elastic/go-elasticsearch/v8`)
- Command-line flag parsing uses Go's `flag` package

## License

Same as the parent project.
