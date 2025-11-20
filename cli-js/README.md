# Flight Data Import Script (JavaScript)

This is a JavaScript/Node.js version of the Ruby flight import script. It imports flight data from CSV files (including gzipped and zipped files) into Elasticsearch.

## Installation

Install dependencies:

```bash
npm install
```

## Usage

```bash
node import_flights.js [options]
```

### Options

- `-c, --config <path>` - Path to Elasticsearch config YAML (default: `config/elasticsearch.yml`)
- `-m, --mapping <path>` - Path to mappings JSON (default: `config/mappings-flights.json`)
- `-d, --data-dir <path>` - Directory containing data files (default: `data`)
- `-f, --file <path>` - Only import the specified file
- `-a, --all` - Import all files found in the data directory
- `-g, --glob <pattern>` - Import files matching the glob pattern
- `--index <name>` - Override index name (default: `flights`)
- `--batch-size <n>` - Number of documents per bulk request (default: `500`)
- `--refresh` - Request an index refresh after each bulk request
- `--status` - Test connection and print cluster health status
- `--delete-index` - Delete indices matching the index pattern and exit
- `--delete-all` - Delete all flights-* indices and exit
- `--sample` - Print the first document and exit
- `--airports-file <path>` - Path to airports CSV file (default: `data/airports.csv.gz`)
- `--cancellations-file <path>` - Path to cancellations CSV file (default: `data/cancellations.csv`)

### Examples

Import all flight files:
```bash
node import_flights.js --all
```

Import a specific file:
```bash
node import_flights.js --file data/flights-2024.csv.gz
```

Import files matching a pattern:
```bash
node import_flights.js --glob "data/flights-2024*.csv.gz"
```

Check Elasticsearch connection:
```bash
node import_flights.js --status
```

Sample a document:
```bash
node import_flights.js --sample --file data/flights-2024.csv.gz
```

## Requirements

- Node.js 18+ (ES modules support)
- Elasticsearch cluster accessible via the configured endpoint
- Required data files (CSV, CSV.gz, or ZIP format)

## Notes

- The script uses ES modules (`import`/`export` syntax)
- For complex glob patterns, the shell may expand them automatically
- Large files are processed in batches to manage memory usage
- Progress is displayed during import operations
