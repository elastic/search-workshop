# .NET CLI Tools for Elasticsearch

This directory contains .NET (C#) scripts for importing data into Elasticsearch.

## Prerequisites

- .NET 8.0 SDK or later
- Elasticsearch cluster (local or remote)

## Installation

Restore the NuGet packages:

```bash
dotnet restore
```

## Building

Build the project:

```bash
dotnet build
```

## Usage

### Import Flights

Import all flight data files:

```bash
dotnet run -- --all
```

Import a specific file:

```bash
dotnet run -- --file data/flights-2024.csv.gz
```

Import files matching a glob pattern:

```bash
dotnet run -- --glob "flights-2024*.csv.gz"
```

### Other Options

Test connection and print cluster health:

```bash
dotnet run -- --status
```

Delete all flights indices:

```bash
dotnet run -- --delete-all
```

Delete indices matching a pattern:

```bash
dotnet run -- --delete-index flights
```

Sample the first document from a file:

```bash
dotnet run -- --sample --file data/flights-2024.csv.gz
```

### Command-Line Options

- `-c, --config PATH` - Path to Elasticsearch config YAML (default: config/elasticsearch.yml)
- `-m, --mapping PATH` - Path to mappings JSON (default: config/mappings-flights.json)
- `-d, --data-dir PATH` - Directory containing data files (default: data)
- `-f, --file PATH` - Only import the specified file
- `-a, --all` - Import all files found in the data directory
- `-g, --glob PATTERN` - Import files matching the glob pattern
- `--index NAME` - Override index name (default: flights)
- `--batch-size N` - Number of documents per bulk request (default: 500)
- `--refresh` - Request an index refresh after each bulk request
- `--status` - Test connection and print cluster health status
- `--delete-index` - Delete indices matching the index pattern and exit
- `--delete-all` - Delete all flights-* indices and exit
- `--sample` - Print the first document and exit
- `--airports-file PATH` - Path to airports CSV file (default: data/airports.csv.gz)
- `--cancellations-file PATH` - Path to cancellations CSV file (default: data/cancellations.csv)

## Configuration

All scripts use the Elasticsearch configuration file at `config/elasticsearch.yml`. See the sample configuration file for details.

Example configuration:

```yaml
endpoint: http://localhost:9200
# Optional authentication:
# api_key: "your-api-key"
# Or:
# user: "elastic"
# password: "your-password"
# Optional SSL:
# ssl_verify: false
# ca_file: "/path/to/ca.crt"
```

## Dependencies

- `Elastic.Clients.Elasticsearch` (8.15.0) - Official Elasticsearch .NET client
- `YamlDotNet` (16.0.1) - YAML configuration parsing
- `CsvHelper` (33.0.1) - CSV file parsing
- `System.CommandLine` (2.0.0-beta4) - Command-line argument parsing

## Features

- Supports CSV files (plain, gzipped, and zipped)
- Automatic index creation based on date patterns (flights-YYYY or flights-YYYY-MM)
- Airport coordinate lookup from airports.csv.gz
- Cancellation reason lookup from cancellations.csv
- Progress reporting during import
- Batch processing for efficient bulk imports
- Error handling and validation

## Notes

- The script automatically creates indices based on the flight date (extracted from the `@timestamp` or `FlightDate` field)
- Indices are named using the pattern `flights-YYYY` or `flights-YYYY-MM` depending on filename patterns
- Existing indices are deleted before import (to ensure clean data)
- The script processes files in batches of 500 documents by default (configurable with `--batch-size`)
