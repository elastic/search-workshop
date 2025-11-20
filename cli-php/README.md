# PHP CLI Tools for Elasticsearch

This directory contains PHP scripts for importing data into Elasticsearch.

## Prerequisites

- PHP 7.4+ (PHP 8.0+ recommended)
- Composer

## Installation

Install the required dependencies using Composer:

```bash
composer install
```

## Usage

### Import Flights

```bash
php import_flights.php --all
```

### Command Line Options

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
- `-h, --help` - Show help message

## Configuration

All scripts use the Elasticsearch configuration file at `config/elasticsearch.yml`. See the sample configuration file for details.

## Dependencies

- `elasticsearch/elasticsearch` (~8.0) - Official Elasticsearch PHP client
- `symfony/yaml` (~6.0) - YAML parser for configuration files

## Examples

Import all flight files:
```bash
php import_flights.php --all
```

Import a specific file:
```bash
php import_flights.php --file data/flights-2024.csv.gz
```

Import files matching a pattern:
```bash
php import_flights.php --glob "data/flights-2024*.csv.gz"
```

Check cluster status:
```bash
php import_flights.php --status
```

Sample a document:
```bash
php import_flights.php --sample --file data/flights-2024.csv.gz
```
