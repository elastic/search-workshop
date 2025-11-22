# Elasticsearch Flight Data Importer (Java)

Java implementation of the flight data importer using the official [Elasticsearch Java client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html).

## Requirements

- Java 11 or higher
- Maven 3.6 or higher
- Elasticsearch cluster (local or remote)

## Building

```bash
mvn clean package
```

This will create a JAR file in the `target/` directory with all dependencies included (via Maven Shade plugin).

## Usage

### Basic Import

Import all flight data files from the default data directory:

```bash
java -jar target/import-flights-1.0.0.jar --all
```

### Import Specific File

```bash
java -jar target/import-flights-1.0.0.jar --file data/flights-2024.csv.gz
```

### Import with Glob Pattern

```bash
java -jar target/import-flights-1.0.0.jar --glob "data/flights-202*.csv.gz"
```

### Custom Configuration

```bash
java -jar target/import-flights-1.0.0.jar \
  --config config/elasticsearch.yml \
  --mapping config/mappings-flights.json \
  --data-dir data \
  --index flights \
  --all
```

### Check Cluster Status

```bash
java -jar target/import-flights-1.0.0.jar --status
```

### Delete Indices

Delete all flights indices:

```bash
java -jar target/import-flights-1.0.0.jar --delete-all
```

Delete indices matching a pattern:

```bash
java -jar target/import-flights-1.0.0.jar --delete-index flights-2024
```

### Sample Document

Print the first document from a file (useful for debugging):

```bash
java -jar target/import-flights-1.0.0.jar --sample --file data/flights-2024.csv.gz
```

## Command Line Options

```
Usage: import_flights [options]

Options:
  -c, --config PATH       Path to Elasticsearch config YAML (default: config/elasticsearch.yml)
  -m, --mapping PATH      Path to mappings JSON (default: mappings-flights.json)
  -d, --data-dir PATH     Directory containing data files (default: data)
  -f, --file PATH         Only import the specified file
  -a, --all               Import all files found in the data directory
  -g, --glob PATTERN      Import files matching the glob pattern
  --index NAME            Override index name (default: flights)
  --batch-size N          Number of documents per bulk request (default: 500)
  --refresh               Request an index refresh after each bulk request
  --status                Test connection and print cluster health status
  --delete-index          Delete indices matching the index pattern and exit
  --delete-all            Delete all flights-* indices and exit
  --sample                Print the first document and exit
  -h, --help              Show this help message
```

## Configuration

The Elasticsearch configuration file (default: `config/elasticsearch.yml`) should contain:

```yaml
endpoint: http://localhost:9200
# Optional authentication
user: elastic
password: changeme
# Or use API key
# api_key: your_api_key_here
# Optional SSL configuration
ssl_verify: true
# ca_file: /path/to/ca.crt
# ca_path: /path/to/ca/directory
# Optional custom headers
# headers:
#   X-Custom-Header: value
```

## Features

- **Bulk Indexing**: Efficiently imports large CSV files using Elasticsearch bulk API
- **Compressed Files**: Supports `.csv`, `.csv.gz`, and `.zip` files
- **Airport Lookup**: Enriches flight data with airport coordinates from `airports.csv.gz`
- **Cancellation Lookup**: Adds cancellation reason descriptions from `cancellations.csv`
- **Index Management**: Automatically creates time-based indices (e.g., `flights-2024`, `flights-2024-07`)
- **Progress Tracking**: Shows real-time progress during import
- **Error Handling**: Comprehensive error handling with helpful error messages

## Index Naming

Indices are created based on the flight date:
- If filename contains year and month (e.g., `flights-2024-07.csv.gz`): `flights-2024-07`
- If filename contains only year (e.g., `flights-2024.csv.gz`): `flights-2024`
- Otherwise, extracted from `@timestamp` field: `flights-YYYY`

## Dependencies

- **Elasticsearch Java Client** (`co.elastic.clients:elasticsearch-java`): Official Elasticsearch Java client
- **Jackson**: JSON processing
- **SnakeYAML**: YAML configuration parsing
- **Apache Commons CSV**: CSV file parsing
- **SLF4J**: Logging

## Implementation Notes

This Java implementation leverages Java-specific features:

- Uses Maven for dependency management
- Leverages Java's built-in GZIP and ZIP support
- Uses Apache Commons CSV for robust CSV parsing
- Follows Java naming conventions (camelCase for methods, PascalCase for classes)

## Troubleshooting

### Connection Issues

If you encounter connection errors, verify:
1. Elasticsearch is running and accessible
2. The endpoint URL in `config/elasticsearch.yml` is correct
3. Network connectivity (firewall, VPN, etc.)
4. Authentication credentials are correct

### Memory Issues

For very large files, you may need to increase Java heap size:

```bash
java -Xmx4g -jar target/import-flights-1.0.0.jar --all
```

### SSL Certificate Issues

If using HTTPS with self-signed certificates, set `ssl_verify: false` in your config file (not recommended for production).

## License

Same license as the parent project.
