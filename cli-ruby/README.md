# Ruby CLI Tools for Elasticsearch

This directory contains Ruby scripts for importing data into Elasticsearch.

## Prerequisites

- Ruby 2.7+ (or Ruby 3.x recommended)
- Bundler (optional, but recommended)

## Installation

Install the required gem using Bundler:

```bash
bundle install
```

Or install the gem directly:

```bash
gem install elasticsearch
```

## Usage

### Import Flights

```bash
ruby import_flights.rb --all
```

### Import Airlines

```bash
ruby import_airlines.rb
```

### Import Cancellations

```bash
ruby import_cancellations.rb
```

## Configuration

All scripts use the Elasticsearch configuration file at `config/elasticsearch.yml`. See the sample configuration file for details.

## Dependencies

- `elasticsearch` gem (~> 8.0) - Official Elasticsearch Ruby client
