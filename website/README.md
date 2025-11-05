# Flight Search Web App

A simple, Google-like web interface for searching flights using Elasticsearch with support for BM25, Semantic Search, and AI Agent Builder.

## Features

- **BM25 Search**: Traditional keyword-based search with fuzzy matching
- **Semantic Search**: Vector-based semantic search using ELSER model
- **AI Agent Search**: AI-powered search with tool calling capabilities

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Elasticsearch connection:**
   - Copy `config/elasticsearch.sample.yml` to `config/elasticsearch.yml`
   - Fill in your Elasticsearch endpoint, credentials, and configuration

3. **Run the application:**
   ```bash
   python app.py
   ```

4. **Open in browser:**
   Navigate to `http://localhost:5000`

## Requirements

- Python 3.7+
- Elasticsearch cluster with 'contracts' index
- The contracts index should be created with mappings from `mappings-contracts.json`

## Note

This app searches the 'contracts' index in Elasticsearch, despite being named "Flight Search". This naming matches the workshop context where flight data is being explored.

## Usage

1. Enter your search query in the search box
2. Select your preferred search mode (BM25, Semantic, or AI Agent)
3. Click the search button or press Enter
4. Results will display below with highlights and metadata

## Search Modes

- **BM25**: Best for exact keyword matches and traditional text search
- **Semantic**: Best for finding conceptually similar content, even without exact keyword matches
- **AI Agent**: Advanced search that combines semantic understanding with AI-powered analysis
