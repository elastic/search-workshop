#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""AIR Search - Flask web app for searching flights, airlines, and contracts indices with Keyword, Semantic, and AI Agent search."""

import json
import logging
import os
import ssl
from pathlib import Path
from typing import Dict, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import requests
import yaml
from flask import Flask, Response, jsonify, request, stream_with_context
from flask_cors import CORS

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

LOGGER = logging.getLogger(__name__)

# Project root directory (parent of website/)
PROJECT_ROOT = Path(__file__).parent.parent


def load_config(config_path: Optional[Path] = None) -> Dict[str, object]:
    """Load Elasticsearch configuration from YAML file."""
    if config_path is None:
        config_path = PROJECT_ROOT / 'config' / 'elasticsearch.yml'
    
    # Fallback to sample config if main config doesn't exist
    if not config_path.exists():
        config_path = PROJECT_ROOT / 'config' / 'elasticsearch.sample.yml'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with config_path.open('r', encoding='utf-8') as handle:
        data = yaml.safe_load(handle) or {}
    
    if not isinstance(data, dict):
        raise ValueError(f"Configuration must be a mapping (found {type(data).__name__})")
    return data


def build_auth_header(config: Dict[str, object]) -> str:
    """Build Authorization header from config."""
    api_key = config.get('api_key')
    if api_key:
        return f'ApiKey {api_key}'
    
    user = config.get('user')
    password = config.get('password')
    if user and password:
        import base64
        credentials = base64.b64encode(f'{user}:{password}'.encode()).decode()
        return f'Basic {credentials}'
    
    return ''


def make_es_request(method: str, path: str, body: Optional[Dict] = None) -> Dict:
    """Make a request to Elasticsearch."""
    config = load_config()
    endpoint = str(config.get('endpoint', '')).strip()
    if not endpoint:
        raise ValueError("Elasticsearch endpoint not configured")
    
    # Parse endpoint
    parsed = urllib_parse.urlparse(endpoint)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip('/') if parsed.path else ''
    full_path = f"{base_path}/{path.lstrip('/')}"
    url = f"{base_url}{full_path}"
    
    # Build headers
    headers = {
        'Content-Type': 'application/json',
    }
    
    # Add custom headers from config
    custom_headers = config.get('headers', {})
    if isinstance(custom_headers, dict):
        headers.update({str(k): str(v) for k, v in custom_headers.items()})
    
    # Add auth header
    auth_header = build_auth_header(config)
    if auth_header:
        headers['Authorization'] = auth_header
    
    # Log the request in Kibana Dev Tools format (only for _search and _query endpoints)
    # Extract just the path and index from the full URL
    request_path = full_path
    if body and ('/_search' in request_path or '/_query' in request_path):
        LOGGER.info(f"\n{'='*80}\nKibana Dev Tools Format:\n{'='*80}\n{method} {request_path}\n{json.dumps(body, indent=2)}\n{'='*80}")

    # Build request
    if body:
        data = json.dumps(body).encode('utf-8')
        req = urllib_request.Request(url, data=data, headers=headers, method=method)
    else:
        req = urllib_request.Request(url, headers=headers, method=method)

    # SSL context
    ssl_verify = config.get('ssl_verify', True)
    if isinstance(ssl_verify, str):
        ssl_verify = ssl_verify.lower() not in ('false', '0', 'no', 'n')

    context = None
    if parsed.scheme == 'https' and not ssl_verify:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    try:
        with urllib_request.urlopen(req, context=context) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib_error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else 'No error details'
        LOGGER.error(f"Elasticsearch error: {e.code} {e.reason} - {error_body}")
        raise
    except Exception as e:
        LOGGER.error(f"Request error: {e}")
        raise


def make_kibana_request(method: str, path: str, body: Optional[Dict] = None, stream: bool = False):
    """Make a request to Kibana."""
    config = load_config()

    raw_endpoint = str(config.get('kibana_endpoint', '')).strip()
    kibana_config = config.get('kibana')
    if not raw_endpoint and isinstance(kibana_config, dict):
        raw_endpoint = str(kibana_config.get('endpoint', '')).strip()

    if not raw_endpoint:
        es_endpoint = str(config.get('endpoint', '')).strip()
        if es_endpoint:
            if '://' not in es_endpoint:
                es_endpoint = f"http://{es_endpoint}"
            parsed_es = urllib_parse.urlparse(es_endpoint)
            hostname = parsed_es.hostname or 'localhost'
            scheme = parsed_es.scheme or 'http'
            if hostname in ('localhost', '127.0.0.1'):
                raw_endpoint = f"{scheme}://{hostname}:5601"
            else:
                raise ValueError(
                    "Kibana endpoint not configured. Set 'kibana_endpoint' under config/elasticsearch.yml "
                    "or config/elasticsearch.sample.yml."
                )
        else:
            raw_endpoint = 'http://localhost:5601'

    if '://' not in raw_endpoint:
        raw_endpoint = f"http://{raw_endpoint}"

    parsed = urllib_parse.urlparse(raw_endpoint)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid Kibana endpoint: {raw_endpoint}")

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip('/') if parsed.path else ''
    full_path = f"{base_path}/{path.lstrip('/')}"
    url = f"{base_url}{full_path}"
    
    # Build headers
    headers = {
        'Content-Type': 'application/json',
        'kbn-xsrf': 'true',
    }

    if isinstance(kibana_config, dict) and isinstance(kibana_config.get('headers'), dict):
        headers.update({str(k): str(v) for k, v in kibana_config['headers'].items()})
    
    # Add auth header
    auth_header = build_auth_header(config)
    if auth_header:
        headers['Authorization'] = auth_header

    # Build request
    data = json.dumps(body).encode('utf-8') if body else None
    
    LOGGER.info(f"Kibana request: {method} {url}")
    LOGGER.info(f"Kibana request headers: {headers}")
    LOGGER.info(f"Kibana request body: {data}")

    verify_setting = config.get('kibana_ssl_verify', config.get('ssl_verify', True))
    if isinstance(verify_setting, str):
        lowered = verify_setting.lower()
        if lowered in ('false', '0', 'no', 'n'):
            verify_setting = False
        elif lowered in ('true', '1', 'yes', 'y'):
            verify_setting = True

    try:
        response = requests.request(
            method,
            url,
            data=data,
            headers=headers,
            stream=stream,
            verify=verify_setting
        )
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        LOGGER.error(f"Kibana request error: {e}")
        if e.response is not None:
            LOGGER.error(f"Kibana response: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Kibana request error: {e}")
        raise


@app.route('/')
def index():
    """Serve the main page."""
    return app.send_static_file('index.html')


@app.route('/api/search', methods=['POST'])
def search():
    """Search endpoint that routes to appropriate search type."""
    data = request.get_json() or {}
    search_type = data.get('type', 'keyword')
    
    if search_type == 'ai':
        # AI search is now streaming
        return jsonify({'error': "AI search has moved to a streaming endpoint. Please use /api/search/stream."}), 400

    query = data.get('query', '').strip()
    index_name = data.get('index', 'all')  # 'all', 'flights', 'airlines', or 'contracts'
    filters = data.get('filters', {})  # Extract filters from request
    
    try:
        # When no query, load 10 documents
        size = 10 if not query else 20
        
        if index_name == 'all':
            # Search all indices and combine results
            results = search_all_indices(query, search_type, size, filters)
        elif index_name == 'flights':
            results = search_flights(query, search_type, size, filters)
        elif index_name == 'airlines':
            results = search_airlines(query, search_type, size, filters)
        elif index_name == 'contracts':
            results = search_contracts(query, search_type, size, filters)
        else:
            return jsonify({'error': f'Unknown index: {index_name}'}), 400
        
        return jsonify(results)
    except urllib_error.HTTPError as e:
        error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
        LOGGER.error(f"Elasticsearch HTTP error: {e.code} - {error_body}")
        try:
            error_json = json.loads(error_body) if error_body else {}
            error_type = error_json.get('error', {}).get('type', '')
            error_reason = error_json.get('error', {}).get('root_cause', [{}])[0].get('reason', error_body)
            
            # Handle index_not_found_exception specifically
            if error_type == 'index_not_found_exception' or 'no such index' in error_reason.lower():
                missing_index = error_json.get('error', {}).get('index', index_name)
                error_msg = f"Index '{missing_index}' not found. Please ensure the index exists in Elasticsearch."
            else:
                error_msg = error_reason
        except:
            error_msg = f"Elasticsearch error: {e.code} {e.reason}"
        return jsonify({'error': error_msg}), 500
    except Exception as e:
        LOGGER.error(f"Search error: {e}", exc_info=True)
        return jsonify({'error': f'Search failed: {str(e)}'}), 500


@app.route('/api/search/stream', methods=['POST'])
def search_stream():
    """Streaming search endpoint for AI agent."""
    data = request.get_json() or {}
    query = data.get('query', '').strip()
    filters = data.get('filters', {})

    def generate():
        try:
            # Use a generator to stream the response from the AI agent
            for chunk in ai_agent_search(query, filters=filters, stream=True):
                yield chunk
        except Exception as e:
            LOGGER.error(f"Streaming search error: {e}", exc_info=True)
            # Yield a JSON error message
            error_message = json.dumps({'error': f'Streaming search failed: {str(e)}'})
            yield error_message

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

def search_all_indices(query: str, search_type: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Search across all indices (flights, airlines, contracts) and combine results."""
    indices = ['flights', 'airlines', 'contracts']
    all_hits = []
    total_hits = 0
    successful_indices = []
    failed_indices = []
    index_counts = {'flights': 0, 'airlines': 0, 'contracts': 0}

    # Calculate size per index (ensure at least 1)
    size_per_index = max(1, size // len(indices))
    
    for index in indices:
        try:
            if index == 'flights':
                result = search_flights(query, search_type, size_per_index, filters)
            elif index == 'airlines':
                result = search_airlines(query, search_type, size_per_index, filters)
            else:  # contracts
                result = search_contracts(query, search_type, size_per_index, filters)
            
            if result.get('hits', {}).get('hits'):
                for hit in result['hits']['hits']:
                    # Preserve actual index name from Elasticsearch (e.g., flights-2019)
                    # Only set if not already present
                    if '_index' not in hit or not hit['_index']:
                        hit['_index'] = index
                    all_hits.append(hit)

                # Extract total count
                result_total = result.get('hits', {}).get('total', 0)
                if isinstance(result_total, dict):
                    count = result_total.get('value', 0)
                    total_hits += count
                    index_counts[index] = count
                else:
                    total_hits += result_total
                    index_counts[index] = result_total

            successful_indices.append(index)
        except urllib_error.HTTPError as e:
            # Check if it's an index_not_found_exception
            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            try:
                error_json = json.loads(error_body) if error_body else {}
                error_type = error_json.get('error', {}).get('type', '')
                if error_type == 'index_not_found_exception':
                    LOGGER.warning(f"Index '{index}' not found, skipping")
                    failed_indices.append(index)
                    continue
            except:
                pass
            # Re-raise if it's not an index_not_found_exception
            raise
        except Exception as e:
            LOGGER.warning(f"Error searching {index} index: {e}")
            failed_indices.append(index)
            continue
    
    # Sort by score (if available) and limit to size
    all_hits.sort(key=lambda x: x.get('_score', 0), reverse=True)
    all_hits = all_hits[:size]

    result = {
        'hits': {
            'total': {'value': total_hits, 'relation': 'eq'},
            'hits': all_hits
        },
        'search_type': search_type,
        'searched_indices': successful_indices,
        'aggregations': {
            'record_types': {
                'buckets': [
                    {'key': 'flights', 'doc_count': index_counts['flights']},
                    {'key': 'airlines', 'doc_count': index_counts['airlines']},
                    {'key': 'contracts', 'doc_count': index_counts['contracts']}
                ]
            }
        }
    }

    # Add warning if some indices failed
    if failed_indices:
        result['warnings'] = [f"Index '{idx}' not found" for idx in failed_indices]

    return result


def search_flights(query: str, search_type: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Search flights indices using ES|QL with LOOKUP JOIN to enrich with airline names."""

    # Build the WHERE clause for filters
    where_clauses = []
    if query:
        # Add query conditions for different fields
        query_escaped = query.replace('"', '\\"')  # Escape double quotes for ES|QL
        where_clauses.append(f'(Flight_Number LIKE "*{query_escaped}*" OR Reporting_Airline LIKE "*{query_escaped}*" OR Origin LIKE "*{query_escaped}*" OR Dest LIKE "*{query_escaped}*")')

    if filters:
        if filters.get('cancelled') is not None:
            where_clauses.append(f"Cancelled == {str(filters['cancelled']).lower()}")
        if filters.get('diverted') is not None:
            where_clauses.append(f"Diverted == {str(filters['diverted']).lower()}")
        if filters.get('airline'):
            airline_escaped = filters['airline'].replace('"', '\\"')
            where_clauses.append(f'Reporting_Airline == "{airline_escaped}"')
        if filters.get('origin'):
            origin_escaped = filters['origin'].replace('"', '\\"')
            where_clauses.append(f'Origin == "{origin_escaped}"')
        if filters.get('dest'):
            dest_escaped = filters['dest'].replace('"', '\\"')
            where_clauses.append(f'Dest == "{dest_escaped}"')
        if filters.get('flight_date'):
            # Use date range for filtering - ES|QL datetime literal format with double quotes
            flight_date = filters['flight_date']
            # Format: YYYY-MM-DDTHH:MM:SS (no timezone, will match local time in index)
            where_clauses.append(f'(@timestamp >= "{flight_date}T00:00:00" AND @timestamp < "{flight_date}T23:59:59")')

    # Always filter out flights with null Flight_Number
    where_clauses.append("Flight_Number IS NOT NULL")

    where_clause = " AND ".join(where_clauses) if where_clauses else ""

    # Build ES|QL query with LOOKUP JOIN
    esql_query = f"""
        FROM flights-*
        | LOOKUP JOIN airlines ON Reporting_Airline
        {f'| WHERE {where_clause}' if where_clause else ''}
        | KEEP FlightID, Reporting_Airline, Airline_Name, Flight_Number, Origin, Dest,
               CRSDepTimeLocal, CRSArrTimeLocal, DepDelayMin, ArrDelayMin,
               Cancelled, Diverted, DistanceMiles, @timestamp
        | LIMIT {size}
    """

    body = {
        "query": esql_query.strip()
    }

    # Log the query for debugging
    LOGGER.info(f"ES|QL Query: {esql_query.strip()}")
    LOGGER.info(f"Filters: {filters}")

    try:
        # Execute ES|QL query
        esql_result = make_es_request('POST', '/_query', body)

        # Build a count query that matches the same WHERE clause
        count_body = {
            "query": {"match_all": {}}
        }

        # Apply the same filters to the count query
        filter_clauses = []

        # Always filter out null Flight_Number
        filter_clauses.append({"exists": {"field": "Flight_Number"}})

        if query:
            filter_clauses.append({
                "multi_match": {
                    "query": query,
                    "fields": ["Flight_Number", "Reporting_Airline", "Origin", "Dest"]
                }
            })

        if filters:
            if filters.get('cancelled') is not None:
                filter_clauses.append({"term": {"Cancelled": filters['cancelled']}})
            if filters.get('diverted') is not None:
                filter_clauses.append({"term": {"Diverted": filters['diverted']}})
            if filters.get('airline'):
                filter_clauses.append({"term": {"Reporting_Airline": filters['airline']}})
            if filters.get('origin'):
                filter_clauses.append({"term": {"Origin": filters['origin']}})
            if filters.get('dest'):
                filter_clauses.append({"term": {"Dest": filters['dest']}})
            if filters.get('flight_date'):
                filter_clauses.append({
                    "range": {
                        "@timestamp": {
                            "gte": f"{filters['flight_date']}T00:00:00",
                            "lt": f"{filters['flight_date']}T23:59:59"
                        }
                    }
                })

        if filter_clauses:
            count_body["query"] = {
                "bool": {
                    "filter": filter_clauses
                }
            }

        # Get total count
        count_result = make_es_request('POST', '/flights-*/_count', count_body)
        total_count = count_result.get('count', 0)

        # Also get aggregations using standard search API
        aggs_body = {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "cancelled": {
                    "terms": {"field": "Cancelled", "size": 2}
                },
                "diverted": {
                    "terms": {"field": "Diverted", "size": 2}
                },
                "airlines": {
                    "terms": {"field": "Reporting_Airline", "size": 20, "order": {"_count": "desc"}}
                },
                "origins": {
                    "terms": {"field": "Origin", "size": 20, "order": {"_count": "desc"}}
                },
                "destinations": {
                    "terms": {"field": "Dest", "size": 20, "order": {"_count": "desc"}}
                },
                "flight_dates": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "day",
                        "format": "yyyy-MM-dd",
                        "order": {"_key": "desc"}
                    }
                }
            }
        }

        # Apply filters to aggregation query
        if filters:
            filter_clauses = []
            if filters.get('cancelled') is not None:
                filter_clauses.append({"term": {"Cancelled": filters['cancelled']}})
            if filters.get('diverted') is not None:
                filter_clauses.append({"term": {"Diverted": filters['diverted']}})
            if filters.get('airline'):
                filter_clauses.append({"term": {"Reporting_Airline": filters['airline']}})
            if filters.get('origin'):
                filter_clauses.append({"term": {"Origin": filters['origin']}})
            if filters.get('dest'):
                filter_clauses.append({"term": {"Dest": filters['dest']}})
            if filters.get('flight_date'):
                filter_clauses.append({
                    "range": {
                        "@timestamp": {
                            "gte": f"{filters['flight_date']}T00:00:00",
                            "lt": f"{filters['flight_date']}T23:59:59"
                        }
                    }
                })

            if filter_clauses:
                aggs_body["query"] = {
                    "bool": {
                        "filter": filter_clauses
                    }
                }

        aggs_result = make_es_request('POST', '/flights-*/_search', aggs_body)

        # Convert ES|QL result to standard search response format
        hits = []
        columns = esql_result.get('columns', [])
        column_names = [col['name'] for col in columns]

        for row in esql_result.get('values', []):
            source = {}
            for i, value in enumerate(row):
                if i < len(column_names):
                    source[column_names[i]] = value

            hit = {
                '_index': 'flights',
                '_source': source,
                '_score': 1.0,
                'highlight': {}
            }
            hits.append(hit)

        return {
            'hits': {
                'total': {'value': total_count, 'relation': 'eq'},
                'hits': hits
            },
            'aggregations': aggs_result.get('aggregations', {}),
            'search_type': search_type
        }

    except Exception as e:
        LOGGER.warning(f"ES|QL query failed, falling back to standard search: {e}")
        # Fallback to original implementation
        return search_flights_fallback(query, search_type, size, filters)


def search_flights_fallback(query: str, search_type: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Fallback search for flights using standard Query DSL."""
    if query:
        query_clause = {
            "multi_match": {
                "query": query,
                "fields": [
                    "Flight_Number^2",
                    "Reporting_Airline^1.5",
                    "Origin^1.5",
                    "Dest^1.5",
                    "Tail_Number"
                ],
                "type": "best_fields"
            }
        }
    else:
        query_clause = {"match_all": {}}

    # Add filters if provided
    if filters:
        filter_clauses = []
        if filters.get('cancelled') is not None:
            filter_clauses.append({"term": {"Cancelled": filters['cancelled']}})
        if filters.get('diverted') is not None:
            filter_clauses.append({"term": {"Diverted": filters['diverted']}})
        if filters.get('airline'):
            filter_clauses.append({"term": {"Reporting_Airline": filters['airline']}})
        if filters.get('origin'):
            filter_clauses.append({"term": {"Origin": filters['origin']}})
        if filters.get('dest'):
            filter_clauses.append({"term": {"Dest": filters['dest']}})
        if filters.get('flight_date'):
            filter_clauses.append({
                "range": {
                    "@timestamp": {
                        "gte": f"{filters['flight_date']}T00:00:00",
                        "lt": f"{filters['flight_date']}T23:59:59"
                    }
                }
            })

        if filter_clauses:
            query_clause = {
                "bool": {
                    "must": [query_clause],
                    "filter": filter_clauses
                }
            }

    body = {
        "query": query_clause,
        "size": size,
        "_source": {
            "includes": [
                "FlightID", "Reporting_Airline", "Flight_Number", "Origin", "Dest",
                "CRSDepTimeLocal", "CRSArrTimeLocal", "DepDelayMin", "ArrDelayMin",
                "Cancelled", "Diverted", "DistanceMiles", "@timestamp"
            ]
        },
        "highlight": {
            "fields": {
                "Flight_Number": {},
                "Reporting_Airline": {},
                "Origin": {},
                "Dest": {}
            }
        },
        "aggs": {
            "cancelled": {
                "terms": {"field": "Cancelled", "size": 2}
            },
            "diverted": {
                "terms": {"field": "Diverted", "size": 2}
            },
            "airlines": {
                "terms": {"field": "Reporting_Airline", "size": 20, "order": {"_count": "desc"}}
            },
            "origins": {
                "terms": {"field": "Origin", "size": 20, "order": {"_count": "desc"}}
            },
            "destinations": {
                "terms": {"field": "Dest", "size": 20, "order": {"_count": "desc"}}
            },
            "flight_dates": {
                "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd",
                    "order": {"_key": "desc"}
                }
            }
        }
    }

    return make_es_request('POST', '/flights-*/_search', body)


def search_airlines(query: str, search_type: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Search airlines index."""
    if query:
        if search_type == 'keyword':
            # Only search the text subfield
            query_clause = {
                "match": {
                    "Airline_Name.text": {
                        "query": query
                    }
                }
            }
        elif search_type == 'semantic':
            # Only search the semantic_text subfield
            query_clause = {
                "semantic": {
                    "field": "Airline_Name.semantic",
                    "query": query
                }
            }
        else:
            # For ai search, use multi_match with both fields
            query_clause = {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "Airline_Name.text^2",
                        "Reporting_Airline^1.5"
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }
    else:
        query_clause = {"match_all": {}}

    # Add filters if provided
    if filters:
        filter_clauses = []
        if filters.get('airline_code'):
            filter_clauses.append({"term": {"Reporting_Airline": filters['airline_code']}})

        if filter_clauses:
            query_clause = {
                "bool": {
                    "must": [query_clause],
                    "filter": filter_clauses
                }
            }

    body = {
        "query": query_clause,
        "size": size,
        "_source": {
            "includes": ["Reporting_Airline", "Airline_Name"]
        },
        "aggs": {
            "airline_codes": {
                "terms": {"field": "Reporting_Airline", "size": 50, "order": {"_key": "asc"}}
            }
        }
    }

    # Add fields for semantic queries
    if search_type == 'semantic':
        body["fields"] = ["_inference_fields"]

    # Add appropriate highlighting based on search type
    if query:
        if search_type == 'semantic':
            body["highlight"] = {
                "fields": {
                    "Airline_Name.semantic": {
                        "type": "semantic",
                        "number_of_fragments": 1,
                        "fragment_size": 200
                    }
                }
            }
        else:
            body["highlight"] = {
                "fields": {
                    "Airline_Name.text": {},
                    "Reporting_Airline": {}
                }
            }

    # Get the search results
    search_result = make_es_request('POST', '/airlines/_search', body)

    # Get accurate count using _count API
    count_body = {
        "query": query_clause
    }
    count_result = make_es_request('POST', '/airlines/_count', count_body)
    total_count = count_result.get('count', 0)

    # Log the count for debugging
    LOGGER.info(f"Airlines count: {total_count}")

    # Update the total in the search result
    search_result['hits']['total'] = {'value': total_count, 'relation': 'eq'}

    return search_result


def search_contracts(query: str, search_type: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Search contracts index."""
    if search_type == 'semantic':
        return semantic_search(query, size, filters)
    elif search_type == 'ai':
        return ai_agent_search(query, size, filters)
    else:  # keyword
        return keyword_search(query, size, filters)


def keyword_search(query: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Perform Keyword search on contracts index.

    Only searches the attachment.content field.
    """
    if query:
        query_clause = {
            "match": {
                "attachment.content": {
                    "query": query
                }
            }
        }
    else:
        query_clause = {"match_all": {}}

    # Add filters if provided
    if filters:
        filter_clauses = []
        if filters.get('author'):
            filter_clauses.append({"term": {"attachment.author.keyword": filters['author']}})
        if filters.get('upload_year'):
            filter_clauses.append({
                "range": {
                    "upload_date": {
                        "gte": f"{filters['upload_year']}-01-01",
                        "lt": f"{int(filters['upload_year']) + 1}-01-01"
                    }
                }
            })

        if filter_clauses:
            query_clause = {
                "bool": {
                    "must": [query_clause],
                    "filter": filter_clauses
                }
            }

    body = {
        "query": query_clause,
        "size": size,
        "fields": ["_inference_fields"],
        "_source": {
            "includes": ["filename", "attachment.title", "upload_date", "attachment.author", "attachment.description"],
            "excludes": ["attachment.content", "content"]
        },
        "highlight": {
            "fields": {
                "attachment.content": {
                    "fragment_size": 150,
                    "number_of_fragments": 3
                }
            }
        },
        "aggs": {
            "authors": {
                "terms": {"field": "attachment.author.keyword", "size": 20, "order": {"_count": "desc"}}
            },
            "upload_years": {
                "date_histogram": {
                    "field": "upload_date",
                    "calendar_interval": "year",
                    "format": "yyyy",
                    "order": {"_key": "desc"}
                }
            }
        }
    }

    return make_es_request('POST', '/contracts/_search', body)


def semantic_search(query: str, size: int = 20, filters: Optional[Dict] = None) -> Dict:
    """Perform semantic search using only the semantic_content field."""
    if query:
        query_clause = {
            "semantic": {
                "field": "semantic_content",
                "query": query
            }
        }
    else:
        query_clause = {"match_all": {}}

    # Add filters if provided
    if filters:
        filter_clauses = []
        if filters.get('author'):
            filter_clauses.append({"term": {"attachment.author.keyword": filters['author']}})
        if filters.get('upload_year'):
            filter_clauses.append({
                "range": {
                    "upload_date": {
                        "gte": f"{filters['upload_year']}-01-01",
                        "lt": f"{int(filters['upload_year']) + 1}-01-01"
                    }
                }
            })

        if filter_clauses:
            # Wrap the existing query in a bool query with filters
            query_clause = {
                "bool": {
                    "must": [query_clause],
                    "filter": filter_clauses
                }
            }

    body = {
        "query": query_clause,
        "size": size,
        "fields": ["_inference_fields"],
        "_source": {
            "includes": ["filename", "attachment.title", "upload_date", "attachment.author", "attachment.description"],
            "excludes": ["attachment.content", "content"]
        },
        "aggs": {
            "authors": {
                "terms": {"field": "attachment.author.keyword", "size": 20, "order": {"_count": "desc"}}
            },
            "upload_years": {
                "date_histogram": {
                    "field": "upload_date",
                    "calendar_interval": "year",
                    "format": "yyyy",
                    "order": {"_key": "desc"}
                }
            }
        }
    }

    # Add highlighting for semantic and text fields to drive UI snippets
    if query:
        body["highlight"] = {
            "fields": {
                "semantic_content": {
                    "type": "semantic",
                    "number_of_fragments": 3,
                    "fragment_size": 20
                },
                "attachment.content": {
                    "fragment_size": 20,
                    "number_of_fragments": 3
                },
                "attachment.title": {},
                "attachment.description": {
                    "fragment_size": 20,
                    "number_of_fragments": 2
                }
            }
        }

    result = make_es_request('POST', '/contracts/_search', body)
    result['search_type'] = 'semantic'
    return result


def ai_agent_search(query: str, size: int = 20, filters: Optional[Dict] = None, stream: bool = False):
    """Perform AI Agent Builder search with tool calling."""
    if not query:
        return {} if not stream else iter([])

    if stream:
        body = {
            "input": query,
            "agent_id": "flight-ai"
        }
        response = make_kibana_request('POST', '/api/agent_builder/converse/async', body=body, stream=True)

        def event_stream():
            event_type: Optional[str] = None
            data_lines = []

            for raw_line in response.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue

                stripped = raw_line.rstrip('\r\n')
                LOGGER.debug("AI agent stream line: %s", stripped)

                if stripped == '':
                    if not data_lines:
                        event_type = None
                        continue

                    data_str = '\n'.join(data_lines)
                    data_lines = []

                    try:
                        payload = json.loads(data_str) if data_str else {}
                    except json.JSONDecodeError:
                        payload = {"raw": data_str}

                    if 'kind' not in payload:
                        if event_type:
                            payload['kind'] = event_type
                        elif 'type' in payload:
                            payload['kind'] = payload['type']

                    if event_type:
                        payload.setdefault('event', event_type)

                    serialized = (json.dumps(payload) + '\n').encode('utf-8')
                    yield serialized
                    event_type = None
                    continue

                if stripped.startswith(':'):
                    # Comment / heartbeat line
                    continue

                if stripped.startswith('event:'):
                    event_type = stripped[len('event:'):].strip() or None
                    continue

                if stripped.startswith('data:'):
                    data_lines.append(stripped[len('data:'):].lstrip())
                    continue

                # Fallback: treat as a data line
                data_lines.append(stripped)

            # Flush any remaining buffered data (in case stream ends without blank line)
            if data_lines:
                data_str = '\n'.join(data_lines)
                try:
                    payload = json.loads(data_str) if data_str else {}
                except json.JSONDecodeError:
                    payload = {"raw": data_str}

                if 'kind' not in payload:
                    if event_type:
                        payload['kind'] = event_type
                    elif 'type' in payload:
                        payload['kind'] = payload['type']

                if event_type:
                    payload.setdefault('event', event_type)

                yield (json.dumps(payload) + '\n').encode('utf-8')

        return event_stream()

    # This part of the function will now only be called for non-streaming requests
    # Hybrid search combining semantic and keyword search with RRF (Reciprocal Rank Fusion)
    query_clause = {
        "bool": {
            "should": [
                {
                    "semantic": {
                        "semantic_content": {
                            "query": query
                        }
                    }
                },
                {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "attachment.content^2",
                            "attachment.title^1.5",
                            "attachment.description",
                            "filename"
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                }
            ]
        }
    }

    # Add filters if provided
    if filters:
        filter_clauses = []
        if filters.get('author'):
            filter_clauses.append({"term": {"attachment.author.keyword": filters['author']}})
        if filters.get('upload_year'):
            filter_clauses.append({
                "range": {
                    "upload_date": {
                        "gte": f"{filters['upload_year']}-01-01",
                        "lt": f"{int(filters['upload_year']) + 1}-01-01"
                    }
                }
            })

        if filter_clauses:
            # Wrap the existing query in a bool query with filters
            query_clause = {
                "bool": {
                    "must": [query_clause],
                    "filter": filter_clauses
                }
            }

    body = {
        "query": query_clause,
        "size": size,
        "fields": ["_inference_fields"],
        "_source": {
            "includes": ["filename", "attachment.title", "upload_date", "attachment.author", "attachment.description"],
            "excludes": ["attachment.content", "content"]
        },
        "highlight": {
            "fields": {
                "attachment.content": {
                    "fragment_size": 200,
                    "number_of_fragments": 3
                },
                "attachment.title": {},
                "attachment.description": {
                    "fragment_size": 150,
                    "number_of_fragments": 2
                }
            }
        },
        "aggs": {
            "authors": {
                "terms": {"field": "attachment.author.keyword", "size": 20, "order": {"_count": "desc"}}
            },
            "upload_years": {
                "date_histogram": {
                    "field": "upload_date",
                    "calendar_interval": "year",
                    "format": "yyyy",
                    "order": {"_key": "desc"}
                }
            }
        }
    }

    # Only use RRF when there's a query (it's not needed for match_all)
    if query:
        body["rank"] = {"rrf": {}}

    try:
        results = make_es_request('POST', '/contracts/_search', body)
        results['search_type'] = 'ai_agent'
        return results
    except Exception as e:
        # If RRF fails, try without it
        if "rank" in str(e).lower() or "rrf" in str(e).lower():
            LOGGER.warning(f"RRF not supported, trying hybrid search without RRF: {e}")
            # Remove rank clause and retry
            if "rank" in body:
                del body["rank"]
            try:
                results = make_es_request('POST', '/contracts/_search', body)
                results['search_type'] = 'ai_agent'
                return results
            except Exception as e2:
                LOGGER.warning(f"Hybrid search failed, falling back to semantic: {e2}")
                return semantic_search(query, size, filters)
        else:
            # Fallback to semantic search if hybrid fails for other reasons
            LOGGER.warning(f"Hybrid search failed, falling back to semantic: {e}")
            return semantic_search(query, size, filters)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5000)
