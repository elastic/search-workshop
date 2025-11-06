#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""AIR Search - Flask web app for searching flights, airlines, and contracts indices with BM25, Semantic, and AI Agent search."""

import json
import logging
import os
import ssl
from pathlib import Path
from typing import Dict, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import yaml
from flask import Flask, jsonify, request
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


@app.route('/')
def index():
    """Serve the main page."""
    return app.send_static_file('index.html')


@app.route('/api/search', methods=['POST'])
def search():
    """Search endpoint that routes to appropriate search type."""
    data = request.get_json() or {}
    search_type = data.get('type', 'bm25')
    query = data.get('query', '').strip()
    index_name = data.get('index', 'all')  # 'all', 'flights', 'airlines', or 'contracts'
    
    if not query:
        return jsonify({'error': 'Query is required'}), 400
    
    try:
        if index_name == 'all':
            # Search all indices and combine results
            results = search_all_indices(query, search_type)
        elif index_name == 'flights':
            results = search_flights(query, search_type)
        elif index_name == 'airlines':
            results = search_airlines(query, search_type)
        elif index_name == 'contracts':
            results = search_contracts(query, search_type)
        else:
            return jsonify({'error': f'Unknown index: {index_name}'}), 400
        
        return jsonify(results)
    except urllib_error.HTTPError as e:
        error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
        LOGGER.error(f"Elasticsearch HTTP error: {e.code} - {error_body}")
        try:
            error_json = json.loads(error_body) if error_body else {}
            error_msg = error_json.get('error', {}).get('root_cause', [{}])[0].get('reason', error_body)
        except:
            error_msg = f"Elasticsearch error: {e.code} {e.reason}"
        return jsonify({'error': error_msg}), 500
    except Exception as e:
        LOGGER.error(f"Search error: {e}", exc_info=True)
        return jsonify({'error': f'Search failed: {str(e)}'}), 500


def search_all_indices(query: str, search_type: str, size: int = 20) -> Dict:
    """Search across all indices (flights, airlines, contracts) and combine results."""
    indices = ['flights', 'airlines', 'contracts']
    all_hits = []
    total_hits = 0
    
    # Calculate size per index (ensure at least 1)
    size_per_index = max(1, size // len(indices))
    
    for index in indices:
        try:
            if index == 'flights':
                result = search_flights(query, search_type, size_per_index)
            elif index == 'airlines':
                result = search_airlines(query, search_type, size_per_index)
            else:  # contracts
                result = search_contracts(query, search_type, size_per_index)
            
            if result.get('hits', {}).get('hits'):
                for hit in result['hits']['hits']:
                    hit['_index'] = index  # Tag each hit with its index
                    all_hits.append(hit)
                
                # Extract total count
                result_total = result.get('hits', {}).get('total', 0)
                if isinstance(result_total, dict):
                    total_hits += result_total.get('value', 0)
                else:
                    total_hits += result_total
        except Exception as e:
            LOGGER.warning(f"Error searching {index} index: {e}")
            continue
    
    # Sort by score (if available) and limit to size
    all_hits.sort(key=lambda x: x.get('_score', 0), reverse=True)
    all_hits = all_hits[:size]
    
    return {
        'hits': {
            'total': {'value': total_hits, 'relation': 'eq'},
            'hits': all_hits
        },
        'search_type': search_type,
        'searched_indices': indices
    }


def search_flights(query: str, search_type: str, size: int = 20) -> Dict:
    """Search flights index."""
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "Flight_Number^2",
                    "Reporting_Airline^1.5",
                    "Origin^1.5",
                    "Dest^1.5",
                    "Tail_Number"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        },
        "size": size,
        "_source": {
            "includes": [
                "FlightID", "Reporting_Airline", "Flight_Number", "Origin", "Dest",
                "CRSDepTimeLocal", "CRSArrTimeLocal", "DepDelayMin", "ArrDelayMin",
                "Cancelled", "DistanceMiles", "@timestamp"
            ]
        },
        "highlight": {
            "fields": {
                "Flight_Number": {},
                "Reporting_Airline": {},
                "Origin": {},
                "Dest": {}
            }
        }
    }
    
    return make_es_request('POST', '/flights/_search', body)


def search_airlines(query: str, search_type: str, size: int = 20) -> Dict:
    """Search airlines index."""
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "Airline_Name^2",
                    "Reporting_Airline^1.5"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        },
        "size": size,
        "_source": {
            "includes": ["Reporting_Airline", "Airline_Name"]
        },
        "highlight": {
            "fields": {
                "Airline_Name": {},
                "Reporting_Airline": {}
            }
        }
    }
    
    return make_es_request('POST', '/airlines/_search', body)


def search_contracts(query: str, search_type: str, size: int = 20) -> Dict:
    """Search contracts index."""
    if search_type == 'semantic':
        return semantic_search(query, size)
    elif search_type == 'ai':
        return ai_agent_search(query, size)
    else:  # bm25
        return bm25_search(query, size)


def bm25_search(query: str, size: int = 20) -> Dict:
    """Perform BM25 (keyword) search on contracts index."""
    body = {
        "query": {
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
        },
        "size": size,
        "_source": {
            "includes": ["filename", "attachment.title", "attachment.content", "upload_date", "attachment.author", "attachment.description"]
        },
        "highlight": {
            "fields": {
                "attachment.content": {
                    "fragment_size": 150,
                    "number_of_fragments": 3
                },
                "attachment.title": {}
            }
        }
    }
    
    return make_es_request('POST', '/contracts/_search', body)


def semantic_search(query: str, size: int = 20) -> Dict:
    """Perform semantic search using semantic_content field."""
    body = {
        "query": {
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
                        "match": {
                            "attachment.title": {
                                "query": query,
                                "boost": 0.5
                            }
                        }
                    }
                ]
            }
        },
        "size": size,
        "_source": {
            "includes": ["filename", "attachment.title", "attachment.content", "upload_date", "attachment.author", "attachment.description"]
        },
        "highlight": {
            "fields": {
                "attachment.content": {
                    "fragment_size": 150,
                    "number_of_fragments": 3
                },
                "attachment.title": {},
                "attachment.description": {
                    "fragment_size": 150,
                    "number_of_fragments": 2
                }
            }
        }
    }
    
    return make_es_request('POST', '/contracts/_search', body)


def ai_agent_search(query: str, size: int = 20) -> Dict:
    """Perform AI Agent Builder search with tool calling.
    
    This uses a hybrid approach combining semantic and BM25 search,
    and can optionally use Elasticsearch inference API for enhanced results.
    """
    # Hybrid search combining semantic and keyword search with RRF (Reciprocal Rank Fusion)
    body = {
        "query": {
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
        },
        "size": size,
        "_source": {
            "includes": ["filename", "attachment.title", "attachment.content", "upload_date", "attachment.author", "attachment.description"]
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
        # Use RRF for better result fusion (if available in Elasticsearch 8.11+)
        # Remove this if your cluster doesn't support it
        "rank": {
            "rrf": {}
        }
    }
    
    try:
        results = make_es_request('POST', '/contracts/_search', body)
        results['search_type'] = 'ai_agent'
        return results
    except Exception as e:
        # If RRF fails, try without it
        if "rank" in str(e).lower() or "rrf" in str(e).lower():
            LOGGER.warning(f"RRF not supported, trying hybrid search without RRF: {e}")
            # Remove rank clause and retry
            del body["rank"]
            try:
                results = make_es_request('POST', '/contracts/_search', body)
                results['search_type'] = 'ai_agent'
                return results
            except Exception as e2:
                LOGGER.warning(f"Hybrid search failed, falling back to semantic: {e2}")
                return semantic_search(query, size)
        else:
            # Fallback to semantic search if hybrid fails for other reasons
            LOGGER.warning(f"Hybrid search failed, falling back to semantic: {e}")
            return semantic_search(query, size)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5000)
