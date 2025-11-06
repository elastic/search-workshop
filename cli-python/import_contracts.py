#!/usr/bin/env python3
"""
Combined script to setup pipeline, create index, and ingest PDFs.
Runs all steps in sequence for easy deployment.
"""

import sys
import argparse
import requests
import base64
import json
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR.parent / "config" / "elasticsearch.yml"

# Default values
ES_INDEX = 'contracts'
ELSER_MODEL = '.elser_model_2_linux-x86_64'


def load_yaml(path: Path) -> dict:
    """Load YAML configuration file."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load configuration files. Install with 'pip install PyYAML'."
        )

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Configuration must be a mapping (found {type(data).__name__})")
    return data


def build_auth_header(config: dict) -> str:
    """Build authorization header from config (prefer api_key over user/password)."""
    api_key = config.get("api_key", "").strip()
    if api_key:
        return f"ApiKey {api_key}"
    
    user = config.get("user", "").strip()
    password = config.get("password", "").strip()
    if user and password:
        token = f"{user}:{password}"
        encoded = base64.b64encode(token.encode("utf-8")).decode("ascii")
        return f"Basic {encoded}"
    
    raise ValueError("Config must include either 'api_key' or both 'user' and 'password'")


def load_config(config_path: Path = None) -> tuple:
    """Load Elasticsearch configuration and return endpoint and headers."""
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    
    config = load_yaml(config_path)
    
    endpoint = config.get("endpoint", "").strip()
    if not endpoint:
        raise ValueError("Config must include an 'endpoint'")
    
    auth_header = build_auth_header(config)
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json"
    }
    
    # Add any custom headers from config
    custom_headers = config.get("headers", {})
    if isinstance(custom_headers, dict):
        headers.update({str(k): str(v) for k, v in custom_headers.items()})
    
    return endpoint, headers


# Global variables (will be set in main)
ES_ENDPOINT = None
headers = None
INFERENCE_ENDPOINT = '.elser-2-elastic'  # Default, will be auto-detected if not found


def print_header(title):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")


def check_elasticsearch():
    """Check if Elasticsearch is reachable."""
    print_header("Checking Elasticsearch Connection")
    try:
        response = requests.get(ES_ENDPOINT, headers=headers, timeout=5)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ Connected to Elasticsearch")
            print(f"   Cluster: {info.get('cluster_name', 'unknown')}")
            print(f"   Version: {info.get('version', {}).get('number', 'unknown')}")
            return True
        else:
            print(f"❌ Failed to connect: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")
        return False


def check_inference_endpoint():
    """Check if ELSER inference endpoint is available, auto-detect if needed."""
    global INFERENCE_ENDPOINT
    
    print_header("Checking ELSER Inference Endpoint")
    try:
        response = requests.get(f"{ES_ENDPOINT}/_inference/_all", headers=headers)
        if response.status_code == 200:
            endpoints = response.json().get('endpoints', [])
            
            # First, try to find the specified endpoint
            for endpoint in endpoints:
                if endpoint.get('inference_id') == INFERENCE_ENDPOINT:
                    print(f"✅ Found inference endpoint: {INFERENCE_ENDPOINT}")
                    print(f"   Task type: {endpoint.get('task_type', 'unknown')}")
                    return True
            
            # If not found, try to auto-detect any ELSER endpoint
            elser_endpoints = []
            for endpoint in endpoints:
                inference_id = endpoint.get('inference_id', '')
                if 'elser' in inference_id.lower():
                    elser_endpoints.append(inference_id)
            
            if elser_endpoints:
                # Prefer endpoints starting with .elser-2- or .elser_model_2
                preferred = [e for e in elser_endpoints if '.elser-2-' in e or '.elser_model_2' in e]
                if preferred:
                    INFERENCE_ENDPOINT = preferred[0]
                else:
                    INFERENCE_ENDPOINT = elser_endpoints[0]
                
                # Find the endpoint object to get task type
                endpoint_obj = next((e for e in endpoints if e.get('inference_id') == INFERENCE_ENDPOINT), None)
                task_type = endpoint_obj.get('task_type', 'unknown') if endpoint_obj else 'unknown'
                
                print(f"⚠️  Specified endpoint not found, using auto-detected: {INFERENCE_ENDPOINT}")
                print(f"   Task type: {task_type}")
                return True
            
            print(f"❌ Inference endpoint '{INFERENCE_ENDPOINT}' not found")
            print(f"\n   Available endpoints:")
            for endpoint in endpoints:
                print(f"   - {endpoint.get('inference_id')}")
            return False
        else:
            print(f"⚠️  Could not check inference endpoints: HTTP {response.status_code}")
            return True  # Continue anyway
    except Exception as e:
        print(f"⚠️  Error checking inference endpoint: {str(e)}")
        return True  # Continue anyway


def create_pipeline():
    """Create the PDF processing pipeline."""
    print_header("Creating PDF Processing Pipeline")

    pipeline_config = {
        'description': 'Extract text from PDF - semantic_text field handles chunking and embeddings',
        'processors': [
            {
                'attachment': {
                    'field': 'data',
                    'target_field': 'attachment',
                    'remove_binary': True
                }
            },
            {
                'set': {
                    'field': 'semantic_content',
                    'copy_from': 'attachment.content',
                    'ignore_empty_value': True
                }
            },
            {
                'remove': {
                    'field': 'data',
                    'ignore_missing': True
                }
            },
            {
                'set': {
                    'field': 'upload_date',
                    'value': '{{ _ingest.timestamp }}'
                }
            }
        ]
    }

    try:
        response = requests.put(
            f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline",
            headers=headers,
            json=pipeline_config
        )

        if response.status_code == 200:
            print(f"✅ Pipeline created/updated successfully")
            return True
        else:
            print(f"❌ Failed to create pipeline: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False
    except Exception as e:
        print(f"❌ Error creating pipeline: {str(e)}")
        return False


def check_index_exists():
    """Check if the index already exists."""
    try:
        response = requests.head(f"{ES_ENDPOINT}/{ES_INDEX}", headers=headers)
        return response.status_code == 200
    except:
        return False


def delete_index():
    """Delete the index if it exists."""
    print(f"🗑️  Deleting existing index: {ES_INDEX}")
    try:
        response = requests.delete(f"{ES_ENDPOINT}/{ES_INDEX}", headers=headers)
        if response.status_code == 200:
            print(f"✅ Index deleted successfully")
            return True
        else:
            print(f"❌ Failed to delete index: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error deleting index: {str(e)}")
        return False


def create_index():
    """Create index with proper mappings."""
    print_header("Creating Contracts Index")

    # Check if index exists
    if check_index_exists():
        print(f"⚠️  Index '{ES_INDEX}' already exists")
        return False

    mapping = {
        'mappings': {
            'properties': {
                'filename': {'type': 'keyword'},
                'airline': {'type': 'keyword'},
                'upload_date': {'type': 'date'},
                'attachment': {
                    'properties': {
                        'content': {
                            'type': 'text',
                            'fields': {
                                'keyword': {
                                    'type': 'keyword',
                                    'ignore_above': 256
                                }
                            }
                        },
                        'title': {'type': 'text'},
                        'author': {'type': 'keyword'},
                        'date': {'type': 'date'},
                        'content_type': {'type': 'keyword'},
                        'content_length': {'type': 'long'},
                        'language': {'type': 'keyword'},
                        'keywords': {'type': 'text'},
                        'creator_tool': {'type': 'keyword'}
                    }
                },
                'semantic_content': {
                    'type': 'semantic_text',
                    'inference_id': INFERENCE_ENDPOINT
                }
            }
        }
    }

    try:
        response = requests.put(
            f"{ES_ENDPOINT}/{ES_INDEX}",
            headers=headers,
            json=mapping
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ Index created successfully")
            print(f"   - Acknowledged: {result.get('acknowledged', False)}")
            print(f"   - Shards acknowledged: {result.get('shards_acknowledged', False)}")
            print(f"   - Inference endpoint: {INFERENCE_ENDPOINT}")
            return True
        else:
            print(f"❌ Failed to create index: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False
    except Exception as e:
        print(f"❌ Error creating index: {str(e)}")
        return False


def get_pdf_files(path):
    """Get list of PDF files from a path."""
    path_obj = Path(path)

    if not path_obj.exists():
        print(f"❌ Path '{path}' does not exist")
        return []

    if path_obj.is_file():
        if path_obj.suffix.lower() == '.pdf':
            return [path_obj]
        else:
            print(f"❌ '{path}' is not a PDF file")
            return []

    elif path_obj.is_dir():
        pdf_files = list(path_obj.glob('*.pdf'))
        if not pdf_files:
            print(f"⚠️  No PDF files found in directory '{path}'")
        return pdf_files

    return []


def extract_airline_name(filename):
    """Extract airline name from filename."""
    filename_lower = filename.lower()

    # Handle both old and new naming conventions
    if 'american' in filename_lower:
        return 'American Airlines'
    elif 'southwest' in filename_lower:
        return 'Southwest'
    elif 'united' in filename_lower:
        return 'United'
    elif 'delta' in filename_lower or 'dl-' in filename_lower:
        return 'Delta'
    else:
        return 'Unknown'


def index_pdf(pdf_path):
    """Index a single PDF file."""
    pdf_path = Path(pdf_path)
    filename = pdf_path.name
    airline = extract_airline_name(filename)

    print(f"\nProcessing: {filename}")
    print(f"Airline: {airline}")

    try:
        # Read and encode the PDF
        with open(pdf_path, 'rb') as pdf_file:
            encoded_pdf = base64.b64encode(pdf_file.read()).decode('utf-8')

        file_size_mb = len(encoded_pdf) / 1024 / 1024 * 0.75
        print(f"File size: {file_size_mb:.2f} MB")

        # Index the document
        index_payload = {
            "data": encoded_pdf,
            "filename": filename,
            "airline": airline
        }

        response = requests.post(
            f"{ES_ENDPOINT}/{ES_INDEX}/_doc?pipeline=pdf_pipeline",
            headers=headers,
            json=index_payload
        )

        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ Successfully indexed: {filename}")
            print(f"   Document ID: {result.get('_id', 'N/A')}")
            return True
        else:
            print(f"❌ Indexing failed: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False

    except Exception as e:
        print(f"❌ Error processing {filename}: {str(e)}")
        return False


def ingest_pdfs(pdf_path):
    """Ingest all PDFs from the specified path."""
    print_header("Ingesting PDF Files")

    pdf_files = get_pdf_files(pdf_path)

    if not pdf_files:
        print("❌ No PDF files to process")
        return False

    print(f"Found {len(pdf_files)} PDF file(s) to process\n")

    success_count = 0
    failed_count = 0

    for pdf_file in pdf_files:
        if index_pdf(pdf_file):
            success_count += 1
        else:
            failed_count += 1

    # Summary
    print_header("SUMMARY")
    print(f"Total files: {len(pdf_files)}")
    print(f"✅ Successfully indexed: {success_count}")
    if failed_count > 0:
        print(f"❌ Failed: {failed_count}")

    return failed_count == 0


def verify_ingestion():
    """Verify documents were ingested successfully."""
    print_header("Verifying Ingestion")

    try:
        response = requests.get(f"{ES_ENDPOINT}/{ES_INDEX}/_count", headers=headers)
        if response.status_code == 200:
            count = response.json().get('count', 0)
            print(f"✅ Index '{ES_INDEX}' contains {count} document(s)")
            return True
        else:
            print(f"⚠️  Could not verify document count")
            return True
    except Exception as e:
        print(f"⚠️  Error verifying: {str(e)}")
        return True


def main():
    global ES_ENDPOINT, headers, INFERENCE_ENDPOINT
    
    parser = argparse.ArgumentParser(
        description='Setup Elasticsearch infrastructure and ingest PDF files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Setup and ingest PDFs from default location
  python3 setup_and_ingest.py

  # Setup and ingest PDFs from specific directory
  python3 setup_and_ingest.py --pdf-path /path/to/pdfs

  # Recreate index if it already exists
  python3 setup_and_ingest.py --recreate

  # Only setup infrastructure (skip PDF ingestion)
  python3 setup_and_ingest.py --setup-only

  # Skip setup and only ingest PDFs
  python3 setup_and_ingest.py --ingest-only
        """
    )

    parser.add_argument(
        '-c', '--config',
        default=str(DEFAULT_CONFIG_PATH),
        help=f'Path to Elasticsearch config YAML (default: {DEFAULT_CONFIG_PATH})'
    )
    parser.add_argument(
        '--pdf-path',
        default='airline_contracts',
        help='Path to PDF file or directory containing PDFs (default: airline_contracts)'
    )
    parser.add_argument(
        '--recreate',
        action='store_true',
        help='Delete and recreate the index if it exists'
    )
    parser.add_argument(
        '--setup-only',
        action='store_true',
        help='Only setup infrastructure (pipeline and index), skip PDF ingestion'
    )
    parser.add_argument(
        '--ingest-only',
        action='store_true',
        help='Skip setup, only ingest PDFs (assumes infrastructure exists)'
    )
    parser.add_argument(
        '--inference-endpoint',
        help='Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)'
    )

    args = parser.parse_args()
    
    # Set inference endpoint if provided
    if args.inference_endpoint:
        INFERENCE_ENDPOINT = args.inference_endpoint
    
    # Load configuration
    try:
        ES_ENDPOINT, headers = load_config(Path(args.config))
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        sys.exit(1)

    print("\n" + "="*60)
    print("Elasticsearch PDF Ingestion Setup")
    print("="*60)
    print(f"Endpoint: {ES_ENDPOINT}")
    print(f"Index: {ES_INDEX}")
    print(f"Inference: {INFERENCE_ENDPOINT}")
    print("="*60)

    # Check Elasticsearch connection
    if not check_elasticsearch():
        print("\n❌ Cannot connect to Elasticsearch. Exiting.")
        sys.exit(1)

    # Setup phase
    if not args.ingest_only:
        # Check ELSER endpoint
        if not check_inference_endpoint():
            print("\n⚠️  ELSER inference endpoint not found!")
            print("   Please deploy ELSER via Kibana or API before continuing.")
            print("   See: Management → Machine Learning → Trained Models → ELSER → Deploy")
            sys.exit(1)

        # Create pipeline
        if not create_pipeline():
            print("\n❌ Failed to create pipeline. Exiting.")
            sys.exit(1)

        # Handle index creation/recreation
        if check_index_exists():
            if args.recreate:
                if not delete_index():
                    print("\n❌ Failed to delete existing index. Exiting.")
                    sys.exit(1)
                if not create_index():
                    print("\n❌ Failed to create index. Exiting.")
                    sys.exit(1)
            else:
                print(f"\n⚠️  Index '{ES_INDEX}' already exists. Use --recreate to delete and recreate.")
                if not args.setup_only:
                    print("   Continuing with PDF ingestion...")
        else:
            if not create_index():
                print("\n❌ Failed to create index. Exiting.")
                sys.exit(1)

    # Ingestion phase
    if not args.setup_only:
        import time
        start_time = time.time()

        if not ingest_pdfs(args.pdf_path):
            print("\n❌ PDF ingestion had errors.")
            sys.exit(1)

        elapsed_time = time.time() - start_time
        print(f"\n⏱️  Total ingestion time: {elapsed_time:.2f} seconds")

        # Verify ingestion
        verify_ingestion()

    print("\n" + "="*60)
    print("✅ Setup Complete!")
    print("="*60)

    if not args.setup_only:
        print("\n💡 Note: ELSER embeddings are generated asynchronously.")
        print("   Wait ~30-60 seconds before testing semantic search.")
        print("\nTest semantic search:")
        auth_header = headers.get("Authorization", "")
        print(f'  curl -H "Authorization: {auth_header}" \\')
        print(f'    -H "Content-Type: application/json" \\')
        print(f'    {ES_ENDPOINT}/{ES_INDEX}/_search -d \'{{')
        print('    "query": {"semantic": {"field": "semantic_content", "query": "baggage fees"}},')
        print('    "_source": ["filename", "airline"]')
        print("  }' | python3 -m json.tool")

    print()


if __name__ == "__main__":
    main()
