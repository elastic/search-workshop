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
DEFAULT_MAPPING_PATH = SCRIPT_DIR.parent / "config" / "mappings-contracts.json"

# Default values
ES_INDEX = 'contracts'
DEFAULT_INFERENCE_ENDPOINT = '.elser-2-elastic'


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


def load_json(path: Path) -> dict:
    """Load JSON configuration file."""
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(f"JSON file must define a JSON object (found {type(data).__name__})")
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
INFERENCE_ENDPOINT = DEFAULT_INFERENCE_ENDPOINT  # Default, will be auto-detected if not found


def check_elasticsearch():
    """Check if Elasticsearch is reachable."""
    try:
        response = requests.get(ES_ENDPOINT, headers=headers, timeout=5)
        if response.status_code == 200:
            info = response.json()
            print(f"Cluster: {info.get('cluster_name', 'unknown')}")
            print(f"Status: {info.get('status', 'unknown')}")
            return True
        else:
            print(f"Failed to connect: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"Connection error: {str(e)}")
        return False


def check_inference_endpoint():
    """Check if ELSER inference endpoint is available, auto-detect if needed."""
    global INFERENCE_ENDPOINT
    
    try:
        response = requests.get(f"{ES_ENDPOINT}/_inference/_all", headers=headers)
        if response.status_code == 200:
            endpoints = response.json().get('endpoints', [])
            
            # First, try to find the specified endpoint
            for endpoint in endpoints:
                if endpoint.get('inference_id') == INFERENCE_ENDPOINT:
                    print(f"Found inference endpoint: {INFERENCE_ENDPOINT}")
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
                
                print(f"Specified endpoint not found, using auto-detected: {INFERENCE_ENDPOINT}")
                return True
            
            print(f"Inference endpoint '{INFERENCE_ENDPOINT}' not found")
            print("Available endpoints:")
            for endpoint in endpoints:
                print(f"  - {endpoint.get('inference_id')}")
            return False
        else:
            print(f"Could not check inference endpoints: HTTP {response.status_code}")
            return True  # Continue anyway
    except Exception as e:
        print(f"Error checking inference endpoint: {str(e)}")
        print("Continuing anyway...")
        return True  # Continue anyway


def create_pipeline():
    """Create the PDF processing pipeline."""
    pipeline_path = SCRIPT_DIR.parent / "config" / "pipeline-contracts.json"
    
    try:
        pipeline_config = load_json(pipeline_path)
    except Exception as e:
        print(f"Error loading pipeline config: {e}")
        return False

    try:
        response = requests.put(
            f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline",
            headers=headers,
            json=pipeline_config
        )

        if response.status_code == 200:
            return True
        else:
            print(f"Failed to create pipeline: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False
    except Exception as e:
        print(f"Error creating pipeline: {str(e)}")
        return False


def check_index_exists():
    """Check if the index already exists."""
    try:
        response = requests.head(f"{ES_ENDPOINT}/{ES_INDEX}", headers=headers)
        return response.status_code == 200
    except:
        return False


def create_index(mapping):
    """Create index with proper mappings."""
    # Delete index if it exists before creating a new one
    if check_index_exists():
        print(f"Deleting existing index '{ES_INDEX}' before import")
        try:
            response = requests.delete(f"{ES_ENDPOINT}/{ES_INDEX}", headers=headers)
            if response.status_code == 200:
                print(f"Index '{ES_INDEX}' deleted")
            else:
                print(f"Failed to delete index: HTTP {response.status_code}")
        except Exception as e:
            print(f"Failed to delete index: {str(e)}")

    # Update mapping with detected inference endpoint
    mapping_with_inference = mapping.copy()
    if 'mappings' in mapping_with_inference and 'properties' in mapping_with_inference['mappings']:
        if 'semantic_content' in mapping_with_inference['mappings']['properties']:
            mapping_with_inference['mappings']['properties']['semantic_content']['inference_id'] = INFERENCE_ENDPOINT

    print(f"Creating index: {ES_INDEX}")
    try:
        response = requests.put(
            f"{ES_ENDPOINT}/{ES_INDEX}",
            headers=headers,
            json=mapping_with_inference
        )

        if response.status_code == 200:
            print(f"Successfully created index: {ES_INDEX}")
            return True
        else:
            print(f"Failed to create index: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False
    except Exception as e:
        print(f"Error creating index: {str(e)}")
        return False


def get_pdf_files(path):
    """Get list of PDF files from a path."""
    path_obj = Path(path)

    if not path_obj.exists():
        print(f"Path '{path}' does not exist")
        return []

    if path_obj.is_file():
        if path_obj.suffix.lower() == '.pdf':
            return [path_obj]
        else:
            print(f"'{path}' is not a PDF file")
            return []

    elif path_obj.is_dir():
        pdf_files = list(path_obj.glob('*.pdf'))
        if not pdf_files:
            print(f"No PDF files found in directory '{path}'")
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

    try:
        # Read and encode the PDF
        with open(pdf_path, 'rb') as pdf_file:
            encoded_pdf = base64.b64encode(pdf_file.read()).decode('utf-8')

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
            # Don't print here - progress is handled in ingest_pdfs()
            return True
        else:
            # Only print errors, not success messages
            print(f"\nIndexing failed for {filename}: HTTP {response.status_code}")
            print(json.dumps(response.json(), indent=2))
            return False

    except Exception as e:
        print(f"\nError processing {filename}: {str(e)}")
        return False


def ingest_pdfs(pdf_path):
    """Ingest all PDFs from the specified path."""
    pdf_files = get_pdf_files(pdf_path)

    if not pdf_files:
        print("No PDF files to process")
        return False

    total_files = len(pdf_files)
    print(f"Processing {total_files} PDF file(s)...")

    success_count = 0
    failed_count = 0
    processed_count = 0

    for pdf_file in pdf_files:
        if index_pdf(pdf_file):
            success_count += 1
        else:
            failed_count += 1
        
        processed_count += 1
        
        # Update progress
        percentage = round(processed_count / total_files * 100, 1)
        progress = f"\r{processed_count} of {total_files} files processed ({percentage}%)"
        sys.stdout.write(progress)
        sys.stdout.flush()

    # Print newline after progress line
    print()
    
    print(f"Indexed {success_count} of {total_files} file(s)")
    if failed_count > 0:
        print(f"Failed: {failed_count}")

    return failed_count == 0


def verify_ingestion():
    """Verify documents were ingested successfully."""
    try:
        response = requests.get(f"{ES_ENDPOINT}/{ES_INDEX}/_count", headers=headers)
        if response.status_code == 200:
            count = response.json().get('count', 0)
            print(f"Index '{ES_INDEX}' contains {count} document(s)")
            return True
        else:
            print(f"Could not verify document count")
            return True
    except Exception as e:
        print(f"Could not verify document count: {str(e)}")
        return True


def main():
    global ES_ENDPOINT, headers, INFERENCE_ENDPOINT
    
    parser = argparse.ArgumentParser(
        description='Setup Elasticsearch infrastructure and ingest PDF files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Setup and ingest PDFs from default location
  python3 import_contracts.py

  # Setup and ingest PDFs from specific directory
  python3 import_contracts.py --pdf-path /path/to/pdfs

  # Only setup infrastructure (skip PDF ingestion)
  python3 import_contracts.py --setup-only

  # Skip setup and only ingest PDFs
  python3 import_contracts.py --ingest-only
        """
    )

    parser.add_argument(
        '-c', '--config',
        default=str(DEFAULT_CONFIG_PATH),
        help=f'Path to Elasticsearch config YAML (default: {DEFAULT_CONFIG_PATH})'
    )
    parser.add_argument(
        '-m', '--mapping',
        default=str(DEFAULT_MAPPING_PATH),
        help=f'Path to mappings JSON (default: {DEFAULT_MAPPING_PATH})'
    )
    parser.add_argument(
        '--pdf-path',
        default='data',
        help='Path to PDF file or directory containing PDFs (default: data)'
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
        print(f"Failed to load config: {e}")
        sys.exit(1)

    # Load mapping
    try:
        mapping = load_json(Path(args.mapping).resolve())
    except Exception as e:
        print(f"Failed to load mapping: {e}")
        sys.exit(1)

    # Check Elasticsearch connection
    if not check_elasticsearch():
        print("Cannot connect to Elasticsearch. Exiting.")
        sys.exit(1)

    # Setup phase
    if not args.ingest_only:
        # Check ELSER endpoint
        if not check_inference_endpoint():
            print("ELSER inference endpoint not found!")
            print("Please deploy ELSER via Kibana or API before continuing.")
            print("See: Management → Machine Learning → Trained Models → ELSER → Deploy")
            sys.exit(1)

        # Create pipeline
        if not create_pipeline():
            print("Failed to create pipeline. Exiting.")
            sys.exit(1)

        # Create index (will delete existing one if present)
        if not create_index(mapping):
            print("Failed to create index. Exiting.")
            sys.exit(1)

    # Ingestion phase
    if not args.setup_only:
        import time
        start_time = time.time()

        if not ingest_pdfs(args.pdf_path):
            print("PDF ingestion had errors.")
            sys.exit(1)

        elapsed_time = time.time() - start_time
        print(f"Total ingestion time: {elapsed_time:.2f} seconds")

        # Verify ingestion
        verify_ingestion()


if __name__ == "__main__":
    main()
