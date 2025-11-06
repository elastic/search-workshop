import os
import sys
import argparse
import base64
import copy
import json
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, List
from urllib import parse as urllib_parse

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    yaml = None

# Load environment variables from .env file
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "elasticsearch.yml"
DEFAULT_MAPPING_PATH = PROJECT_ROOT / "config" / "mappings-contracts.json"
DEFAULT_DATA_PATH = PROJECT_ROOT / "data"

load_dotenv(PROJECT_ROOT / ".env")

ES_INDEX = os.getenv('ES_INDEX', 'contracts')  # Default to 'contracts' if not set
INFERENCE_ENDPOINT = os.getenv('INFERENCE_ENDPOINT', '.elser_model_2')


def _default_elser_model() -> str:
    arch = platform.machine().lower()
    system = platform.system().lower()
    if "arm" in arch or "aarch" in arch or system == "darwin":
        return ".elser_model_2"
    return ".elser_model_2_linux-x86_64"


ELSER_MODEL = os.getenv('ELSER_MODEL', _default_elser_model())
PIPELINE_NAME = "pdf_pipeline"


def get_pdf_files(path):
    """Get list of PDF files from a path (file or directory)."""
    path_obj = Path(path)

    if not path_obj.exists():
        print(f"Error: Path '{path}' does not exist")
        return []

    if path_obj.is_file():
        if path_obj.suffix.lower() == '.pdf':
            return [path_obj]
        else:
            print(f"Error: '{path}' is not a PDF file")
            return []

    elif path_obj.is_dir():
        pdf_files = list(path_obj.glob('*.pdf'))
        if not pdf_files:
            print(f"Warning: No PDF files found in directory '{path}'")
        return pdf_files

    return []


def extract_airline_name(filename):
    """Extract airline name from filename."""
    filename_lower = filename.lower()

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


def load_yaml(path: Path) -> Dict[str, object]:
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


@dataclass
class ElasticsearchConfig:
    endpoint: str
    headers: Dict[str, str]
    user: Optional[str]
    password: Optional[str]
    api_key: Optional[str]
    ssl_verify: bool
    ca_file: Optional[str]
    ca_path: Optional[str]

    @classmethod
    def from_mapping(cls, data: Dict[str, object]) -> "ElasticsearchConfig":
        endpoint = str(data.get("endpoint") or "").strip()
        if not endpoint:
            raise ValueError("The Elasticsearch config must include an 'endpoint'.")

        headers = data.get("headers") or {}
        if not isinstance(headers, dict):
            raise ValueError("'headers' must be a mapping of string keys to values.")

        def optional_str(value: object) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        ssl_verify_value = data.get("ssl_verify", True)
        if isinstance(ssl_verify_value, bool):
            ssl_verify = ssl_verify_value
        elif isinstance(ssl_verify_value, str):
            lowered = ssl_verify_value.strip().lower()
            if lowered in {"false", "0", "no", "n"}:
                ssl_verify = False
            else:
                ssl_verify = True
        else:
            ssl_verify = True

        return cls(
            endpoint=endpoint,
            headers={str(k): str(v) for k, v in headers.items()},
            user=optional_str(data.get("user")),
            password=optional_str(data.get("password")),
            api_key=optional_str(data.get("api_key")),
            ssl_verify=ssl_verify,
            ca_file=optional_str(data.get("ca_file")),
            ca_path=optional_str(data.get("ca_path")),
        )


class ElasticsearchClient:
    def __init__(self, config: ElasticsearchConfig):
        parsed = urllib_parse.urlparse(config.endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid Elasticsearch endpoint: {config.endpoint}")

        self._base_url = config.endpoint.rstrip("/") + "/"
        self._session = requests.Session()
        self._session.headers.update(config.headers)
        header_keys = {k.lower() for k in self._session.headers.keys()}
        if config.api_key and "authorization" not in header_keys:
            self._session.headers["Authorization"] = f"ApiKey {config.api_key}"
        header_keys = {k.lower() for k in self._session.headers.keys()}
        if "content-type" not in header_keys:
            self._session.headers["Content-Type"] = "application/json"
        if config.user and config.password:
            self._session.auth = HTTPBasicAuth(config.user, config.password)

        if not config.ssl_verify:
            self._session.verify = False
        elif config.ca_file:
            self._session.verify = config.ca_file
        else:
            self._session.verify = True

        if config.ca_path:
            os.environ.setdefault("SSL_CERT_DIR", config.ca_path)
            os.environ.setdefault("REQUESTS_CA_BUNDLE", config.ca_path)

        self._timeout = 60

    def _resolve_url(self, path: str) -> str:
        if path.startswith(("http://", "https://")):
            return path
        return urllib_parse.urljoin(self._base_url, path.lstrip("/"))

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = self._resolve_url(path)
        timeout = kwargs.pop("timeout", self._timeout)
        return self._session.request(method.upper(), url, timeout=timeout, **kwargs)

    def head(self, path: str, **kwargs) -> requests.Response:
        return self.request("HEAD", path, **kwargs)

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs) -> requests.Response:
        return self.request("PUT", path, **kwargs)


def index_pdf(pdf_path, client: ElasticsearchClient, test_pipeline=False):
    """Index a single PDF file to Elasticsearch."""
    pdf_path = Path(pdf_path)
    filename = pdf_path.name
    airline = extract_airline_name(filename)

    print(f"\n{'='*60}")
    print(f"Processing: {filename}")
    print(f"Airline: {airline}")
    print(f"{'='*60}")

    try:
        # Read and encode the PDF
        with open(pdf_path, 'rb') as pdf_file:
            encoded_pdf = base64.b64encode(pdf_file.read()).decode('utf-8')

        file_size_mb = len(encoded_pdf) / 1024 / 1024 * 0.75  # Approximate original size
        print(f"File size: {file_size_mb:.2f} MB")

        # Test the pipeline if requested
        if test_pipeline:
            print("\nTesting pipeline...")
            test_payload = {
                "docs": [
                    {
                        "_source": {
                            "data": encoded_pdf,
                            "filename": filename,
                            "airline": airline
                        }
                    }
                ]
            }

            response = client.post(
                "_ingest/pipeline/pdf_pipeline/_simulate",
                json=test_payload
            )

            if response.status_code == 200:
                print("✅ Pipeline test passed")
            else:
                print(f"❌ Pipeline test failed: {response.status_code}")
                print(json.dumps(response.json(), indent=2))
                return False

        # Index the document
        print("Indexing document...")
        index_payload = {
            "data": encoded_pdf,
            "filename": filename,
            "airline": airline
        }

        index_response = client.post(
            f"{ES_INDEX}/_doc",
            params={"pipeline": PIPELINE_NAME},
            json=index_payload
        )

        if index_response.status_code in [200, 201]:
            result = index_response.json()
            print(f"✅ Successfully indexed: {filename}")
            print(f"   Document ID: {result.get('_id', 'N/A')}")
            print(f"   Airline: {airline}")
            return True
        else:
            print(f"❌ Indexing failed: {index_response.status_code}")
            print(json.dumps(index_response.json(), indent=2))
            return False

    except FileNotFoundError:
        print(f"❌ Error: File not found: {pdf_path}")
        return False
    except Exception as e:
        print(f"❌ Error processing {filename}: {str(e)}")
        return False


def load_mapping(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Mapping file must define a JSON object (found {type(data).__name__})")
    return data


def ensure_index_exists(client: ElasticsearchClient, mapping_path: Path, recreate: bool = False) -> bool:
    response = client.head(ES_INDEX)

    if response.status_code == 200:
        if not recreate:
            print(f"\n✅ Index '{ES_INDEX}' already exists")
            print("   Use --recreate-index to delete and recreate it.")
            return True
        print(f"\n🗑️  Recreating index '{ES_INDEX}'")
        delete_response = client.request("DELETE", ES_INDEX)
        if delete_response.status_code not in (200, 202):
            print(f"❌ Failed to delete index: {delete_response.status_code}")
            try:
                print(json.dumps(delete_response.json(), indent=2))
            except Exception:
                print(delete_response.text)
            return False

    elif response.status_code not in (404, 400):
        print(f"❌ Failed to check index: {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except Exception:
            print(response.text)
        return False
    else:
        print(f"\n📊 Creating index: {ES_INDEX}")

    mapping = load_mapping(mapping_path)
    try:
        semantic = mapping["mappings"]["properties"]["semantic_content"]
        semantic["inference_id"] = INFERENCE_ENDPOINT
    except KeyError:
        pass

    response = client.put(
        ES_INDEX,
        json=mapping
    )

    if response.status_code in (200, 201):
        try:
            result = response.json()
        except Exception:
            result = {}
        print("✅ Index created successfully")
        print(f"   - Acknowledged: {result.get('acknowledged', False)}")
        print(f"   - Shards acknowledged: {result.get('shards_acknowledged', False)}")
        return True
    else:
        print(f"❌ Failed to create index: {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except Exception:
            print(response.text)
        return False


def ensure_pipeline_exists(client: ElasticsearchClient) -> bool:
    print(f"\n📋 Ensuring ingest pipeline '{PIPELINE_NAME}' exists")
    print("=" * 60)

    pipeline_config = {
        "description": "Extract text from PDF and populate semantic_content for ELSER demos",
        "processors": [
            {
                "attachment": {
                    "field": "data",
                    "target_field": "attachment",
                    "remove_binary": True,
                }
            },
            {
                "set": {
                    "field": "semantic_content",
                    "copy_from": "attachment.content",
                    "ignore_empty_value": True,
                }
            },
            {
                "remove": {
                    "field": "data",
                    "ignore_missing": True,
                }
            },
            {
                "set": {
                    "field": "upload_date",
                    "value": "{{ _ingest.timestamp }}",
                }
            },
        ],
    }

    response = client.put(
        f"_ingest/pipeline/{PIPELINE_NAME}",
        json=pipeline_config,
    )

    if response.status_code in (200, 201):
        try:
            result = response.json()
        except Exception:
            result = {}
        acknowledged = result.get("acknowledged", False)
        print(f"✅ Pipeline '{PIPELINE_NAME}' ready (acknowledged: {acknowledged})")
        print(f"   Using ELSER model/endpoint: {ELSER_MODEL}")
        return True

    print(f"❌ Failed to create/update pipeline: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
    return False


def ensure_semantic_mapping(client: ElasticsearchClient) -> bool:
    try:
        response = client.get(f"{ES_INDEX}/_mapping")
    except Exception as exc:
        print(f"⚠️  Unable to retrieve current mappings: {exc}")
        return False

    if response.status_code != 200:
        print(f"⚠️  Failed to retrieve mappings: {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except Exception:
            print(response.text)
        return False

    try:
        data = response.json()
    except Exception as exc:
        print(f"⚠️  Could not parse mapping response: {exc}")
        return False

    index_info = data.get(ES_INDEX)
    if not index_info:
        print("⚠️  Index mapping not found in response.")
        return False

    properties = index_info.get("mappings", {}).get("properties", {})
    semantic = properties.get("semantic_content")
    if not isinstance(semantic, dict):
        print("⚠️  semantic_content mapping not found; skipping inference update.")
        return False

    current_inference = semantic.get("inference_id")
    if current_inference == INFERENCE_ENDPOINT:
        return True

    print(f"\n🔁 Updating semantic_content inference_id to '{INFERENCE_ENDPOINT}' (was '{current_inference}')")
    new_semantic = copy.deepcopy(semantic)
    new_semantic["inference_id"] = INFERENCE_ENDPOINT

    payload = {"properties": {"semantic_content": new_semantic}}

    update_response = client.put(f"{ES_INDEX}/_mapping", json=payload)
    if update_response.status_code in (200, 201):
        print("✅ semantic_content mapping updated successfully")
        return True

    print(f"⚠️  Failed to update semantic_content mapping: {update_response.status_code}")
    try:
        print(json.dumps(update_response.json(), indent=2))
    except Exception:
        print(update_response.text)
    return False


def check_inference_id(client: ElasticsearchClient, inference_id: str) -> Optional[dict]:
    try:
        response = client.get(f"_ml/trained_models/{inference_id}/_stats")
    except Exception:
        return None

    if response.status_code != 200:
        return None

    try:
        data = response.json()
    except Exception:
        return None

    stats = data.get("trained_model_stats") or []
    if not stats:
        return None

    model_info = stats[0]
    deployment = model_info.get("deployment_stats", {})
    return {
        "state": deployment.get("state"),
        "model_id": model_info.get("model_id") or inference_id,
        "inference_threads": deployment.get("inference_threads"),
        "queue": deployment.get("queue", {}).get("size"),
    }


def start_model_deployment(client: ElasticsearchClient, model_id: str) -> bool:
    print(f"⚙️  Starting ELSER model deployment '{model_id}'...")
    try:
        response = client.post(
            f"_ml/trained_models/{model_id}/deployment/_start",
            params={"wait_for": "started"},
        )
    except Exception as exc:
        print(f"❌ Failed to start model '{model_id}': {exc}")
        return False

    if response.status_code in (200, 202):
        print(f"✅ Model '{model_id}' started successfully")
        return True

    print(f"❌ Failed to start model '{model_id}': {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
    return False


def resolve_inference_id(client: ElasticsearchClient, preferred: str) -> str:
    candidates: List[str] = []
    if preferred:
        candidates.append(preferred)
    for fallback in (".elser_model_2", ".elser_model_2_linux-x86_64"):
        if fallback not in candidates:
            candidates.append(fallback)

    print("\n🔍 Checking available ELSER models")
    print("=" * 60)
    for candidate in candidates:
        info = check_inference_id(client, candidate)
        if info:
            state = (info.get("state") or "unknown").lower()
            print(f"ℹ️  Found model '{candidate}' (state: {state})")
            if state != "started":
                model_id = info.get("model_id") or candidate
                if not start_model_deployment(client, model_id):
                    continue
                info = check_inference_id(client, candidate)
                state = (info.get("state") or "unknown").lower() if info else "unknown"
            if state == "started":
                print(f"✅ Using model '{candidate}' (state: started)")
                return candidate
            print(f"❌ Model '{candidate}' is in state '{state}' after start attempt")
        else:
            print(f"❌ Model '{candidate}' not available")

    raise SystemExit(
        "No compatible ELSER model found. Set the ELSER_MODEL environment variable to a valid trained model ID."
    )


def create_data_view(client: ElasticsearchClient) -> bool:
    print(f"\n📋 Creating data view for: {ES_INDEX}")
    print("=" * 60)

    try:
        parsed = urllib_parse.urlparse(client._base_url)
        kibana_netloc = parsed.netloc.replace(".es.", ".kb.")
        if kibana_netloc == parsed.netloc:
            raise ValueError("Unable to derive Kibana endpoint from Elasticsearch endpoint.")
        kibana_base = urllib_parse.urlunparse(
            (parsed.scheme, kibana_netloc, "/", "", "", "")
        )
    except Exception as exc:
        print(f"⚠️  Could not derive Kibana endpoint automatically: {exc}")
        print("   Run `python create_dataview.py` to create the data view manually.")
        return False

    payload = {
        "data_view": {
            "title": ES_INDEX,
            "name": ES_INDEX.replace("-", " ").replace("_", " ").title(),
            "timeFieldName": "upload_date",
        }
    }

    headers = dict(client._session.headers)
    headers["kbn-xsrf"] = "true"

    url = urllib_parse.urljoin(kibana_base, "api/data_views/data_view")
    try:
        response = client._session.post(url, headers=headers, json=payload, timeout=30)
    except Exception as exc:
        print(f"⚠️  Could not create data view automatically: {exc}")
        print("   Run `python create_dataview.py` to create it manually.")
        return False

    if response.status_code in (200, 201):
        try:
            result = response.json()
        except Exception:
            result = {}
        data_view = result.get("data_view", {})
        print("✅ Data view created successfully")
        print(f"   ID: {data_view.get('id', 'N/A')}")
        print(f"   Title: {data_view.get('title', 'N/A')}")
        print(f"   Time Field: {data_view.get('timeFieldName', 'N/A')}")
        return True

    if response.status_code == 409:
        print(f"✅ Data view already exists (ID: {ES_INDEX})")
        return True

    print(f"⚠️  Could not create data view automatically: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
    print("   Run `python create_dataview.py` to create it manually.")
    return False


def main():
    parser = argparse.ArgumentParser(
        description='Index PDF files to Elasticsearch with ELSER embeddings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Index a single PDF file
  python index_pdf.py path/to/file.pdf

  # Index all PDFs in a folder
  python index_pdf.py path/to/folder/

  # Index with pipeline testing (slower but safer)
  python index_pdf.py path/to/folder/ --test
        """
    )

    parser.add_argument(
        '--config',
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to Elasticsearch config YAML (default: {DEFAULT_CONFIG_PATH})",
    )

    parser.add_argument(
        '--recreate-index',
        action='store_true',
        help='Delete and recreate the index before importing',
    )

    parser.add_argument(
        'path',
        nargs='?',
        default=str(DEFAULT_DATA_PATH),
        help=f'Path to PDF file or directory containing PDFs (default: {DEFAULT_DATA_PATH})',
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Test pipeline before indexing each document'
    )
    parser.add_argument(
        '--mapping',
        default=str(DEFAULT_MAPPING_PATH),
        help=f"Path to index mapping JSON (default: {DEFAULT_MAPPING_PATH})"
    )

    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        config_path = (PROJECT_ROOT / config_path).resolve()

    try:
        raw_config = load_yaml(config_path)
        es_config = ElasticsearchConfig.from_mapping(raw_config)
    except Exception as exc:
        print(f"\n❌ Failed to load Elasticsearch config: {exc}")
        sys.exit(1)

    try:
        client = ElasticsearchClient(es_config)
    except Exception as exc:
        print(f"\n❌ Failed to initialize Elasticsearch client: {exc}")
        sys.exit(1)

    resolved_inference_id = resolve_inference_id(client, INFERENCE_ENDPOINT)
    globals()["INFERENCE_ENDPOINT"] = resolved_inference_id
    globals()["ELSER_MODEL"] = resolved_inference_id

    if not ensure_pipeline_exists(client):
        sys.exit(1)

    mapping_path = Path(args.mapping).expanduser()
    if not mapping_path.is_absolute():
        mapping_path = (PROJECT_ROOT / mapping_path).resolve()

    # Ensure index exists with proper mapping
    try:
        index_ready = ensure_index_exists(client, mapping_path, recreate=args.recreate_index)
    except Exception as exc:
        print(f"\n❌ Failed to prepare index: {exc}")
        sys.exit(1)

    if not index_ready:
        print("\n❌ Failed to setup index. Exiting.")
        sys.exit(1)

    ensure_semantic_mapping(client)

    create_data_view(client)

    data_path = Path(args.path).expanduser()
    if not data_path.is_absolute():
        data_path = (PROJECT_ROOT / data_path).resolve()

    # Get list of PDF files
    pdf_files = get_pdf_files(data_path)

    if not pdf_files:
        print("\nNo PDF files to process. Exiting.")
        sys.exit(1)

    print(f"\n📊 Elasticsearch Index: {ES_INDEX}")
    print(f"Found {len(pdf_files)} PDF file(s) to process")

    # Process each PDF
    success_count = 0
    failed_count = 0

    for pdf_file in pdf_files:
        if index_pdf(pdf_file, client, test_pipeline=args.test):
            success_count += 1
        else:
            failed_count += 1

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total files: {len(pdf_files)}")
    print(f"✅ Successfully indexed: {success_count}")
    if failed_count > 0:
        print(f"❌ Failed: {failed_count}")
    print(f"{'='*60}\n")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
