import os
import sys
import argparse
import requests
import base64
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Elasticsearch configuration from environment variables
ES_ENDPOINT = os.getenv('ES_ENDPOINT')
API_KEY = os.getenv('ES_API_KEY')
ES_INDEX = os.getenv('ES_INDEX', 'pdf-docs-2025')  # Default to 'pdf-docs-2025' if not set
INFERENCE_ENDPOINT = os.getenv('INFERENCE_ENDPOINT', '.elser-2-elastic')

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}


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


def index_pdf(pdf_path, test_pipeline=False):
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

            response = requests.post(
                f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline/_simulate",
                headers=headers,
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

        index_response = requests.post(
            f"{ES_ENDPOINT}/{ES_INDEX}/_doc?pipeline=pdf_pipeline",
            headers=headers,
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


def ensure_index_exists():
    """Ensure the index exists with proper mapping. Create it if it doesn't."""
    # Check if index exists
    response = requests.head(f"{ES_ENDPOINT}/{ES_INDEX}", headers=headers)

    if response.status_code == 200:
        return True

    # Index doesn't exist, create it
    print(f"⚠️  Index '{ES_INDEX}' does not exist. Creating it...")

    mapping = {
        'mappings': {
            'properties': {
                'filename': {'type': 'keyword'},
                'airline': {'type': 'keyword'},
                'upload_date': {'type': 'date'},
                'attachment': {
                    'properties': {
                        'content': {'type': 'text'},
                        'title': {'type': 'text'},
                        'author': {'type': 'keyword'},
                        'date': {'type': 'date'},
                        'content_type': {'type': 'keyword'},
                        'content_length': {'type': 'long'},
                        'language': {'type': 'keyword'}
                    }
                },
                'semantic_content': {
                    'type': 'semantic_text',
                    'inference_id': INFERENCE_ENDPOINT
                }
            }
        }
    }

    response = requests.put(
        f"{ES_ENDPOINT}/{ES_INDEX}",
        headers=headers,
        json=mapping
    )

    if response.status_code == 200:
        print(f"✅ Index '{ES_INDEX}' created successfully")
        return True
    else:
        print(f"❌ Failed to create index: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
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
        'path',
        nargs='?',
        default='airline_contracts',
        help='Path to PDF file or directory containing PDFs (default: airline_contracts)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Test pipeline before indexing each document'
    )

    args = parser.parse_args()

    # Ensure index exists with proper mapping
    if not ensure_index_exists():
        print("\n❌ Failed to setup index. Exiting.")
        sys.exit(1)

    # Get list of PDF files
    pdf_files = get_pdf_files(args.path)

    if not pdf_files:
        print("\nNo PDF files to process. Exiting.")
        sys.exit(1)

    print(f"\n📊 Elasticsearch Index: {ES_INDEX}")
    print(f"Found {len(pdf_files)} PDF file(s) to process")

    # Process each PDF
    success_count = 0
    failed_count = 0

    for pdf_file in pdf_files:
        if index_pdf(pdf_file, test_pipeline=args.test):
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