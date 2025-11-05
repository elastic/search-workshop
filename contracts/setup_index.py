#!/usr/bin/env python3
"""
Setup Elasticsearch index with proper mappings and data view.
Run this before indexing PDFs to ensure proper configuration.
"""

import os
import sys
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ES_ENDPOINT = os.getenv('ES_ENDPOINT')
API_KEY = os.getenv('ES_API_KEY')
ES_INDEX = os.getenv('ES_INDEX', 'pdf-docs-2025')
INFERENCE_ENDPOINT = os.getenv('INFERENCE_ENDPOINT', '.elser-2-elastic')

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}


def check_index_exists():
    """Check if the index already exists."""
    response = requests.head(
        f"{ES_ENDPOINT}/{ES_INDEX}",
        headers=headers
    )
    return response.status_code == 200


def create_index():
    """Create index with proper mappings for PDF content and ELSER embeddings."""
    print(f"\n📊 Creating index: {ES_INDEX}")
    print("="*60)

    mapping = {
        'mappings': {
            'properties': {
                'filename': {
                    'type': 'keyword'
                },
                'upload_date': {
                    'type': 'date'
                },
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
                        'title': {
                            'type': 'text'
                        },
                        'author': {
                            'type': 'keyword'
                        },
                        'date': {
                            'type': 'date'
                        },
                        'content_type': {
                            'type': 'keyword'
                        },
                        'content_length': {
                            'type': 'long'
                        },
                        'language': {
                            'type': 'keyword'
                        },
                        'keywords': {
                            'type': 'text'
                        },
                        'creator_tool': {
                            'type': 'keyword'
                        }
                    }
                },
                'airline': {
                    'type': 'keyword'
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
        result = response.json()
        print(f"✅ Index created successfully")
        print(f"   - Acknowledged: {result.get('acknowledged', False)}")
        print(f"   - Shards acknowledged: {result.get('shards_acknowledged', False)}")
        return True
    else:
        print(f"❌ Failed to create index: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        return False


def create_data_view():
    """Create a persistent data view (index pattern) for the index."""
    print(f"\n📋 Creating data view for: {ES_INDEX}")
    print("="*60)

    # Get Kibana endpoint - for serverless, replace .es. with .kb.
    kibana_endpoint = ES_ENDPOINT.replace('.es.', '.kb.')

    # Data view payload
    data_view_payload = {
        "data_view": {
            "title": ES_INDEX,
            "name": ES_INDEX.replace('-', ' ').replace('_', ' ').title(),
            "timeFieldName": "upload_date"
        }
    }

    kibana_headers = {
        **headers,
        "kbn-xsrf": "true"  # Required for Kibana API
    }

    try:
        response = requests.post(
            f"{kibana_endpoint}/api/data_views/data_view",
            headers=kibana_headers,
            json=data_view_payload,
            timeout=30
        )

        if response.status_code in [200, 201]:
            result = response.json()
            data_view = result.get('data_view', {})
            print(f"✅ Data view created successfully")
            print(f"   ID: {data_view.get('id', 'N/A')}")
            print(f"   Title: {data_view.get('title', 'N/A')}")
            print(f"   Time Field: {data_view.get('timeFieldName', 'N/A')}")
            return True

        elif response.status_code == 409:
            print(f"✅ Data view already exists (ID: {ES_INDEX})")
            return True

        else:
            print(f"⚠️  Could not create data view automatically: {response.status_code}")
            print(f"\n   To create manually:")
            print(f"   Run: python create_dataview.py")
            return False

    except Exception as e:
        print(f"⚠️  Could not create data view: {str(e)}")
        print(f"\n   To create manually:")
        print(f"   Run: python create_dataview.py")
        return False


def delete_index():
    """Delete the index if it exists."""
    if not check_index_exists():
        print(f"Index '{ES_INDEX}' does not exist")
        return True

    print(f"\n🗑️  Deleting index: {ES_INDEX}")
    response = requests.delete(
        f"{ES_ENDPOINT}/{ES_INDEX}",
        headers=headers
    )

    if response.status_code == 200:
        print(f"✅ Index deleted successfully")
        return True
    else:
        print(f"❌ Failed to delete index: {response.status_code}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Setup Elasticsearch index and data view for PDF indexing'
    )
    parser.add_argument(
        '--recreate',
        action='store_true',
        help='Delete and recreate the index if it exists'
    )
    parser.add_argument(
        '--delete-only',
        action='store_true',
        help='Only delete the index, do not recreate'
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print(f"Elasticsearch Index Setup")
    print("="*60)
    print(f"Index: {ES_INDEX}")
    print(f"Endpoint: {ES_ENDPOINT}")
    print("="*60)

    # Check if index exists
    exists = check_index_exists()

    if args.delete_only:
        if exists:
            delete_index()
        else:
            print(f"\nIndex '{ES_INDEX}' does not exist")
        sys.exit(0)

    if exists and not args.recreate:
        print(f"\n✅ Index '{ES_INDEX}' already exists")
        print(f"   Use --recreate to delete and recreate it")

        # Still try to create data view
        create_data_view()
        sys.exit(0)

    if exists and args.recreate:
        if not delete_index():
            sys.exit(1)

    # Create index
    if not create_index():
        sys.exit(1)

    # Create data view
    create_data_view()

    print("\n" + "="*60)
    print("✅ Setup complete!")
    print("="*60)
    print(f"\nYou can now index PDFs using:")
    print(f"  python index_pdf.py <path-to-pdfs>")
    print("\n")


if __name__ == "__main__":
    main()
