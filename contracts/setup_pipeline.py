#!/usr/bin/env python3
"""
Setup or update the PDF processing pipeline with ELSER embeddings.
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
ELSER_MODEL = os.getenv('ELSER_MODEL', '.elser_model_2_linux-x86_64')

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}


def check_elser_model():
    """Check if the ELSER model is deployed."""
    print(f"\n🔍 Checking ELSER model: {ELSER_MODEL}")
    print("="*60)

    response = requests.get(
        f"{ES_ENDPOINT}/_ml/trained_models/{ELSER_MODEL}/_stats",
        headers=headers
    )

    if response.status_code == 200:
        result = response.json()
        stats = result.get('trained_model_stats', [])

        if stats:
            model = stats[0]
            deployment = model.get('deployment_stats', {})
            state = deployment.get('state', 'not deployed')

            print(f"✅ Model found: {ELSER_MODEL}")
            print(f"   State: {state}")

            if state == 'started':
                print(f"   Status: Ready for inference")
                return True
            else:
                print(f"   ⚠️  Model not started")
                print(f"   You may need to start it in Kibana")
                return False
        else:
            print(f"❌ Model not found: {ELSER_MODEL}")
            return False
    else:
        print(f"❌ Could not check model status: {response.status_code}")
        return False


def create_or_update_pipeline():
    """Create or update the PDF processing pipeline."""
    print(f"\n📋 Creating/Updating pipeline: pdf_pipeline")
    print("="*60)

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

    response = requests.put(
        f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline",
        headers=headers,
        json=pipeline_config
    )

    if response.status_code == 200:
        result = response.json()
        print(f"✅ Pipeline created/updated successfully")
        print(f"   Acknowledged: {result.get('acknowledged', False)}")
        print(f"   Using ELSER model: {ELSER_MODEL}")
        return True
    else:
        print(f"❌ Failed to create/update pipeline: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        return False


def test_pipeline():
    """Test the pipeline with sample text."""
    print(f"\n🧪 Testing pipeline")
    print("="*60)

    test_doc = {
        'docs': [
            {
                '_source': {
                    'data': 'VGVzdCBQREYgY29udGVudCBmb3IgRUxTRVIgdGVzdGluZw==',  # Base64 encoded "Test PDF content"
                    'filename': 'test.pdf'
                }
            }
        ]
    }

    response = requests.post(
        f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline/_simulate",
        headers=headers,
        json=test_doc
    )

    if response.status_code == 200:
        result = response.json()
        doc = result.get('docs', [{}])[0]
        doc_source = doc.get('doc', {}).get('_source', {})

        if 'content_embedding' in doc_source:
            num_tokens = len(doc_source['content_embedding'])
            print(f"✅ Pipeline test passed")
            print(f"   Generated {num_tokens} embedding tokens")
            return True
        else:
            print(f"❌ Pipeline test failed: no content_embedding generated")
            print(json.dumps(result, indent=2))
            return False
    else:
        print(f"❌ Pipeline test failed: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        return False


def get_pipeline_info():
    """Get current pipeline configuration."""
    print(f"\n📋 Current pipeline configuration")
    print("="*60)

    response = requests.get(
        f"{ES_ENDPOINT}/_ingest/pipeline/pdf_pipeline",
        headers=headers
    )

    if response.status_code == 200:
        result = response.json()
        pipeline = result.get('pdf_pipeline', {})

        # Find the inference processor
        processors = pipeline.get('processors', [])
        for proc in processors:
            if 'inference' in proc:
                model_id = proc['inference'].get('model_id', 'N/A')
                print(f"Current ELSER model: {model_id}")

                if model_id == ELSER_MODEL:
                    print(f"✅ Pipeline is using the correct model")
                else:
                    print(f"⚠️  Pipeline is using a different model")
                    print(f"   Expected: {ELSER_MODEL}")
                    print(f"   Actual: {model_id}")
                break

        return True
    elif response.status_code == 404:
        print(f"⚠️  Pipeline does not exist")
        return False
    else:
        print(f"❌ Could not get pipeline info: {response.status_code}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Setup or update PDF processing pipeline with ELSER'
    )
    parser.add_argument(
        '--info',
        action='store_true',
        help='Show current pipeline configuration'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test the pipeline after creating/updating'
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print(f"PDF Processing Pipeline Setup")
    print("="*60)
    print(f"Endpoint: {ES_ENDPOINT}")
    print(f"ELSER Model: {ELSER_MODEL}")
    print("="*60)

    if args.info:
        get_pipeline_info()
        sys.exit(0)

    # Note: No need to check ELSER model anymore since semantic_text field
    # handles inference automatically via the inference endpoint
    print("\n💡 Using semantic_text field - inference handled automatically")

    # Create/update pipeline
    if not create_or_update_pipeline():
        sys.exit(1)

    # Test pipeline if requested
    if args.test:
        if not test_pipeline():
            sys.exit(1)

    print("\n" + "="*60)
    print("✅ Setup complete!")
    print("="*60)
    print(f"\nYou can now index PDFs using:")
    print(f"  python index_pdf.py <path-to-pdfs>")
    print("\n")


if __name__ == "__main__":
    main()
