#!/usr/bin/env python3
"""
Inspect the chunks created for indexed documents.
Shows how semantic_text field breaks up documents.
"""

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ES_ENDPOINT = os.getenv('ES_ENDPOINT')
API_KEY = os.getenv('ES_API_KEY')
ES_INDEX = os.getenv('ES_INDEX')

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}


def list_documents():
    """List all documents in the index."""
    print(f"\n{'='*60}")
    print(f"Documents in index: {ES_INDEX}")
    print('='*60)

    response = requests.get(
        f"{ES_ENDPOINT}/{ES_INDEX}/_search",
        headers=headers,
        json={
            "size": 100,
            "_source": ["filename", "airline"]
        }
    )

    if response.status_code == 200:
        result = response.json()
        hits = result['hits']['hits']

        if not hits:
            print("\n⚠️  No documents found in index")
            return []

        print(f"\nFound {len(hits)} document(s):\n")
        documents = []

        for i, hit in enumerate(hits, 1):
            doc_id = hit['_id']
            filename = hit['_source'].get('filename', 'N/A')
            airline = hit['_source'].get('airline', 'Unknown')

            print(f"{i}. {airline}")
            print(f"   Filename: {filename}")
            print(f"   Document ID: {doc_id}")
            print()

            documents.append({
                'id': doc_id,
                'filename': filename,
                'airline': airline
            })

        return documents
    else:
        print(f"❌ Failed to list documents: {response.status_code}")
        return []


def inspect_document_chunks(doc_id):
    """Inspect the chunks for a specific document."""
    print(f"\n{'='*60}")
    print(f"Inspecting chunks for document: {doc_id}")
    print('='*60)

    # Get the document with semantic_text field details
    response = requests.get(
        f"{ES_ENDPOINT}/{ES_INDEX}/_doc/{doc_id}",
        headers=headers
    )

    if response.status_code != 200:
        print(f"❌ Failed to get document: {response.status_code}")
        return

    result = response.json()
    source = result.get('_source', {})

    filename = source.get('filename', 'N/A')
    airline = source.get('airline', 'Unknown')

    print(f"\n📄 Document: {airline} - {filename}")
    print()

    # Check if semantic_content exists
    semantic_content = source.get('semantic_content')

    if not semantic_content:
        print("⚠️  No semantic_content field found")
        print("   The document may not have been processed for semantic search")
        return

    # semantic_text fields can store either:
    # 1. The text directly (for short content)
    # 2. An object with chunking metadata
    if isinstance(semantic_content, str):
        print(f"📝 Content Length: {len(semantic_content)} characters")
        print(f"   Word Count: {len(semantic_content.split())} words")
        print(f"\n💡 Content appears to be stored as single text (no visible chunks)")
        print(f"   Elasticsearch may have chunked internally for embedding")
    elif isinstance(semantic_content, dict):
        # Check for inference results
        if 'inference' in semantic_content:
            inference = semantic_content['inference']
            print(f"📊 Inference Results:")
            print(f"   Chunks: {len(inference.get('chunks', []))}")

            for i, chunk in enumerate(inference.get('chunks', []), 1):
                print(f"\n  Chunk {i}:")
                chunk_text = chunk.get('text', '')
                word_count = len(chunk_text.split())
                print(f"    Words: {word_count}")
                print(f"    Preview: {chunk_text[:150]}...")
        else:
            print("📝 semantic_content structure:")
            print(json.dumps(semantic_content, indent=2)[:500])

    # Get the total document size
    attachment = source.get('attachment', {})
    content = attachment.get('content', '')
    if content:
        print(f"\n📏 Original Document:")
        print(f"   Total Characters: {len(content):,}")
        print(f"   Total Words: {len(content.split()):,}")
        print(f"   Estimated Pages: {len(content) // 3000}")  # ~3000 chars per page


def get_chunking_stats():
    """Get statistics about chunking across all documents."""
    print(f"\n{'='*60}")
    print(f"Chunking Statistics")
    print('='*60)

    response = requests.get(
        f"{ES_ENDPOINT}/{ES_INDEX}/_search",
        headers=headers,
        json={
            "size": 100,
            "_source": ["filename", "airline", "attachment.content", "semantic_content"]
        }
    )

    if response.status_code != 200:
        print(f"❌ Failed to get documents: {response.status_code}")
        return

    result = response.json()
    hits = result['hits']['hits']

    total_docs = len(hits)
    total_words = 0
    total_chunks = 0

    print(f"\n📊 Overall Statistics:")
    print(f"   Total Documents: {total_docs}\n")

    for hit in hits:
        source = hit['_source']
        airline = source.get('airline', 'Unknown')
        content = source.get('attachment', {}).get('content', '')
        word_count = len(content.split())
        total_words += word_count

        # Try to estimate chunks (250 words each)
        estimated_chunks = max(1, word_count // 250)
        total_chunks += estimated_chunks

        print(f"   {airline}:")
        print(f"     Words: {word_count:,}")
        print(f"     Est. Chunks: ~{estimated_chunks}")

    print(f"\n   Average words/doc: {total_words // total_docs:,}")
    print(f"   Estimated total chunks: ~{total_chunks}")
    print(f"   Average chunks/doc: ~{total_chunks // total_docs}")

    print(f"\n💡 Note: Actual chunk count may vary based on sentence boundaries")
    print(f"   and the inference endpoint's chunking strategy.")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Inspect document chunks created by semantic_text field'
    )
    parser.add_argument(
        '--doc-id',
        type=str,
        help='Inspect specific document by ID'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show chunking statistics for all documents'
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print("DOCUMENT CHUNKS INSPECTOR")
    print("="*60)
    print(f"Index: {ES_INDEX}")
    print(f"Endpoint: {ES_ENDPOINT}")
    print("="*60)

    if args.stats:
        get_chunking_stats()
        return

    if args.doc_id:
        inspect_document_chunks(args.doc_id)
    else:
        # List all documents and let user choose
        documents = list_documents()

        if not documents:
            return

        print("="*60)
        print("\nTo inspect a specific document:")
        print(f"  python3 inspect_chunks.py --doc-id <document-id>")
        print("\nTo see chunking statistics:")
        print(f"  python3 inspect_chunks.py --stats")
        print()


if __name__ == "__main__":
    main()
