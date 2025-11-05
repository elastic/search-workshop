#!/usr/bin/env python3
"""
Example semantic search queries for airline contract documents.
Demonstrates various query patterns using the inference endpoint.
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
INFERENCE_ENDPOINT = os.getenv('INFERENCE_ENDPOINT')

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}


def semantic_search(query_text, size=3, show_snippets=True):
    """Perform semantic search and display results."""
    print(f"\n{'='*60}")
    print(f"🔍 Query: \"{query_text}\"")
    print('='*60)

    search_query = {
        'size': size,
        'query': {
            'semantic': {
                'field': 'semantic_content',
                'query': query_text
            }
        },
        '_source': ['filename', 'airline', 'attachment.title', 'attachment.author']
    }

    # Add highlighting if requested - semantic_content field shows matched chunks
    if show_snippets:
        search_query['highlight'] = {
            'fields': {
                'semantic_content': {
                    'fragment_size': 300,
                    'number_of_fragments': 2
                }
            }
        }

    response = requests.post(
        f"{ES_ENDPOINT}/{ES_INDEX}/_search",
        headers=headers,
        json=search_query
    )

    if response.status_code == 200:
        result = response.json()
        hits = result['hits']['hits']

        print(f"\nFound {result['hits']['total']['value']} documents:")

        for i, hit in enumerate(hits, 1):
            airline = hit['_source'].get('airline', 'Unknown')
            title = hit['_source'].get('attachment', {}).get('title', 'N/A')
            score = hit['_score']

            print(f"\n{i}. {airline}")
            print(f"   Title: {title}")
            print(f"   Relevance Score: {score:.2f}")

            # Show highlighted snippets - these are the actual matched chunks
            if show_snippets and 'highlight' in hit:
                snippets = hit['highlight'].get('semantic_content', [])
                if snippets:
                    print(f"   📌 Matched Section(s):")
                    for j, snippet in enumerate(snippets[:2], 1):
                        # Clean up snippet - preserve some structure but make readable
                        snippet = snippet.replace('\n\n', ' ¶ ').replace('\n', ' ').strip()
                        # Truncate if very long
                        if len(snippet) > 400:
                            snippet = snippet[:400] + '...'
                        print(f"   {j}. {snippet}")
    else:
        print(f"❌ Search failed: {response.status_code}")
        print(json.dumps(response.json(), indent=2))


def run_examples():
    """Run various example queries."""

    print("\n" + "="*60)
    print("SEMANTIC SEARCH EXAMPLES")
    print(f"Index: {ES_INDEX}")
    print(f"Inference Endpoint: {INFERENCE_ENDPOINT}")
    print("="*60)

    # Category 1: Baggage and Luggage
    print("\n\n" + "="*60)
    print("CATEGORY: Baggage & Luggage")
    print("="*60)

    semantic_search("What are the baggage fees for checked luggage?")
    semantic_search("How many carry-on bags can I bring?")
    semantic_search("What items are prohibited in checked baggage?")
    semantic_search("Oversized baggage charges")

    # Category 2: Children and Minors
    print("\n\n" + "="*60)
    print("CATEGORY: Children & Minors")
    print("="*60)

    semantic_search("Can my child fly without an adult?")
    semantic_search("What is the unaccompanied minor policy?")
    semantic_search("How much does it cost for kids to fly alone?")
    semantic_search("What age can children travel by themselves?")

    # Category 3: Cancellations and Refunds
    print("\n\n" + "="*60)
    print("CATEGORY: Cancellations & Refunds")
    print("="*60)

    semantic_search("What if my flight is cancelled?")
    semantic_search("How do I get a refund for my ticket?")
    semantic_search("Can I change my flight for free?")
    semantic_search("What compensation do I get for delays?")

    # Category 4: Special Passengers
    print("\n\n" + "="*60)
    print("CATEGORY: Special Passengers")
    print("="*60)

    semantic_search("What assistance is available for disabled passengers?")
    semantic_search("Can I bring a service animal on the plane?")
    semantic_search("Wheelchair accessibility on flights")
    semantic_search("Do pregnant women need special documentation to fly?")

    # Category 5: Booking and Tickets
    print("\n\n" + "="*60)
    print("CATEGORY: Booking & Tickets")
    print("="*60)

    semantic_search("How do I confirm my reservation?")
    semantic_search("Can I fly standby on a different flight?")
    semantic_search("What forms of payment are accepted?")
    semantic_search("Do I need to print my boarding pass?")

    # Category 6: Denied Boarding
    print("\n\n" + "="*60)
    print("CATEGORY: Denied Boarding & Overbooking")
    print("="*60)

    semantic_search("What happens if I'm bumped from my flight?")
    semantic_search("Compensation for involuntary denied boarding")
    semantic_search("What if the airline overbooks my flight?")

    # Category 7: International Travel
    print("\n\n" + "="*60)
    print("CATEGORY: International Travel")
    print("="*60)

    semantic_search("What documents do I need for international flights?")
    semantic_search("Customs and immigration requirements")
    semantic_search("Can I transit through another country?")

    # Category 8: Natural Language Questions
    print("\n\n" + "="*60)
    print("CATEGORY: Natural Language Questions")
    print("="*60)

    semantic_search("I need to bring my emotional support dog, is that allowed?")
    semantic_search("My elderly parent needs help getting on the plane")
    semantic_search("What happens if I miss my connecting flight due to a delay?")
    semantic_search("Can I get my money back if I'm too sick to travel?")

    print("\n\n" + "="*60)
    print("✅ Example queries complete!")
    print("="*60)
    print("\nThese examples demonstrate:")
    print("  - Natural language queries")
    print("  - Concept-based search (not just keyword matching)")
    print("  - Understanding of synonyms and related terms")
    print("  - Context-aware results")
    print()


def compare_semantic_vs_keyword(query_text):
    """Compare semantic search vs traditional keyword search."""

    print("\n" + "="*60)
    print(f"COMPARISON: Semantic vs Keyword Search")
    print(f"Query: \"{query_text}\"")
    print("="*60)

    # Semantic search
    print("\n📊 SEMANTIC SEARCH (using ELSER embeddings):")
    print("-"*60)

    semantic_query = {
        'size': 3,
        'query': {
            'semantic': {
                'field': 'semantic_content',
                'query': query_text
            }
        },
        '_source': ['filename', 'airline']
    }

    response = requests.post(
        f"{ES_ENDPOINT}/{ES_INDEX}/_search",
        headers=headers,
        json=semantic_query
    )

    if response.status_code == 200:
        result = response.json()
        for i, hit in enumerate(result['hits']['hits'], 1):
            airline = hit['_source'].get('airline', 'Unknown')
            print(f"  {i}. {airline} (score: {hit['_score']:.2f})")

    # Keyword search
    print("\n📝 KEYWORD SEARCH (traditional full-text):")
    print("-"*60)

    keyword_query = {
        'size': 3,
        'query': {
            'match': {
                'attachment.content': query_text
            }
        },
        '_source': ['filename', 'airline']
    }

    response = requests.post(
        f"{ES_ENDPOINT}/{ES_INDEX}/_search",
        headers=headers,
        json=keyword_query
    )

    if response.status_code == 200:
        result = response.json()
        for i, hit in enumerate(result['hits']['hits'], 1):
            airline = hit['_source'].get('airline', 'Unknown')
            print(f"  {i}. {airline} (score: {hit['_score']:.2f})")

    print("\n" + "="*60)
    print("Key Differences:")
    print("  - Semantic: Understands meaning and context")
    print("  - Keyword: Matches exact words/phrases")
    print("  - Semantic: Better for natural language questions")
    print("  - Keyword: Better for specific term searches")
    print("="*60)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Run example semantic search queries'
    )
    parser.add_argument(
        '--query',
        type=str,
        help='Run a single custom query'
    )
    parser.add_argument(
        '--compare',
        action='store_true',
        help='Compare semantic vs keyword search'
    )
    parser.add_argument(
        '--size',
        type=int,
        default=3,
        help='Number of results to return (default: 3)'
    )

    args = parser.parse_args()

    if args.query:
        semantic_search(args.query, size=args.size)

        if args.compare:
            compare_semantic_vs_keyword(args.query)
    else:
        # Run all examples
        run_examples()


if __name__ == "__main__":
    main()
