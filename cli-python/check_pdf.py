#!/usr/bin/env python3
"""
Utility to check PDF characteristics and compatibility with the indexing pipeline.
"""

import sys
import argparse
from pathlib import Path
import PyPDF2


def check_pdf(pdf_path):
    """Check PDF file characteristics."""
    pdf_path = Path(pdf_path)

    if not pdf_path.exists():
        print(f"❌ Error: File not found: {pdf_path}")
        return False

    if pdf_path.suffix.lower() != '.pdf':
        print(f"❌ Error: Not a PDF file: {pdf_path}")
        return False

    print(f"\n{'='*60}")
    print(f"Checking PDF: {pdf_path.name}")
    print(f"{'='*60}\n")

    try:
        with open(pdf_path, 'rb') as file:
            # Try to read the PDF
            try:
                reader = PyPDF2.PdfReader(file)
            except Exception as e:
                print(f"❌ Cannot read PDF: {str(e)}")
                return False

            # Basic info
            num_pages = len(reader.pages)
            print(f"📄 Number of pages: {num_pages}")

            # Check if encrypted
            if reader.is_encrypted:
                print("🔒 Status: ENCRYPTED/PASSWORD-PROTECTED")
                print("❌ This PDF cannot be processed without decryption")
                return False
            else:
                print("🔓 Status: Not encrypted")

            # Get metadata
            metadata = reader.metadata
            if metadata:
                print("\n📋 Metadata:")
                if metadata.author:
                    print(f"   Author: {metadata.author}")
                if metadata.creator:
                    print(f"   Creator: {metadata.creator}")
                if metadata.producer:
                    print(f"   Producer: {metadata.producer}")
                if metadata.title:
                    print(f"   Title: {metadata.title}")

            # Try to extract text from first page
            print("\n📝 Text extraction test:")
            try:
                first_page = reader.pages[0]
                text = first_page.extract_text()

                if text and len(text.strip()) > 0:
                    word_count = len(text.split())
                    print(f"✅ Successfully extracted {word_count} words from first page")
                    print(f"\n   Sample (first 200 chars):")
                    print(f"   {text[:200].strip()}...")

                    # Estimate if it's text-based or scanned
                    if word_count < 10 and num_pages > 1:
                        print("\n⚠️  WARNING: Very little text extracted")
                        print("   This might be a scanned/image-based PDF")
                        print("   Text extraction may be poor or empty")
                else:
                    print("❌ No text extracted from first page")
                    print("   This is likely a scanned/image-based PDF")
                    print("   The attachment processor will not extract meaningful text")
                    return False

            except Exception as e:
                print(f"❌ Text extraction failed: {str(e)}")
                return False

            # File size check
            file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
            print(f"\n📦 File size: {file_size_mb:.2f} MB")

            if file_size_mb > 10:
                print("⚠️  WARNING: Large file (>10MB)")
                print("   Processing may be slow or fail")
            elif file_size_mb > 50:
                print("❌ File too large (>50MB)")
                print("   Likely to fail or timeout")
                return False

            print("\n" + "="*60)
            print("✅ PDF appears compatible with the indexing pipeline")
            print("="*60 + "\n")
            return True

    except Exception as e:
        print(f"❌ Error checking PDF: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Check PDF compatibility with Elasticsearch indexing pipeline'
    )
    parser.add_argument('pdf_path', help='Path to PDF file to check')

    args = parser.parse_args()

    result = check_pdf(args.pdf_path)
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
