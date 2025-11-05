#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Count rows in each flight data file and print totals."""

import argparse
import csv
import io
import re
import zipfile
from pathlib import Path


def extract_year_from_filename(filename: str) -> int | None:
    """Extract year from filename pattern like ..._YYYY_M.zip
    
    Args:
        filename: The filename to parse
        
    Returns:
        Year as integer, or None if no year found
    """
    # Pattern matches _YYYY_ followed by a digit (month) at the end
    # This avoids matching "1987" in "1987_present"
    match = re.search(r'_(\d{4})_\d+\.(zip|csv)$', filename)
    if match:
        return int(match.group(1))
    return None


def count_rows_in_file(file_path: Path) -> int:
    """Count the number of data rows in a CSV or ZIP file containing CSV.
    
    Args:
        file_path: Path to the file (can be .zip or .csv)
        
    Returns:
        Number of data rows (excluding header)
    """
    if file_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(file_path) as archive:
            # Find the CSV file inside the archive
            entry_name = next(
                (name for name in archive.namelist() if name.lower().endswith(".csv")), None
            )
            if entry_name is None:
                print(f"Warning: No CSV entry found in {file_path.name}")
                return 0
            
            with archive.open(entry_name, "r") as entry:
                with io.TextIOWrapper(entry, encoding="utf-8") as text_stream:
                    reader = csv.reader(text_stream)
                    # Skip header row
                    next(reader, None)
                    # Count remaining rows
                    return sum(1 for _ in reader)
    else:
        # Handle plain CSV files
        with file_path.open("r", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            # Skip header row
            next(reader, None)
            # Count remaining rows
            return sum(1 for _ in reader)


def main():
    """Count rows in all flight data files."""
    parser = argparse.ArgumentParser(
        description="Count rows in flight data files, optionally filtered by year"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Filter files by year (e.g., 2020, 2021)",
    )
    args = parser.parse_args()
    
    flights_dir = Path(__file__).parent.parent.parent / "data" / "flights"
    
    if not flights_dir.exists():
        print(f"Error: Directory not found: {flights_dir}")
        return
    
    # Find all .zip and .csv files
    all_files = sorted(flights_dir.glob("*.zip")) + sorted(flights_dir.glob("*.csv"))
    
    # Filter by year if specified
    if args.year:
        files = [
            f for f in all_files
            if extract_year_from_filename(f.name) == args.year
        ]
        if not files:
            print(f"No files found for year {args.year}")
            return
        print(f"Filtering by year {args.year}...")
    else:
        files = all_files
    
    if not files:
        print(f"No .zip or .csv files found in {flights_dir}")
        return
    
    print(f"Counting rows in {len(files)} file(s)...\n")
    
    total_rows = 0
    file_counts = []
    
    for file_path in files:
        try:
            count = count_rows_in_file(file_path)
            file_counts.append((file_path.name, count))
            total_rows += count
            print(f"{file_path.name}: {count:,} rows")
        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
            file_counts.append((file_path.name, 0))
    
    print(f"\n{'=' * 60}")
    if args.year:
        print(f"Total for year {args.year}: {total_rows:,} rows across {len(files)} file(s)")
    else:
        print(f"Total: {total_rows:,} rows across {len(files)} file(s)")


if __name__ == "__main__":
    main()
