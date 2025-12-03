#!/usr/bin/env python3
"""
Script to deduplicate URLs in CSV files by normalizing trailing slashes.
URLs that differ only by a trailing slash are considered duplicates.
"""

import csv
import sys
from collections import defaultdict


def normalize_url(url):
    """Normalize URL by removing trailing slash."""
    return url.rstrip('/')


def deduplicate_clarity_data(input_file, output_file):
    """
    Deduplicate ClarityData.csv by keeping only normalized URLs.
    For duplicates, keep the URL without trailing slash.
    """
    urls_seen = set()
    deduplicated_count = 0
    total_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write header
        header = next(reader)
        writer.writerow(header)
        
        # Process each URL
        for row in reader:
            if row:
                total_count += 1
                url = row[0]
                normalized = normalize_url(url)
                
                if normalized not in urls_seen:
                    urls_seen.add(normalized)
                    # Write the normalized URL (without trailing slash)
                    writer.writerow([normalized])
                else:
                    deduplicated_count += 1
    
    return total_count, deduplicated_count


def deduplicate_export_data(input_file, output_file):
    """
    Deduplicate export.csv by normalizing URLs and aggregating user counts.
    For duplicate URLs, sum the user counts and keep the LandingPageFlag if any is set.
    """
    url_data = defaultdict(lambda: {'landing_flag': '', 'user_count': 0})
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        
        # Process each row
        for row in reader:
            if row and len(row) >= 3:
                url = row[0]
                landing_flag = row[1]
                user_count_str = row[2]
                
                normalized = normalize_url(url)
                
                # Parse user count, default to 0 if empty or invalid
                try:
                    user_count = int(user_count_str) if user_count_str else 0
                except ValueError:
                    user_count = 0
                
                # Aggregate data
                url_data[normalized]['user_count'] += user_count
                # Keep landing flag if it's set (prefer non-empty values)
                if landing_flag:
                    url_data[normalized]['landing_flag'] = landing_flag
    
    # Write deduplicated data
    total_count = len(url_data)
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        
        # Sort by user count descending for consistency with original file
        sorted_urls = sorted(url_data.items(), 
                           key=lambda x: x[1]['user_count'], 
                           reverse=True)
        
        for url, data in sorted_urls:
            writer.writerow([url, data['landing_flag'], data['user_count']])
    
    return total_count


def main():
    print("Starting URL deduplication process...")
    print()
    
    # Deduplicate ClarityData.csv
    print("Processing ClarityData.csv...")
    clarity_total, clarity_dupes = deduplicate_clarity_data(
        'ClarityData.csv', 
        'ClarityData_deduplicated.csv'
    )
    print(f"  Total URLs: {clarity_total}")
    print(f"  Duplicates removed: {clarity_dupes}")
    print(f"  Unique URLs: {clarity_total - clarity_dupes}")
    print()
    
    # Deduplicate export.csv
    print("Processing export.csv...")
    export_unique = deduplicate_export_data(
        'export.csv',
        'export_deduplicated.csv'
    )
    print(f"  Unique URLs: {export_unique}")
    print()
    
    print("Deduplication complete!")
    print("  - ClarityData_deduplicated.csv created")
    print("  - export_deduplicated.csv created")


if __name__ == '__main__':
    main()
