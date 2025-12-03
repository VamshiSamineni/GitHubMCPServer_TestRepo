# URL Deduplication Refactoring

## Summary
This refactoring removed duplicate URLs from the CSV data files by normalizing URLs that differ only by trailing slashes.

## Problem
The repository contained two CSV files with duplicate URL entries:
- **ClarityData.csv**: 44 duplicate URLs (e.g., `example.com/page` and `example.com/page/`)
- **export.csv**: 41 duplicate URLs with the same issue

URLs differing only by a trailing slash are semantically identical and represent the same resource, so they should be deduplicated.

## Solution
Created a Python script (`deduplicate_urls.py`) that:
1. Normalizes all URLs by removing trailing slashes
2. For ClarityData.csv: Removes duplicate entries, keeping only the normalized URL
3. For export.csv: Aggregates user counts for duplicate URLs and preserves LandingPageFlag values

## Results
- **ClarityData.csv**: Reduced from 4,421 to 4,376 rows (44 duplicates removed)
- **export.csv**: Reduced from 3,736 to 3,695 rows (41 duplicates removed, user counts aggregated)

## Files
- `deduplicate_urls.py`: Deduplication script (can be reused for future datasets)
- `ClarityData_original.csv`: Original file (backup)
- `export_original.csv`: Original file (backup)
- `ClarityData.csv`: Deduplicated file
- `export.csv`: Deduplicated file

## Usage
To deduplicate new data files:
```bash
python3 deduplicate_urls.py
```

## Verification
After deduplication:
- No URLs with trailing slashes remain in the deduplicated files
- All URLs are unique when normalized
- User counts in export.csv were correctly aggregated
