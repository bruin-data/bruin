#!/usr/bin/env python3
"""
Validate web links in markdown files.
Uses only Python standard library - no external dependencies required.
"""
import os
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path
from urllib.parse import urlparse

# Exclude patterns for files/directories
EXCLUDE_PATTERNS = [
    'node_modules',
    '.git',
    'bin',
    'logs',
    'vendor',
    'integration-tests',
    '.db',
    '.db-shm',
    '.db-wal',
]

# URLs to skip (localhost, mailto, etc.)
SKIP_URL_PATTERNS = [
    r'^mailto:',
    r'^localhost',
    r'^127\.0\.0\.1',
    r'^file://',
]

# Placeholder words to skip in URLs
PLACEHOLDER_WORDS = [
    'my_company',
    'mycompany',
    'yourcompany',
    'your_company',
    'your_access_token',
    'your-username',
    'example.com',
]

# HTTP status codes that are considered OK
OK_STATUS_CODES = {200, 301, 302, 303, 307, 308}

def should_exclude_file(filepath):
    """Check if a file should be excluded from checking."""
    path_str = str(filepath)
    for pattern in EXCLUDE_PATTERNS:
        if pattern in path_str:
            return True
    return False

def should_skip_url(url):
    """Check if a URL should be skipped."""
    # Check for URL patterns (localhost, mailto, etc.)
    for pattern in SKIP_URL_PATTERNS:
        if re.match(pattern, url, re.IGNORECASE):
            return True
    
    # Check for curly brackets (placeholders like {variable})
    if '{' in url or '}' in url:
        return True
    
    # Check for placeholder words (case-insensitive)
    url_lower = url.lower()
    for placeholder in PLACEHOLDER_WORDS:
        if placeholder.lower() in url_lower:
            return True
    
    return False

def extract_urls_from_markdown(content):
    """Extract all HTTP/HTTPS URLs from markdown content."""
    # Pattern to match markdown links: [text](url) and plain URLs
    url_pattern = r'https?://[^\s\)\]\>"]+'
    urls = re.findall(url_pattern, content)
    # Also match markdown link syntax: [text](url)
    markdown_link_pattern = r'\[([^\]]+)\]\((https?://[^\)]+)\)'
    markdown_links = re.findall(markdown_link_pattern, content)
    urls.extend([url for _, url in markdown_links])
    
    # Clean up URLs (remove trailing punctuation that might not be part of URL)
    cleaned_urls = []
    for url in urls:
        # Remove trailing common punctuation
        url = url.rstrip('.,;:!?)')
        cleaned_urls.append(url)
    
    return list(set(cleaned_urls))  # Remove duplicates

def check_url(url, timeout=10):
    """Check if a URL is accessible."""
    if should_skip_url(url):
        return True, "skipped"
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status_code = response.getcode()
            if status_code in OK_STATUS_CODES:
                return True, f"OK ({status_code})"
            else:
                return False, f"Status {status_code}"
    except urllib.error.HTTPError as e:
        # Treat 403 (Forbidden) as skipped - many sites block automated checkers
        if e.code == 403:
            return True, "skipped (403 Forbidden - likely blocks automated checkers)"
        if e.code in OK_STATUS_CODES:
            return True, f"OK ({e.code})"
        return False, f"HTTP {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return False, f"Error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def validate_links_in_repo(root_dir='.'):
    """Validate all links in markdown files in the repository."""
    root_path = Path(root_dir)
    broken_links = []
    checked_count = 0
    skipped_count = 0
    
    print("Finding markdown files...")
    markdown_files = []
    for md_file in root_path.rglob('*.md'):
        if not should_exclude_file(md_file):
            markdown_files.append(md_file)
    
    print(f"Found {len(markdown_files)} markdown files to check\n")
    
    for md_file in markdown_files:
        print(f"Checking: {md_file}")
        try:
            with open(md_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            urls = extract_urls_from_markdown(content)
            for url in urls:
                if should_skip_url(url):
                    skipped_count += 1
                    continue
                
                is_valid, message = check_url(url)
                # If the URL was skipped (e.g., 403), count it as skipped
                if message.startswith("skipped"):
                    skipped_count += 1
                    continue
                
                checked_count += 1
                if not is_valid:
                    broken_links.append((md_file, url, message))
                    print(f"  ✗ {url} - {message}")
                else:
                    print(f"  ✓ {url}")
        except Exception as e:
            print(f"  Error reading file: {e}")
            broken_links.append((md_file, None, f"Error: {str(e)}"))
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Files checked: {len(markdown_files)}")
    print(f"  URLs checked: {checked_count}")
    print(f"  URLs skipped: {skipped_count}")
    print(f"  Broken links: {len(broken_links)}")
    
    if broken_links:
        print(f"\n{'='*60}")
        print("Broken links found:")
        for file, url, message in broken_links:
            if url:
                print(f"  {file}: {url} - {message}")
            else:
                print(f"  {file}: {message}")
        return 1
    
    print("\n✓ All links validated successfully!")
    return 0

if __name__ == '__main__':
    root = sys.argv[1] if len(sys.argv) > 1 else '.'
    sys.exit(validate_links_in_repo(root))

