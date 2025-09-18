#!/usr/bin/env python3
"""
Show coverage breakdown by package for integration tests.
"""

import sys
import re
from collections import defaultdict

def parse_coverage_file(filename):
    """Parse a Go coverage file and return coverage by package."""
    package_coverage = defaultdict(lambda: {'statements': 0, 'covered': 0})
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('mode:'):
                continue
                
            # Parse format: file.go:start_line.start_col,end_line.end_col num_statements num_covered
            match = re.match(r'([^:]+):(\d+)\.(\d+),(\d+)\.(\d+)\s+(\d+)\s+(\d+)', line)
            if match:
                file_path = match.group(1)
                start_line = int(match.group(2))
                start_col = int(match.group(3))
                end_line = int(match.group(4))
                end_col = int(match.group(5))
                num_statements = int(match.group(6))
                num_covered = int(match.group(7))
                
                # Extract package from file path
                # e.g., github.com/bruin-data/bruin/pkg/duckdb/db.go -> pkg/duckdb
                # e.g., github.com/bruin-data/bruin/cmd/fetch.go -> cmd
                parts = file_path.split('/')
                if 'pkg' in parts:
                    pkg_index = parts.index('pkg')
                    if pkg_index + 1 < len(parts):
                        package = '/'.join(parts[pkg_index:pkg_index+2])  # pkg/duckdb
                    else:
                        package = parts[pkg_index]  # pkg
                elif 'cmd' in parts:
                    package = 'cmd'
                else:
                    package = 'main'
                
                # Add to package coverage
                package_coverage[package]['statements'] += num_statements
                if num_covered > 0:
                    package_coverage[package]['covered'] += num_statements
    
    return package_coverage

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 coverage_by_package.py <coverage_file>")
        sys.exit(1)
    
    coverage_file = sys.argv[1]
    
    print(f"📊 Coverage by Package: {coverage_file}")
    print("=" * 50)
    
    package_coverage = parse_coverage_file(coverage_file)
    
    # Sort by coverage percentage (descending)
    sorted_packages = sorted(package_coverage.items(), 
                           key=lambda x: (x[1]['covered'] / x[1]['statements']) if x[1]['statements'] > 0 else 0, 
                           reverse=True)
    
    total_statements = 0
    total_covered = 0
    
    for package, data in sorted_packages:
        statements = data['statements']
        covered = data['covered']
        percentage = (covered / statements) * 100 if statements > 0 else 0
        
        total_statements += statements
        total_covered += covered
        
        print(f"{package:<30} {covered:>6}/{statements:<6} {percentage:>6.1f}%")
    
    print("-" * 50)
    total_percentage = (total_covered / total_statements) * 100 if total_statements > 0 else 0
    print(f"{'TOTAL':<30} {total_covered:>6}/{total_statements:<6} {total_percentage:>6.1f}%")

if __name__ == "__main__":
    main()
