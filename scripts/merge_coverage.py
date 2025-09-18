#!/usr/bin/env python3
"""
Simple script to merge unit test and integration test coverage data.
This is a workaround since go tool covdata merge doesn't work with mixed formats.
"""

import sys
import re
from collections import defaultdict

def parse_coverage_file(filename):
    """Parse a Go coverage file and return a dict of line coverage."""
    coverage = {}
    
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
                
                key = f"{file_path}:{start_line}.{start_col},{end_line}.{end_col}"
                # For coverage, we care about the number of covered statements
                # If count > 0, then all statements in this block are covered
                coverage[key] = {
                    'statements': num_statements,
                    'covered': num_statements if num_covered > 0 else 0
                }
    
    return coverage

def merge_coverage(unit_coverage, integration_coverage):
    """Merge two coverage dictionaries, taking the maximum coverage for each line."""
    merged = {}
    
    # Get all unique keys
    all_keys = set(unit_coverage.keys()) | set(integration_coverage.keys())
    
    for key in all_keys:
        unit_data = unit_coverage.get(key, {'statements': 0, 'covered': 0})
        integration_data = integration_coverage.get(key, {'statements': 0, 'covered': 0})
        
        # Take the maximum coverage (union of covered lines)
        merged[key] = {
            'statements': max(unit_data['statements'], integration_data['statements']),
            'covered': max(unit_data['covered'], integration_data['covered'])
        }
    
    return merged

def calculate_total_coverage(coverage):
    """Calculate total coverage percentage."""
    total_statements = 0
    total_covered = 0
    
    for data in coverage.values():
        total_statements += data['statements']
        total_covered += data['covered']
    
    if total_statements == 0:
        return 0.0
    
    return (total_covered / total_statements) * 100

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 merge_coverage.py <unit_coverage.out> <integration_coverage.out>")
        sys.exit(1)
    
    unit_file = sys.argv[1]
    integration_file = sys.argv[2]
    
    print("📊 Merging Coverage Data")
    print("========================")
    
    # Parse coverage files
    print("🔍 Parsing unit test coverage...")
    unit_coverage = parse_coverage_file(unit_file)
    
    print("🔍 Parsing integration test coverage...")
    integration_coverage = parse_coverage_file(integration_file)
    
    # Calculate individual coverage
    unit_percentage = calculate_total_coverage(unit_coverage)
    integration_percentage = calculate_total_coverage(integration_coverage)
    
    print(f"📈 Unit Test Coverage: {unit_percentage:.1f}%")
    print(f"📈 Integration Test Coverage: {integration_percentage:.1f}%")
    
    # Debug: show some stats
    print(f"🔍 Unit coverage entries: {len(unit_coverage)}")
    print(f"🔍 Integration coverage entries: {len(integration_coverage)}")
    
    # Merge coverage
    print("🔄 Merging coverage data...")
    merged_coverage = merge_coverage(unit_coverage, integration_coverage)
    
    # Calculate merged coverage
    merged_percentage = calculate_total_coverage(merged_coverage)
    
    print(f"🎯 Combined Coverage: {merged_percentage:.1f}%")
    print("")
    print("📝 Note: This is a simple union of coverage data.")
    print("   It represents the total lines covered by either test type.")

if __name__ == "__main__":
    main()
