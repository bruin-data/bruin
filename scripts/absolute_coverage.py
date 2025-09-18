#!/usr/bin/env python3
"""
Script to analyze absolute coverage by combining lines of code (LOC) with coverage data.
This provides both relative coverage percentages and absolute lines covered.
"""

import sys
import os
import re
from collections import defaultdict
from pathlib import Path

def count_lines_in_file(file_path):
    """Count lines of code in a single file, excluding comments and empty lines."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        code_lines = 0
        in_multiline_comment = False
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
                
            # Handle multiline comments
            if '/*' in line:
                in_multiline_comment = True
            if '*/' in line:
                in_multiline_comment = False
                continue
            if in_multiline_comment:
                continue
                
            # Skip single line comments
            if line.startswith('//') or line.startswith('#'):
                continue
                
            # Skip package/import statements for more accurate code counting
            if line.startswith('package ') or line.startswith('import '):
                continue
                
            code_lines += 1
            
        return code_lines
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}")
        return 0

def get_package_from_path(file_path, project_root):
    """Extract package name from file path."""
    try:
        # Convert to relative path from project root
        rel_path = os.path.relpath(file_path, project_root)
        parts = rel_path.split(os.sep)
        
        if len(parts) >= 2 and parts[0] == 'pkg':
            return f"pkg/{parts[1]}"
        elif len(parts) >= 2 and parts[0] == 'cmd':
            return "cmd"
        elif parts[0] == 'main.go':
            return "main"
        else:
            return parts[0] if parts else "unknown"
    except:
        return "unknown"

def count_lines_per_package(project_root):
    """Count lines of code per package."""
    package_loc = defaultdict(int)
    
    # Find all Go files
    go_files = []
    for root, dirs, files in os.walk(project_root):
        # Skip vendor, .git, and other non-source directories
        dirs[:] = [d for d in dirs if not d.startswith('.') and d != 'vendor' and d != 'node_modules']
        
        for file in files:
            if file.endswith('.go'):
                go_files.append(os.path.join(root, file))
    
    # Count lines in each file and group by package
    for file_path in go_files:
        loc = count_lines_in_file(file_path)
        package_name = get_package_from_path(file_path, project_root)
        package_loc[package_name] += loc
    
    return package_loc

def parse_coverage_file(filename):
    """Parse a Go coverage file and return package coverage data."""
    package_coverage = defaultdict(lambda: {'statements': 0, 'covered': 0})
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('mode:'):
                continue
            
            # Example line: github.com/bruin-data/bruin/cmd/fetch.go:21.13,25.41 4 1
            match = re.match(r'([^:]+):(\d+)\.(\d+),(\d+)\.(\d+)\s+(\d+)\s+(\d+)', line)
            if match:
                file_path = match.group(1)
                num_statements = int(match.group(6))
                num_covered = int(match.group(7))
                
                # Extract package path
                parts = file_path.split('/')
                if len(parts) > 3 and parts[2] == 'bruin':
                    if parts[3] == 'cmd':
                        package_name = "cmd"
                    elif parts[3] == 'main.go':
                        package_name = "main"
                    elif parts[3] == 'pkg' and len(parts) > 4:
                        package_name = f"pkg/{parts[4]}"
                    else:
                        package_name = parts[3]
                else:
                    package_name = parts[0] if parts else "unknown"
                
                package_coverage[package_name]['statements'] += num_statements
                if num_covered > 0:
                    package_coverage[package_name]['covered'] += num_statements
    
    return package_coverage

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 absolute_coverage.py <unit_coverage.out> <integration_coverage.out>")
        sys.exit(1)
    
    unit_file = sys.argv[1]
    integration_file = sys.argv[2]
    
    # Get project root (assume script is in scripts/ directory)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    print("📊 Absolute Coverage Analysis")
    print("=============================")
    print()
    
    # Count lines of code per package
    print("🔍 Counting lines of code per package...")
    package_loc = count_lines_per_package(project_root)
    
    # Parse coverage files
    print("🔍 Parsing unit test coverage...")
    unit_coverage = parse_coverage_file(unit_file)
    
    print("🔍 Parsing integration test coverage...")
    integration_coverage = parse_coverage_file(integration_file)
    
    # Combine coverage data (union of both)
    print("🔄 Combining coverage data...")
    combined_coverage = {}
    all_packages = set(unit_coverage.keys()) | set(integration_coverage.keys())
    
    for package in all_packages:
        unit_data = unit_coverage.get(package, {'statements': 0, 'covered': 0})
        integration_data = integration_coverage.get(package, {'statements': 0, 'covered': 0})
        
        # Take the maximum coverage (union)
        combined_coverage[package] = {
            'statements': max(unit_data['statements'], integration_data['statements']),
            'covered': max(unit_data['covered'], integration_data['covered']),
            'unit_statements': unit_data['statements'],
            'unit_covered': unit_data['covered'],
            'integration_statements': integration_data['statements'],
            'integration_covered': integration_data['covered']
        }
    
    # Sort packages by total LOC (descending)
    sorted_packages = sorted(package_loc.items(), key=lambda x: x[1], reverse=True)
    
    print()
    print("📈 Coverage Analysis by Package (sorted by LOC)")
    print("=" * 80)
    print(f"{'Package':<25} {'LOC':<8} {'Covered':<8} {'Total':<8} {'%':<6} {'Unit%':<7} {'Int%':<7}")
    print("-" * 80)
    
    total_loc = 0
    total_covered = 0
    total_statements = 0
    
    for package, loc in sorted_packages:
        coverage_data = combined_coverage.get(package, {
            'statements': 0, 'covered': 0,
            'unit_statements': 0, 'unit_covered': 0,
            'integration_statements': 0, 'integration_covered': 0
        })
        
        statements = coverage_data['statements']
        covered = coverage_data['covered']
        unit_covered = coverage_data['unit_covered']
        unit_statements = coverage_data['unit_statements']
        integration_covered = coverage_data['integration_covered']
        integration_statements = coverage_data['integration_statements']
        
        # Calculate percentages
        combined_pct = (covered / statements * 100) if statements > 0 else 0.0
        unit_pct = (unit_covered / unit_statements * 100) if unit_statements > 0 else 0.0
        integration_pct = (integration_covered / integration_statements * 100) if integration_statements > 0 else 0.0
        
        # Only show packages with LOC > 0 or coverage > 0
        if loc > 0 or statements > 0:
            print(f"{package:<25} {loc:<8} {covered:<8} {statements:<8} {combined_pct:5.1f}% {unit_pct:6.1f}% {integration_pct:6.1f}%")
            
            total_loc += loc
            total_covered += covered
            total_statements += statements
    
    print("-" * 80)
    total_combined_pct = (total_covered / total_statements * 100) if total_statements > 0 else 0.0
    print(f"{'TOTAL':<25} {total_loc:<8} {total_covered:<8} {total_statements:<8} {total_combined_pct:5.1f}%")
    
    print()
    print("📝 Legend:")
    print("  LOC      = Lines of Code (excluding comments/empty lines)")
    print("  Covered  = Lines covered by tests (absolute count)")
    print("  Total    = Total testable statements")
    print("  %        = Combined coverage percentage (union of unit + integration)")
    print("  Unit%    = Unit test coverage percentage")
    print("  Int%     = Integration test coverage percentage")

if __name__ == "__main__":
    main()
