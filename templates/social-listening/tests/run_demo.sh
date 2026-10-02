#!/usr/bin/env bash
# Run the template's full local test suite: unit tests, bruin validate, and the
# DuckDB fixture pipeline end to end (see tests/e2e_demo.py).
#
#   bash tests/run_demo.sh [path-to-bruin]
set -euo pipefail

BRUIN="${1:-bruin}"
TEMPLATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== unit tests"
(cd "$TEMPLATE_DIR" && python3 -m unittest discover -s tests -t . -q)

echo "== end-to-end demo"
python3 "$TEMPLATE_DIR/tests/e2e_demo.py" "$BRUIN"
