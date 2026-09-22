#!/usr/bin/env sh
set -eu
bruin validate .
python3 -m unittest discover -s tests
bruin run --workers 1 .
