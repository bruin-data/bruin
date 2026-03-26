""" @bruin
name: mat.exit
materialization:
    type: table
connection: duckdb-python-mat-exit
@bruin """

import sys


def materialize():
    sys.exit(1)
