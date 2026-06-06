""" @bruin
name: mat.pyarrow_tables
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import pyarrow as pa


def materialize():
    yield pa.table({"id": [1, 2], "name": ["a", "b"]})
    yield pa.table({"id": [3, 4], "name": ["c", "d"]})
