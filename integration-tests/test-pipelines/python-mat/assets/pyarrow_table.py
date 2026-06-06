""" @bruin
name: mat.pyarrow_table
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import pyarrow as pa


def materialize():
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
