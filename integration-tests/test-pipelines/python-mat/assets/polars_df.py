""" @bruin
name: mat.polars_df
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import polars as pl


def materialize():
    return pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
