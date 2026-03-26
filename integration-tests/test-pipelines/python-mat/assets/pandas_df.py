""" @bruin
name: mat.pandas_df
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import pandas as pd


def materialize():
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
