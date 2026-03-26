""" @bruin
name: mat.merge_test
materialization:
    type: table
    strategy: merge

columns:
    - name: id
      type: integer
      primary_key: true
    - name: name
      type: string

connection: duckdb-python-mat-merge
@bruin """

import pandas as pd


def materialize():
    return pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
