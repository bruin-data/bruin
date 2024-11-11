""" @bruin

name: myschema.my_mat_asset 
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: merge

columns:
    - name: col1
      type: int
      primary_key: true
      checks:
        - name: unique


@bruin """

import pandas as pd


def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
