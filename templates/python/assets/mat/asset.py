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
    df = pd.DataFrame({
        'col1': range(10),
        'col2': [f'value_new_{i}' for i in range(10)],
        'col3': [i * 6.0 for i in range(10)]
    })

    return df
