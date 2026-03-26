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

import os

import pandas as pd

# Simulates a data source where each date range returns different rows.
# Jan 1-15: ids 1,2,3
# Jan 10-31: ids 2,3,4,5  (ids 2,3 overlap)
# After both runs with merge, we should have exactly 5 unique rows.
ALL_DATA = {
    1: ("alice", "2024-01-01"),
    2: ("bob", "2024-01-10"),
    3: ("charlie", "2024-01-12"),
    4: ("diana", "2024-01-20"),
    5: ("eve", "2024-01-25"),
}


def materialize():
    start = os.environ["BRUIN_START_DATE"]
    end = os.environ["BRUIN_END_DATE"]

    rows = [
        {"id": k, "name": v[0]}
        for k, v in ALL_DATA.items()
        if start <= v[1] <= end
    ]
    return pd.DataFrame(rows)
