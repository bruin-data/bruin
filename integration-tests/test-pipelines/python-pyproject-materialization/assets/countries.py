""" @bruin

name: materialize.countries

materialization:
    type: table

connection: duckdb-python-pyproject-materialization
@bruin """

import pandas as pd


def materialize():
    data = {
        "country_name": ["Germany", "France", "Spain"],
        "population": [83000000, 67000000, 47000000],
    }
    return pd.DataFrame(data)
