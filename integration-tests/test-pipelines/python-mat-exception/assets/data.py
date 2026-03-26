""" @bruin
name: mat.exception
materialization:
    type: table
connection: duckdb-python-mat-exception
@bruin """


def materialize():
    raise ValueError("something went wrong in the script")
