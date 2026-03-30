""" @bruin
name: mat.exception
materialization:
    type: table
connection: duckdb-python-mat-errors
@bruin """


def materialize():
    raise ValueError("something went wrong in the script")
