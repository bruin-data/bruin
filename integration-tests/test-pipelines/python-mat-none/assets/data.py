""" @bruin
name: mat.none_return
materialization:
    type: table
connection: duckdb-python-mat-none
@bruin """


def materialize():
    return None
