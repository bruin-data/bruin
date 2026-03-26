""" @bruin
name: mat.empty_list
materialization:
    type: table
connection: duckdb-python-mat-empty-list
@bruin """


def materialize():
    return []
