""" @bruin
name: mat.empty_list
materialization:
    type: table
connection: duckdb-python-mat
@bruin """


def materialize():
    return []
