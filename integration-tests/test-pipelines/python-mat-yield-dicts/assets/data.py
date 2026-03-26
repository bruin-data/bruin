""" @bruin
name: mat.yield_dicts
materialization:
    type: table
connection: duckdb-python-mat-yield-dicts
@bruin """


def materialize():
    for i in range(5):
        yield {"id": i, "name": f"item_{i}"}
