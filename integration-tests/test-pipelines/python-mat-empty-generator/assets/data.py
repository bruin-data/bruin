""" @bruin
name: mat.empty_generator
materialization:
    type: table
connection: duckdb-python-mat-empty-generator
@bruin """


def materialize():
    return
    yield  # noqa: makes this a generator that yields nothing
