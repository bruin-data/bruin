""" @bruin
name: mat.nullable_dicts
materialization:
    type: table
connection: duckdb-python-mat
@bruin """


def materialize():
    # The first yield has a None in a column whose type is only revealed by a
    # later yield. The schema must be locked once the type resolves so the typed
    # values still land instead of raising a schema-mismatch error.
    yield {"id": 1, "score": None}
    yield {"id": 2, "score": 99}
    yield {"id": 3, "score": 50}
