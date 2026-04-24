""" @bruin
name: mat.yield_batches
materialization:
    type: table
connection: duckdb-python-mat
@bruin """


def materialize():
    for page in range(3):
        batch = [{"id": page * 2 + i, "name": f"item_{page * 2 + i}"} for i in range(2)]
        yield batch
