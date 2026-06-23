""" @bruin
name: mat.table_after_dict
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import pyarrow as pa


def materialize():
    # A nullable dict is buffered (its column infers as null), then a pyarrow
    # Table with concrete types is yielded. The buffered rows must be flushed
    # using the table's schema so both batches agree.
    yield {"col_a": None}
    yield pa.table({"col_a": [1]})
