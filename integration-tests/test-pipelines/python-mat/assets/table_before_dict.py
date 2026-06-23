""" @bruin
name: mat.table_before_dict
materialization:
    type: table
connection: duckdb-python-mat
@bruin """

import pyarrow as pa


def materialize():
    # The reverse order: a pyarrow Table establishes the schema first, then a
    # nullable dict is yielded. The dict must conform to the table's schema.
    yield pa.table({"col_a": [1]})
    yield {"col_a": None}
