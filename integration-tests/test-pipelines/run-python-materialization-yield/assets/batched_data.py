""" @bruin

name: materialize.batched_data

materialization:
    type: table

connection: duckdb-run-python-materialization-yield
@bruin """

import pandas as pd


def materialize():
    """
    Materialize function that yields data in batches.
    This demonstrates the yield-based batch processing feature.
    Each yield returns a batch of data that will be processed separately.
    """
    for batch_num in range(3):
        data = {
            'batch_id': [batch_num] * 10,
            'row_id': list(range(batch_num * 10, (batch_num + 1) * 10)),
            'value': [f'batch_{batch_num}_row_{i}' for i in range(10)]
        }
        df = pd.DataFrame(data)
        yield df
