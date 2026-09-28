"""Typed DataFrames for Python asset outputs.

A column that is NULL in every row of the first load would otherwise be
created as VARCHAR, and a later load with real values would then conflict.
Returning a DataFrame with explicit dtypes keeps warehouse types stable.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

DTYPES = {
    "varchar": "string",
    "double": "float64",
    "integer": "Int64",
    "boolean": "boolean",
    "timestamp": "datetime64[ns]",
}


def to_frame(rows: Iterable[Mapping[str, Any]], columns: Iterable[Mapping[str, Any]]):
    import pandas as pd  # installed with bruin-sdk; not needed by unit tests

    columns = list(columns)
    names = [c["name"] for c in columns]
    frame = pd.DataFrame(list(rows), columns=names)
    for col in columns:
        dtype = DTYPES[col["type"]]
        if dtype.startswith("datetime"):
            frame[col["name"]] = pd.to_datetime(frame[col["name"]], utc=False)
        else:
            frame[col["name"]] = frame[col["name"]].astype(dtype)
    return frame
