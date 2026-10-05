"""Read-only warehouse access for Python assets, via the Bruin Python SDK.

Using ``bruin.get_connection`` keeps the Python assets warehouse-agnostic: the
same code reads DuckDB in the demo and MotherDuck, Postgres, BigQuery or
Snowflake in production. Each read opens and closes its own connection so a
local DuckDB file is never held open while Bruin runs other assets, and lock
conflicts on the file are retried with backoff.
"""

from __future__ import annotations

import math
import os
import time
from typing import Any


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except (TypeError, ValueError):
            return None
    if type(value).__name__ in {"NaTType", "NAType"}:
        return None
    return value


def literal(value: str) -> str:
    """A SQL string literal (single quotes doubled)."""
    return "'" + str(value).replace("'", "''") + "'"


def read(
    sql: str, connection: str | None = None, attempts: int = 6
) -> list[dict[str, Any]]:
    from bruin import get_connection  # imported lazily so unit tests need no SDK

    name = connection or os.environ.get("BRUIN_CONNECTION")
    for attempt in range(attempts):
        try:
            with get_connection(name) as conn:
                frame = conn.query(sql)
            if frame is None:
                return []
            return [
                {k: _clean(v) for k, v in row.items()}
                for row in frame.to_dict("records")
            ]
        except Exception as err:  # noqa: BLE001 - the SDK wraps driver errors
            if "lock" in str(err).lower() and attempt < attempts - 1:
                time.sleep(0.5 * 2**attempt)
                continue
            raise
    return []


def table_exists(schema: str, table: str, connection: str | None = None) -> bool:
    rows = read(
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        f"WHERE lower(table_schema) = {literal(schema.lower())} AND lower(table_name) = {literal(table.lower())}",
        connection,
    )
    return bool(rows and rows[0]["n"])
