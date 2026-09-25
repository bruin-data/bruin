#!/usr/bin/env python3
"""Compliant Reddit collector interface; never run without explicit OAuth configuration."""
import os


def collect(start_at, end_at, communities, max_pages):
    """Return immutable records bounded by the supplied window and pagination cap.

    Implement with Reddit's authorised OAuth/public API path, its current terms and
    rate-limit headers. Persist `(source, external_id, payload_json, published_at,
    collected_at, source_cursor, ingest_run_id)` with a merge on `(source, external_id)`.
    """
    if not os.getenv("REDDIT_CLIENT_ID"):
        raise RuntimeError("REDDIT_CLIENT_ID is required for Reddit collection")
    if not start_at or not end_at or start_at >= end_at or max_pages < 1:
        raise ValueError("collection requires a bounded time window and max_pages >= 1")
    return []
