#!/usr/bin/env python3
"""Bounded Hacker News Algolia API collector interface."""


def collect(start_at, end_at, terms, max_pages):
    """Use https://hn.algolia.com/api; stop at `max_pages` and persist its cursor."""
    if not start_at or not end_at or start_at >= end_at or max_pages < 1:
        raise ValueError("collection requires a bounded time window and max_pages >= 1")
    return []
