#!/usr/bin/env python3
"""Idempotent webhook delivery contract; it cannot alter a source record."""


def deliver(alert, send):
    if alert["status"] == "delivered":
        return "already_delivered"
    if alert["status"] == "dry_run":
        return "dry_run"
    # Callers must persist a delivered status keyed by alert_key only after success.
    send(alert["payload_json"])
    return "delivered"
