"""@bruin

name: bronze.retailers
description: Source-shaped retailer account master for Off Premise sales analysis.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - retailers
domains:
  - sales
meta:
  asset_grain: One row per global retailer channel extract record.
  source_system: Synthetic internal account master stand-in.

materialization:
  type: table
  strategy: append
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: retailer_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: retailer_name
    type: VARCHAR
  - name: channel
    type: VARCHAR
  - name: account_tier
    type: VARCHAR
  - name: national_account_manager
    type: VARCHAR
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "Modern Grocery", "grocery", "global_channel", "Global Grocery Team", date(2024, 1, 1)),
    ("RTL-GLOBAL-MASS", "Mass & Hypermarket", "mass", "global_channel", "Global Mass Team", date(2024, 1, 1)),
    ("RTL-GLOBAL-CONVENIENCE", "Convenience & Fuel", "convenience", "global_channel", "Global Convenience Team", date(2024, 1, 1)),
    ("RTL-GLOBAL-CASH-CARRY", "Cash & Carry / Club", "club", "global_channel", "Global Club Team", date(2024, 1, 1)),
    ("RTL-GLOBAL-ECOM", "Digital Commerce", "ecommerce", "global_channel", "Global Ecommerce Team", date(2024, 1, 1)),
]


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def should_emit(opened_date, start, end):
    if start <= DEFAULT_START_DATE:
        return opened_date < end
    return start <= opened_date < end


def materialize():
    start, end = get_interval()
    updated_at = datetime.combine(start, datetime.min.time())
    rows = []
    for retailer_id, retailer_name, channel, account_tier, manager, opened_date in RETAILERS:
        if not should_emit(opened_date, start, end):
            continue
        rows.append(
            {
                "retailer_id": retailer_id,
                "retailer_name": retailer_name,
                "channel": channel,
                "account_tier": account_tier,
                "national_account_manager": manager,
                "updated_at": updated_at,
            }
        )
    return rows
