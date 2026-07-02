"""@bruin

name: bronze.product_costs
description: Monthly SKU cost and list price inputs for margin analysis.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - costs
domains:
  - sales
meta:
  asset_grain: One row per SKU and cost month extract record.
  source_system: Synthetic finance cost table stand-in.

materialization:
  type: table
  strategy: append

depends:
  - bronze.products
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: sku_id
    type: VARCHAR
    primary_key: true
  - name: cost_month
    type: DATE
    primary_key: true
  - name: unit_cost_usd
    type: DOUBLE
    checks:
      - name: non_negative
  - name: list_price_usd
    type: DOUBLE
    checks:
      - name: non_negative
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

SKU_PRICES = {
    "RB-084-ORG": (1.02, 2.79, date(1987, 4, 1), None),
    "RB-120-ORG": (1.30, 3.29, date(1997, 1, 1), None),
    "RB-084-SF": (1.05, 2.99, date(1999, 1, 1), None),
    "RB-120-SF": (1.34, 3.49, date(2003, 1, 1), None),
    "RB-120-ZERO": (1.33, 3.49, date(2022, 1, 1), None),
    "RB-084-WM": (1.08, 2.99, date(2020, 3, 1), None),
    "RB-120-WM": (1.37, 3.59, date(2020, 3, 1), None),
    "RB-120-TROP": (1.39, 3.59, date(2014, 3, 1), None),
    "RB-120-AMBER": (1.40, 3.59, date(2022, 4, 1), None),
    "RB-120-SEA": (1.41, 3.69, date(2023, 6, 1), None),
    "RB-120-COCONUT": (1.40, 3.59, date(2018, 4, 1), None),
    "RB-120-PEACH": (1.40, 3.59, date(2019, 3, 1), None),
    "RB-120-CURUBA": (1.42, 3.69, date(2024, 4, 1), None),
    "RB-120-PINK": (1.43, 3.69, date(2025, 1, 15), None),
    "RB-120-BLUE": (1.39, 3.59, date(2012, 3, 1), date(2025, 12, 31)),
    "RB-120-DRG": (1.42, 3.69, date(2021, 4, 1), date(2024, 10, 31)),
    "RB-120-IVB": (1.45, 3.79, date(2024, 10, 15), None),
    "RB-120-WPCH": (1.44, 3.79, date(2025, 4, 1), date(2025, 9, 30)),
    "RB-084-SF-SEA": (1.10, 3.09, date(2024, 3, 1), date(2025, 2, 28)),
    "RB-120-SF-CUR": (1.46, 3.79, date(2024, 4, 1), date(2025, 9, 30)),
    "RB-120-CHSAK": (1.47, 3.89, date(2026, 2, 23), date(2026, 6, 30)),
    "RB-120-SF-CHSAK": (1.49, 3.89, date(2026, 2, 23), date(2026, 6, 30)),
    "RB-120-SUDACHI": (1.48, 3.89, date(2026, 4, 1), date(2026, 9, 30)),
    "RB-120-SF-SUDACHI": (1.50, 3.89, date(2026, 4, 1), date(2026, 9, 30)),
}


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def month_starts(start, end):
    current = date(start.year, start.month, 1)
    if current < start and start <= DEFAULT_START_DATE:
        current = DEFAULT_START_DATE
    elif current < start:
        current = date(start.year + (start.month // 12), (start.month % 12) + 1, 1)
    while current < end:
        yield current
        current = date(current.year + (current.month // 12), (current.month % 12) + 1, 1)


def is_active(sku_launch, sku_end, month):
    month_end = date(month.year + (month.month // 12), (month.month % 12) + 1, 1)
    return sku_launch < month_end and (sku_end or date.max) >= month


def materialize():
    start, end = get_interval()
    updated_at = datetime.combine(start, datetime.min.time())
    rows = []
    for cost_month in month_starts(start, end):
        month_number = ((cost_month.year - 2024) * 12) + cost_month.month
        inflation_factor = 1 + (0.018 * (cost_month.year - 2024))
        aluminum_factor = 1.012 if cost_month.month in (5, 6, 7, 8) else 1.0
        list_price_factor = 1.03 if cost_month >= date(2025, 2, 1) else 1.0
        for sku_id, (unit_cost, list_price, launch_date, planned_end_date) in SKU_PRICES.items():
            if not is_active(launch_date, planned_end_date, cost_month):
                continue
            cost_noise = 1 + (((month_number + len(sku_id)) % 5) - 2) * 0.002
            rows.append(
                {
                    "sku_id": sku_id,
                    "cost_month": cost_month,
                    "unit_cost_usd": round(unit_cost * inflation_factor * aluminum_factor * cost_noise, 2),
                    "list_price_usd": round(list_price * list_price_factor, 2),
                    "updated_at": updated_at,
                }
            )
    return rows
