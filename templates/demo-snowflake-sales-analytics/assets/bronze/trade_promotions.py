"""@bruin

name: bronze.trade_promotions
description: Trade promotion calendar for Energy Drink Off Premise launch and seasonal support.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - promotions
domains:
  - sales
meta:
  asset_grain: One row per SKU, retailer, and promotion window extract record.
  source_system: Synthetic trade promotion planning table stand-in.

materialization:
  type: table
  strategy: append

depends:
  - bronze.products
  - bronze.retailers
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: promotion_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: sku_id
    type: VARCHAR
  - name: retailer_id
    type: VARCHAR
  - name: start_date
    type: DATE
  - name: end_date
    type: DATE
  - name: promo_type
    type: VARCHAR
  - name: discount_pct
    type: DOUBLE
  - name: display_support
    type: BOOLEAN
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime, timedelta

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "grocery", 0.080),
    ("RTL-GLOBAL-MASS", "mass", 0.070),
    ("RTL-GLOBAL-CONVENIENCE", "convenience", 0.045),
    ("RTL-GLOBAL-CASH-CARRY", "club", 0.100),
    ("RTL-GLOBAL-ECOM", "ecommerce", 0.075),
]

PROMOTION_PLANS = [
    ("RB-120-CURUBA", date(2024, 4, 1), "summer_launch_display", 35, ("grocery", "mass", "club")),
    ("RB-120-DRG", date(2024, 8, 12), "closeout_feature", 21, ("grocery", "convenience", "ecommerce")),
    ("RB-084-SF-SEA", date(2024, 3, 4), "sugarfree_trial", 28, ("grocery", "mass", "ecommerce")),
    ("RB-120-SF-CUR", date(2024, 5, 6), "sugarfree_launch_display", 28, ("grocery", "mass", "club")),
    ("RB-120-IVB", date(2024, 10, 21), "winter_launch_display", 42, ("grocery", "mass", "club", "ecommerce")),
    ("RB-120-IVB", date(2025, 1, 6), "new_year_energy_feature", 21, ("mass", "grocery", "convenience")),
    ("RB-120-PINK", date(2025, 2, 10), "spring_reset_feature", 28, ("grocery", "mass", "ecommerce")),
    ("RB-120-WPCH", date(2025, 4, 7), "summer_launch_display", 42, ("grocery", "mass", "club", "ecommerce")),
    ("RB-120-WPCH", date(2025, 7, 1), "summer_peak_feature", 28, ("convenience", "grocery", "mass")),
    ("RB-120-BLUE", date(2025, 8, 18), "phaseout_feature", 21, ("grocery", "ecommerce")),
    ("RB-120-ORG", date(2024, 9, 2), "football_season_feature", 28, ("club", "mass", "convenience")),
    ("RB-120-ORG", date(2025, 9, 1), "football_season_feature", 28, ("club", "mass", "convenience")),
    ("RB-120-WM", date(2024, 6, 10), "summer_feature", 28, ("grocery", "mass", "convenience")),
    ("RB-120-WM", date(2025, 6, 9), "summer_feature", 28, ("grocery", "mass", "convenience")),
    ("RB-120-SEA", date(2024, 7, 8), "summer_feature", 21, ("grocery", "ecommerce")),
    ("RB-120-SEA", date(2025, 7, 7), "summer_feature", 21, ("grocery", "ecommerce")),
    ("RB-120-SF", date(2025, 1, 6), "new_year_sugarfree_feature", 21, ("grocery", "mass", "ecommerce")),
    ("RB-120-ZERO", date(2025, 1, 6), "new_year_sugarfree_feature", 21, ("grocery", "mass", "ecommerce")),
    ("RB-120-CHSAK", date(2026, 2, 23), "spring_launch_display", 35, ("grocery", "mass", "ecommerce")),
    ("RB-120-SF-CHSAK", date(2026, 2, 23), "spring_sugarfree_launch", 28, ("grocery", "mass", "ecommerce")),
    ("RB-120-SUDACHI", date(2026, 4, 27), "summer_launch_display", 42, ("grocery", "mass", "club", "ecommerce")),
    ("RB-120-SF-SUDACHI", date(2026, 4, 27), "summer_sugarfree_launch", 35, ("grocery", "mass", "ecommerce")),
    ("RB-120-SUDACHI", date(2026, 6, 8), "summer_peak_feature", 28, ("grocery", "mass", "convenience")),
    ("RB-120-IVB", date(2026, 1, 5), "made_permanent_feature", 21, ("grocery", "mass", "ecommerce")),
]


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def should_emit(plan_start, start, end):
    if start <= DEFAULT_START_DATE:
        return plan_start < end
    return start <= plan_start < end


def materialize():
    interval_start, interval_end = get_interval()
    updated_at = datetime.combine(interval_start, datetime.min.time())
    rows = []
    for sku_id, plan_start, promo_type, duration_days, eligible_channels in PROMOTION_PLANS:
        if not should_emit(plan_start, interval_start, interval_end):
            continue
        for retailer_index, (retailer_id, channel, base_discount) in enumerate(RETAILERS, start=1):
            if channel not in eligible_channels:
                continue
            start_date = plan_start + timedelta(days=(retailer_index % 3) * 3)
            end_date = start_date + timedelta(days=duration_days)
            rows.append(
                {
                    "promotion_id": f"PROMO-{sku_id}-{retailer_id}-{start_date:%Y%m%d}",
                    "sku_id": sku_id,
                    "retailer_id": retailer_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "promo_type": promo_type,
                    "discount_pct": round(base_discount + (0.015 if "launch" in promo_type else 0), 3),
                    "display_support": channel in ("grocery", "mass", "club"),
                    "updated_at": updated_at,
                }
            )
    return rows
