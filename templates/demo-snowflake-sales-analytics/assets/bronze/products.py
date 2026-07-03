"""@bruin

name: bronze.products
description: |
  Source-shaped product catalog for the Energy Drink Off Premise sales analytics
  demo. The table includes core Energy Drink products, current Editions, and
  seasonal or retiring limited-edition SKUs used for lifecycle decisions.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - product_catalog
  - limited_edition
domains:
  - sales
meta:
  asset_grain: One row per sellable SKU extract record.
  pipeline_role: Bronze demo source table materialized by Python.
  source_system: Synthetic internal product master stand-in.

materialization:
  type: table
  strategy: append
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: sku_id
    type: VARCHAR
    description: Stable SKU identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: sku_name
    type: VARCHAR
    description: Product display name.
  - name: product_family
    type: VARCHAR
    description: Energy drink product family.
  - name: flavor
    type: VARCHAR
    description: Flavor or edition name.
  - name: pack_size
    type: VARCHAR
    description: Retail pack format.
  - name: lifecycle_type
    type: VARCHAR
    description: Product lifecycle category such as permanent, limited edition, or permanent candidate.
  - name: lifecycle_status
    type: VARCHAR
    description: Current lifecycle state used by SKU council decisions.
  - name: launch_date
    type: DATE
    description: Initial launch date.
  - name: planned_end_date
    type: DATE
    description: Planned limited-edition end date when applicable.
  - name: decision_cycle
    type: VARCHAR
    description: SKU council decision cycle when the SKU is in review.
  - name: season_year
    type: INTEGER
    description: Seasonal product year when applicable.
  - name: source_confidence
    type: VARCHAR
    description: Whether the demo SKU metadata is based on official public product pages or modeled demo assumptions.
  - name: benchmark_group
    type: VARCHAR
    description: Benchmark group used for decision comparisons.
  - name: target_margin_pct
    type: DOUBLE
    description: Target gross margin percentage.
  - name: updated_at
    type: TIMESTAMP
    description: Source extract timestamp.

@bruin"""

import os
from datetime import date, datetime

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

PRODUCTS = [
    {
        "sku_id": "RB-084-ORG",
        "sku_name": "Energy Drink Original 8.4 fl oz",
        "product_family": "Core Energy",
        "flavor": "Original",
        "pack_size": "8.4 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(1987, 4, 1),
        "planned_end_date": None,
        "benchmark_group": "core_8_4oz",
        "target_margin_pct": 0.42,
    },
    {
        "sku_id": "RB-120-ORG",
        "sku_name": "Energy Drink Original 12 fl oz",
        "product_family": "Core Energy",
        "flavor": "Original",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(1997, 1, 1),
        "planned_end_date": None,
        "benchmark_group": "core_12oz",
        "target_margin_pct": 0.42,
    },
    {
        "sku_id": "RB-084-SF",
        "sku_name": "Energy Drink Sugarfree 8.4 fl oz",
        "product_family": "Sugarfree",
        "flavor": "Sugarfree",
        "pack_size": "8.4 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(1999, 1, 1),
        "planned_end_date": None,
        "benchmark_group": "sugarfree_8_4oz",
        "target_margin_pct": 0.41,
    },
    {
        "sku_id": "RB-120-SF",
        "sku_name": "Energy Drink Sugarfree 12 fl oz",
        "product_family": "Sugarfree",
        "flavor": "Sugarfree",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2003, 1, 1),
        "planned_end_date": None,
        "benchmark_group": "sugarfree_12oz",
        "target_margin_pct": 0.41,
    },
    {
        "sku_id": "RB-120-ZERO",
        "sku_name": "Energy Drink Zero 12 fl oz",
        "product_family": "Zero Sugar",
        "flavor": "Zero Sugar",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2022, 1, 1),
        "planned_end_date": None,
        "benchmark_group": "sugarfree_12oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-084-WM",
        "sku_name": "Energy Drink Red Edition Watermelon 8.4 fl oz",
        "product_family": "Editions",
        "flavor": "Watermelon",
        "pack_size": "8.4 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2020, 3, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_8_4oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-120-WM",
        "sku_name": "Energy Drink Red Edition Watermelon 12 fl oz",
        "product_family": "Editions",
        "flavor": "Watermelon",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2020, 3, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-120-TROP",
        "sku_name": "Energy Drink Yellow Edition Tropical 12 fl oz",
        "product_family": "Editions",
        "flavor": "Tropical",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2014, 3, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-AMBER",
        "sku_name": "Energy Drink Amber Edition Strawberry Apricot 12 fl oz",
        "product_family": "Editions",
        "flavor": "Strawberry Apricot",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2022, 4, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-SEA",
        "sku_name": "Energy Drink Sea Blue Edition Juneberry 12 fl oz",
        "product_family": "Editions",
        "flavor": "Juneberry",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2023, 6, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-COCONUT",
        "sku_name": "Energy Drink Coconut Edition Coconut Berry 12 fl oz",
        "product_family": "Editions",
        "flavor": "Coconut Berry",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2018, 4, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-PEACH",
        "sku_name": "Energy Drink Peach Edition White Peach 12 fl oz",
        "product_family": "Editions",
        "flavor": "White Peach",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2019, 3, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-CURUBA",
        "sku_name": "Energy Drink Green Edition Curuba Elderflower 12 fl oz",
        "product_family": "Editions",
        "flavor": "Curuba Elderflower",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2024, 4, 1),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-PINK",
        "sku_name": "Energy Drink Pink Edition Wild Berries 12 fl oz",
        "product_family": "Editions",
        "flavor": "Wild Berries",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent",
        "launch_date": date(2025, 1, 15),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-BLUE",
        "sku_name": "Energy Drink Blue Edition Blueberry 12 fl oz",
        "product_family": "Editions",
        "flavor": "Blueberry",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2012, 3, 1),
        "planned_end_date": date(2025, 12, 31),
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-DRG",
        "sku_name": "Energy Drink Green Edition Dragon Fruit 12 fl oz",
        "product_family": "Editions",
        "flavor": "Dragon Fruit",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2021, 4, 1),
        "planned_end_date": date(2024, 10, 31),
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-IVB",
        "sku_name": "Energy Drink Iced Edition Iced Vanilla Berry 12 fl oz",
        "product_family": "Seasonal Editions",
        "flavor": "Iced Vanilla Berry",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "permanent_candidate",
        "launch_date": date(2024, 10, 15),
        "planned_end_date": None,
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-WPCH",
        "sku_name": "Energy Drink Summer Edition White Peach 12 fl oz",
        "product_family": "Seasonal Editions",
        "flavor": "White Peach",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2025, 4, 1),
        "planned_end_date": date(2025, 9, 30),
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-084-SF-SEA",
        "sku_name": "Energy Drink Sea Blue Edition Juneberry Sugarfree 8.4 fl oz",
        "product_family": "Editions Sugarfree",
        "flavor": "Juneberry Sugarfree",
        "pack_size": "8.4 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2024, 3, 1),
        "planned_end_date": date(2025, 2, 28),
        "benchmark_group": "sugarfree_8_4oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-120-SF-CUR",
        "sku_name": "Energy Drink Green Edition Curuba Elderflower Sugarfree 12 fl oz",
        "product_family": "Editions Sugarfree",
        "flavor": "Curuba Elderflower Sugarfree",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2024, 4, 1),
        "planned_end_date": date(2025, 9, 30),
        "benchmark_group": "sugarfree_12oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-120-CHSAK",
        "sku_name": "Energy Drink Spring Edition Cherry Sakura 12 fl oz",
        "product_family": "Seasonal Editions",
        "flavor": "Cherry Sakura",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2026, 2, 23),
        "planned_end_date": date(2026, 6, 30),
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-SF-CHSAK",
        "sku_name": "Energy Drink Spring Edition Cherry Sakura Sugarfree 12 fl oz",
        "product_family": "Editions Sugarfree",
        "flavor": "Cherry Sakura Sugarfree",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2026, 2, 23),
        "planned_end_date": date(2026, 6, 30),
        "benchmark_group": "sugarfree_12oz",
        "target_margin_pct": 0.40,
    },
    {
        "sku_id": "RB-120-SUDACHI",
        "sku_name": "Energy Drink Summer Edition Sudachi Lime 12 fl oz",
        "product_family": "Seasonal Editions",
        "flavor": "Sudachi Lime",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2026, 4, 1),
        "planned_end_date": date(2026, 9, 30),
        "benchmark_group": "edition_12oz",
        "target_margin_pct": 0.39,
    },
    {
        "sku_id": "RB-120-SF-SUDACHI",
        "sku_name": "Energy Drink Summer Edition Sudachi Lime Sugarfree 12 fl oz",
        "product_family": "Editions Sugarfree",
        "flavor": "Sudachi Lime Sugarfree",
        "pack_size": "12 fl oz single",
        "lifecycle_type": "limited_edition",
        "launch_date": date(2026, 4, 1),
        "planned_end_date": date(2026, 9, 30),
        "benchmark_group": "sugarfree_12oz",
        "target_margin_pct": 0.40,
    },
]

DEFAULT_PRODUCT_META = {
    "lifecycle_status": "active",
    "decision_cycle": None,
    "season_year": None,
    "source_confidence": "modeled_demo",
}

PRODUCT_META = {
    "RB-120-BLUE": {
        "lifecycle_status": "phaseout",
        "decision_cycle": "2025_q4_retirement",
        "season_year": 2025,
        "source_confidence": "modeled_demo",
    },
    "RB-120-DRG": {
        "lifecycle_status": "retired",
        "decision_cycle": "2024_q4_retirement",
        "season_year": 2024,
        "source_confidence": "modeled_demo",
    },
    "RB-120-IVB": {
        "lifecycle_status": "made_permanent",
        "decision_cycle": "2026_permanence_review",
        "season_year": 2026,
        "source_confidence": "official_public_lineup",
    },
    "RB-120-WPCH": {
        "lifecycle_status": "retired",
        "decision_cycle": "2025_summer_review",
        "season_year": 2025,
        "source_confidence": "modeled_demo",
    },
    "RB-084-SF-SEA": {
        "lifecycle_status": "retired",
        "decision_cycle": "2025_sugarfree_review",
        "season_year": 2025,
        "source_confidence": "modeled_demo",
    },
    "RB-120-SF-CUR": {
        "lifecycle_status": "retired",
        "decision_cycle": "2025_sugarfree_review",
        "season_year": 2025,
        "source_confidence": "modeled_demo",
    },
    "RB-120-CHSAK": {
        "lifecycle_status": "active",
        "decision_cycle": "2026_spring_review",
        "season_year": 2026,
        "source_confidence": "official_public_lineup",
    },
    "RB-120-SF-CHSAK": {
        "lifecycle_status": "active",
        "decision_cycle": "2026_spring_review",
        "season_year": 2026,
        "source_confidence": "official_public_lineup",
    },
    "RB-120-SUDACHI": {
        "lifecycle_status": "active",
        "decision_cycle": "2026_summer_review",
        "season_year": 2026,
        "source_confidence": "official_public_lineup",
    },
    "RB-120-SF-SUDACHI": {
        "lifecycle_status": "active",
        "decision_cycle": "2026_summer_review",
        "season_year": 2026,
        "source_confidence": "official_public_lineup",
    },
}


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def is_active_in_interval(product, start, end):
    planned_end = product["planned_end_date"] or date.max
    return product["launch_date"] < end and planned_end >= start


def should_emit(product, start, end):
    if start <= DEFAULT_START_DATE:
        return is_active_in_interval(product, start, end)
    return start <= product["launch_date"] < end


def materialize():
    start, end = get_interval()
    updated_at = datetime.combine(start, datetime.min.time())
    rows = []
    for product in PRODUCTS:
        if not should_emit(product, start, end):
            continue
        row = product.copy()
        row.update(DEFAULT_PRODUCT_META)
        row.update(PRODUCT_META.get(row["sku_id"], {}))
        row["updated_at"] = updated_at
        rows.append(row)
    return rows
