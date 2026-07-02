"""@bruin

name: bronze.inventory_snapshots
description: Weekly SKU inventory snapshots by store.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - inventory
domains:
  - sales
meta:
  asset_grain: One row per week, store, and SKU extract record.
  source_system: Synthetic distributor or retailer inventory stand-in.

materialization:
  type: table
  strategy: append
  cluster_by:
    - snapshot_date
    - sku_id

depends:
  - bronze.products
  - bronze.stores
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: inventory_snapshot_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: snapshot_date
    type: DATE
  - name: store_id
    type: VARCHAR
  - name: sku_id
    type: VARCHAR
  - name: on_hand_units
    type: INTEGER
  - name: out_of_stock
    type: BOOLEAN
  - name: days_of_supply
    type: DOUBLE
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime, timedelta

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "grocery", 1.10, date(2024, 1, 1)),
    ("RTL-GLOBAL-MASS", "mass", 1.18, date(2024, 1, 1)),
    ("RTL-GLOBAL-CONVENIENCE", "convenience", 0.66, date(2024, 1, 1)),
    ("RTL-GLOBAL-CASH-CARRY", "club", 1.70, date(2024, 1, 1)),
    ("RTL-GLOBAL-ECOM", "ecommerce", 1.20, date(2024, 1, 1)),
]

COUNTRIES_BY_REGION = {
    "North America": "Canada|Mexico|United States",
    "Latin America & Caribbean": "Argentina|Bahamas|Barbados|Belize|Bolivia|Brazil|Chile|Colombia|Costa Rica|Cuba|Dominica|Dominican Republic|Ecuador|El Salvador|Grenada|Guatemala|Guyana|Haiti|Honduras|Jamaica|Nicaragua|Panama|Paraguay|Peru|Saint Kitts and Nevis|Saint Lucia|Saint Vincent and the Grenadines|Suriname|Trinidad and Tobago|Uruguay|Venezuela|Antigua and Barbuda",
    "Europe": "Albania|Andorra|Armenia|Austria|Azerbaijan|Belarus|Belgium|Bosnia and Herzegovina|Bulgaria|Croatia|Cyprus|Czechia|Denmark|Estonia|Finland|France|Georgia|Germany|Greece|Hungary|Iceland|Ireland|Italy|Kazakhstan|Kosovo|Latvia|Liechtenstein|Lithuania|Luxembourg|Malta|Moldova|Monaco|Montenegro|Netherlands|North Macedonia|Norway|Poland|Portugal|Romania|Serbia|Slovakia|Slovenia|Spain|Sweden|Switzerland|Turkey|Ukraine|United Kingdom",
    "Middle East & North Africa": "Algeria|Bahrain|Egypt|Iraq|Israel|Jordan|Kuwait|Lebanon|Libya|Morocco|Oman|Palestine|Qatar|Saudi Arabia|Tunisia|United Arab Emirates|Yemen",
    "Sub-Saharan Africa": "Angola|Benin|Botswana|Burkina Faso|Burundi|Cameroon|Cape Verde|Chad|Congo|Cote d Ivoire|Democratic Republic of the Congo|Djibouti|Equatorial Guinea|Ethiopia|Gabon|Gambia|Ghana|Guinea|Kenya|Lesotho|Liberia|Madagascar|Malawi|Mali|Mauritania|Mauritius|Mozambique|Namibia|Niger|Nigeria|Rwanda|Senegal|Seychelles|Sierra Leone|South Africa|Tanzania|Togo|Uganda|Zambia|Zimbabwe|Eswatini",
    "South Asia": "Bangladesh|Bhutan|India|Maldives|Nepal|Pakistan|Sri Lanka",
    "East Asia": "China|Hong Kong|Japan|Macau|Mongolia|South Korea|Taiwan",
    "Southeast Asia & Oceania": "Australia|Brunei|Cambodia|Fiji|Indonesia|Laos|Malaysia|Myanmar|New Zealand|Papua New Guinea|Philippines|Singapore|Thailand|Timor-Leste|Vietnam|Samoa|Solomon Islands|Tonga|Vanuatu",
    "Central Asia": "Kyrgyzstan|Tajikistan|Turkmenistan|Uzbekistan",
}

REGION_WEIGHTS = {
    "North America": 1.08,
    "Latin America & Caribbean": 1.02,
    "Europe": 1.06,
    "Middle East & North Africa": 1.00,
    "Sub-Saharan Africa": 0.88,
    "South Asia": 0.92,
    "East Asia": 0.96,
    "Southeast Asia & Oceania": 1.04,
    "Central Asia": 0.84,
}

SKUS = [
    ("RB-084-ORG", 42, date(1987, 4, 1), None),
    ("RB-120-ORG", 52, date(1997, 1, 1), None),
    ("RB-084-SF", 24, date(1999, 1, 1), None),
    ("RB-120-SF", 26, date(2003, 1, 1), None),
    ("RB-120-ZERO", 18, date(2022, 1, 1), None),
    ("RB-084-WM", 22, date(2020, 3, 1), None),
    ("RB-120-WM", 25, date(2020, 3, 1), None),
    ("RB-120-TROP", 23, date(2014, 3, 1), None),
    ("RB-120-AMBER", 22, date(2022, 4, 1), None),
    ("RB-120-SEA", 25, date(2023, 6, 1), None),
    ("RB-120-COCONUT", 18, date(2018, 4, 1), None),
    ("RB-120-PEACH", 20, date(2019, 3, 1), None),
    ("RB-120-CURUBA", 24, date(2024, 4, 1), None),
    ("RB-120-PINK", 22, date(2025, 1, 15), None),
    ("RB-120-BLUE", 14, date(2012, 3, 1), date(2025, 12, 31)),
    ("RB-120-DRG", 12, date(2021, 4, 1), date(2024, 10, 31)),
    ("RB-120-IVB", 20, date(2024, 10, 15), None),
    ("RB-120-WPCH", 20, date(2025, 4, 1), date(2025, 9, 30)),
    ("RB-084-SF-SEA", 14, date(2024, 3, 1), date(2025, 2, 28)),
    ("RB-120-SF-CUR", 16, date(2024, 4, 1), date(2025, 9, 30)),
    ("RB-120-CHSAK", 20, date(2026, 2, 23), date(2026, 6, 30)),
    ("RB-120-SF-CHSAK", 15, date(2026, 2, 23), date(2026, 6, 30)),
    ("RB-120-SUDACHI", 22, date(2026, 4, 1), date(2026, 9, 30)),
    ("RB-120-SF-SUDACHI", 16, date(2026, 4, 1), date(2026, 9, 30)),
]


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def weekly_dates(start, end):
    current = DEFAULT_START_DATE if start <= DEFAULT_START_DATE else start + timedelta(days=(7 - start.weekday()) % 7)
    while current < end:
        yield current
        current += timedelta(days=7)


def global_markets():
    rows = []
    for region, countries in COUNTRIES_BY_REGION.items():
        for country in countries.split("|"):
            rows.append((region, country, country, country[:3].upper(), REGION_WEIGHTS[region]))
    return rows


def is_active(snapshot_date, launch_date, planned_end_date):
    return launch_date <= snapshot_date and (planned_end_date is None or snapshot_date <= planned_end_date)


def is_in_assortment(channel, sku_id, market_index):
    if channel == "club" and not sku_id.startswith("RB-120"):
        return False
    if channel == "ecommerce" and sku_id.startswith("RB-084"):
        return False
    if sku_id in {"RB-084-ORG", "RB-120-ORG", "RB-084-SF", "RB-120-SF"}:
        return channel != "club" or sku_id.startswith("RB-120")
    if sku_id == "RB-120-ZERO":
        return channel in {"grocery", "mass", "ecommerce"} and market_index % 2 == 0
    if sku_id in {"RB-120-WM", "RB-120-TROP", "RB-120-SEA", "RB-120-CURUBA"}:
        return (market_index + len(sku_id)) % 3 != 0
    if sku_id in {"RB-120-AMBER", "RB-120-COCONUT", "RB-120-PEACH", "RB-120-PINK"}:
        return channel in {"grocery", "mass", "ecommerce"} and (market_index + len(sku_id)) % 4 == 0
    if sku_id in {"RB-120-BLUE", "RB-120-DRG"}:
        return channel in {"grocery", "mass", "ecommerce"} and market_index % 5 in {0, 2}
    if sku_id in {"RB-120-IVB", "RB-120-WPCH", "RB-120-SF-CUR", "RB-120-CHSAK", "RB-120-SUDACHI"}:
        return channel in {"grocery", "mass", "ecommerce"} and market_index % 4 in {1, 3}
    if sku_id in {"RB-084-SF-SEA", "RB-120-SF-CHSAK", "RB-120-SF-SUDACHI"}:
        return channel in {"grocery", "mass"} and market_index % 4 == 2
    return True


def materialize():
    start, end = get_interval()
    rows = []
    for retailer_index, (_retailer_id, channel, retailer_inventory, retailer_opened_date) in enumerate(RETAILERS, start=1):
        for market_index, (_region, country, market, _country_code, market_inventory) in enumerate(global_markets(), start=1):
            opened_date = retailer_opened_date
            store_id = f"STORE-{retailer_index:02d}-{market_index:03d}"
            for week_number, snapshot_date in enumerate(weekly_dates(start, end), start=max(0, (start - DEFAULT_START_DATE).days // 7)):
                if snapshot_date < opened_date:
                    continue
                updated_at = datetime.combine(snapshot_date, datetime.min.time())
                for sku_index, (sku_id, base_units, launch_date, planned_end_date) in enumerate(SKUS):
                    if not is_active(snapshot_date, launch_date, planned_end_date):
                        continue
                    if not is_in_assortment(channel, sku_id, market_index):
                        continue
                    seasonal = 1.18 if snapshot_date.month in (5, 6, 7, 8) else 1.0
                    if sku_id == "RB-120-IVB" and snapshot_date.month in (11, 12, 1):
                        seasonal = 1.25
                    if sku_id == "RB-120-CHSAK" and snapshot_date.month in (3, 4, 5):
                        seasonal = 1.20
                    if sku_id == "RB-120-SUDACHI" and snapshot_date.month in (5, 6, 7, 8):
                        seasonal = 1.24
                    if sku_id in {"RB-120-DRG", "RB-120-BLUE"} and planned_end_date and snapshot_date >= planned_end_date - timedelta(days=75):
                        seasonal *= 0.45
                    noise = ((retailer_index * 23 + market_index * 17 + sku_index * 5 + week_number) % 19) - 9
                    on_hand_units = max(0, int((base_units + noise) * retailer_inventory * market_inventory * seasonal))
                    stockout_threshold = 5 if channel == "convenience" else 8
                    out_of_stock = on_hand_units <= stockout_threshold
                    rows.append(
                        {
                            "inventory_snapshot_id": f"INV-{snapshot_date:%Y%m%d}-{store_id}-{sku_id}",
                            "snapshot_date": snapshot_date,
                            "store_id": store_id,
                            "sku_id": sku_id,
                            "on_hand_units": on_hand_units,
                            "out_of_stock": out_of_stock,
                            "days_of_supply": round(on_hand_units / max(1.0, base_units / 7), 1),
                            "updated_at": updated_at,
                        }
                    )
    return rows
