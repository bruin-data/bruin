"""@bruin

name: bronze.sales_transactions
description: Daily source-shaped Off Premise sales transactions by store and SKU.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - sales
domains:
  - sales
meta:
  asset_grain: One row per date, store, and SKU transaction summary extract record.
  source_system: Synthetic internal sales database stand-in.

materialization:
  type: table
  strategy: append
  cluster_by:
    - sales_date
    - sku_id

depends:
  - bronze.products
  - bronze.stores
  - bronze.product_costs
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: transaction_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: sales_date
    type: DATE
  - name: store_id
    type: VARCHAR
  - name: sku_id
    type: VARCHAR
  - name: units_sold
    type: INTEGER
    checks:
      - name: non_negative
  - name: gross_sales_usd
    type: DOUBLE
    checks:
      - name: non_negative
  - name: discount_usd
    type: DOUBLE
    checks:
      - name: non_negative
  - name: net_sales_usd
    type: DOUBLE
    checks:
      - name: non_negative
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime, timedelta

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "grocery", 1.10, date(2024, 1, 1)),
    ("RTL-GLOBAL-MASS", "mass", 1.16, date(2024, 1, 1)),
    ("RTL-GLOBAL-CONVENIENCE", "convenience", 0.86, date(2024, 1, 1)),
    ("RTL-GLOBAL-CASH-CARRY", "club", 1.32, date(2024, 1, 1)),
    ("RTL-GLOBAL-ECOM", "ecommerce", 0.72, date(2024, 1, 1)),
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
    "North America": 1.20,
    "Latin America & Caribbean": 1.04,
    "Europe": 1.14,
    "Middle East & North Africa": 1.02,
    "Sub-Saharan Africa": 0.82,
    "South Asia": 0.96,
    "East Asia": 1.00,
    "Southeast Asia & Oceania": 1.08,
    "Central Asia": 0.78,
}

COUNTRY_BOOSTS = {
    "United States": 1.25,
    "Canada": 1.10,
    "Mexico": 1.08,
    "Brazil": 1.16,
    "Austria": 1.18,
    "Germany": 1.20,
    "United Kingdom": 1.18,
    "France": 1.12,
    "Spain": 1.12,
    "Italy": 1.10,
    "Japan": 1.12,
    "Australia": 1.14,
    "Thailand": 1.12,
    "United Arab Emirates": 1.16,
    "South Africa": 1.08,
    "India": 1.10,
}

SKUS = [
    ("RB-084-ORG", 2.79, 15.0, date(1987, 4, 1), None),
    ("RB-120-ORG", 3.29, 18.0, date(1997, 1, 1), None),
    ("RB-084-SF", 2.99, 7.5, date(1999, 1, 1), None),
    ("RB-120-SF", 3.49, 8.5, date(2003, 1, 1), None),
    ("RB-120-ZERO", 3.49, 5.8, date(2022, 1, 1), None),
    ("RB-084-WM", 2.99, 7.8, date(2020, 3, 1), None),
    ("RB-120-WM", 3.59, 8.4, date(2020, 3, 1), None),
    ("RB-120-TROP", 3.59, 7.6, date(2014, 3, 1), None),
    ("RB-120-AMBER", 3.59, 6.9, date(2022, 4, 1), None),
    ("RB-120-SEA", 3.69, 8.1, date(2023, 6, 1), None),
    ("RB-120-COCONUT", 3.59, 6.1, date(2018, 4, 1), None),
    ("RB-120-PEACH", 3.59, 6.4, date(2019, 3, 1), None),
    ("RB-120-CURUBA", 3.69, 7.2, date(2024, 4, 1), None),
    ("RB-120-PINK", 3.69, 6.7, date(2025, 1, 15), None),
    ("RB-120-BLUE", 3.59, 4.8, date(2012, 3, 1), date(2025, 12, 31)),
    ("RB-120-DRG", 3.69, 5.2, date(2021, 4, 1), date(2024, 10, 31)),
    ("RB-120-IVB", 3.79, 7.7, date(2024, 10, 15), None),
    ("RB-120-WPCH", 3.79, 7.9, date(2025, 4, 1), date(2025, 9, 30)),
    ("RB-084-SF-SEA", 3.09, 4.2, date(2024, 3, 1), date(2025, 2, 28)),
    ("RB-120-SF-CUR", 3.79, 4.6, date(2024, 4, 1), date(2025, 9, 30)),
    ("RB-120-CHSAK", 3.89, 8.3, date(2026, 2, 23), date(2026, 6, 30)),
    ("RB-120-SF-CHSAK", 3.89, 4.9, date(2026, 2, 23), date(2026, 6, 30)),
    ("RB-120-SUDACHI", 3.89, 8.8, date(2026, 4, 1), date(2026, 9, 30)),
    ("RB-120-SF-SUDACHI", 3.89, 5.2, date(2026, 4, 1), date(2026, 9, 30)),
]

CHANNEL_MULTIPLIERS = {
    "grocery": 1.10,
    "mass": 1.18,
    "club": 1.45,
    "convenience": 0.82,
    "ecommerce": 0.70,
}


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def daterange(start, end):
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)


def global_markets():
    rows = []
    for region, countries in COUNTRIES_BY_REGION.items():
        for country in countries.split("|"):
            weight = REGION_WEIGHTS[region] * COUNTRY_BOOSTS.get(country, 1.0)
            rows.append((region, country, country, country[:3].upper(), weight))
    return rows


def is_active(sales_date, launch_date, planned_end_date):
    return launch_date <= sales_date and (planned_end_date is None or sales_date <= planned_end_date)


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


def limited_sku_boost(sku_id, channel, market, sales_date):
    if sku_id == "RB-120-CURUBA":
        return 1.25 if channel in ("grocery", "mass") and sales_date.month in (5, 6, 7, 8) else 1.02
    if sku_id == "RB-120-PINK":
        return 1.18 if channel in ("grocery", "ecommerce") else 0.92
    if sku_id == "RB-120-IVB":
        return 1.30 if sales_date.month in (11, 12, 1) else 0.90
    if sku_id == "RB-120-WPCH":
        return 1.28 if sales_date.month in (5, 6, 7, 8) else 0.95
    if sku_id == "RB-120-CHSAK":
        return 1.22 if channel in ("grocery", "mass", "ecommerce") and sales_date.month in (3, 4, 5) else 0.98
    if sku_id == "RB-120-SUDACHI":
        return 1.34 if channel in ("grocery", "mass", "ecommerce") and sales_date.month in (5, 6, 7, 8) else 1.02
    if sku_id in {"RB-120-SF-CHSAK", "RB-120-SF-SUDACHI"}:
        return 1.18 if channel in ("grocery", "mass", "ecommerce") else 0.88
    if sku_id == "RB-120-DRG":
        return 0.72 if sales_date >= date(2024, 8, 1) else 0.95
    if sku_id == "RB-120-BLUE":
        return 0.76 if sales_date >= date(2025, 6, 1) else 0.94
    if sku_id in {"RB-084-SF-SEA", "RB-120-SF-CUR"}:
        return 1.15 if channel in ("grocery", "mass", "ecommerce") else 0.86
    if market in {"Miami", "Los Angeles", "Phoenix"} and sku_id in {"RB-120-WM", "RB-120-TROP"}:
        return 1.10
    return 1.0


def materialize():
    start, end = get_interval()
    rows = []
    for retailer_index, (_retailer_id, channel, retailer_weight, retailer_opened_date) in enumerate(RETAILERS, start=1):
        for market_index, (_region, country, market, _country_code, market_weight) in enumerate(global_markets(), start=1):
            opened_date = retailer_opened_date
            store_id = f"STORE-{retailer_index:02d}-{market_index:03d}"
            for sales_date in daterange(start, end):
                if sales_date < opened_date:
                    continue
                updated_at = datetime.combine(sales_date, datetime.min.time())
                weekend_factor = 1.22 if sales_date.weekday() in (4, 5) else 1.0
                summer_factor = 1.18 if sales_date.month in (5, 6, 7, 8) else 1.0
                new_year_factor = 1.12 if sales_date.month == 1 and channel in ("grocery", "mass") else 1.0
                for sku_index, (sku_id, list_price, base_units, launch_date, planned_end_date) in enumerate(SKUS):
                    if not is_active(sales_date, launch_date, planned_end_date):
                        continue
                    if not is_in_assortment(channel, sku_id, market_index):
                        continue
                    noise = ((retailer_index * 17 + market_index * 11 + sku_index * 7 + sales_date.timetuple().tm_yday) % 13) - 6
                    boost = limited_sku_boost(sku_id, channel, market, sales_date)
                    units = max(
                        0,
                        int(
                            (base_units + noise)
                            * CHANNEL_MULTIPLIERS[channel]
                            * retailer_weight
                            * market_weight
                            * weekend_factor
                            * summer_factor
                            * new_year_factor
                            * boost
                        ),
                    )
                    discount_pct = 0.0
                    if 0 <= (sales_date - launch_date).days <= 42 and sku_id.startswith(
                        (
                            "RB-120-CURUBA",
                            "RB-120-IVB",
                            "RB-120-WPCH",
                            "RB-120-PINK",
                            "RB-120-CHSAK",
                            "RB-120-SF-CHSAK",
                            "RB-120-SUDACHI",
                            "RB-120-SF-SUDACHI",
                        )
                    ):
                        discount_pct = 0.08
                    elif channel == "club" and sales_date.day <= 7:
                        discount_pct = 0.07
                    elif sales_date.month == 9 and channel in ("mass", "convenience") and sku_id == "RB-120-ORG":
                        discount_pct = 0.05
                    gross = round(units * list_price, 2)
                    discount = round(gross * discount_pct, 2)
                    rows.append(
                        {
                            "transaction_id": f"SALE-{sales_date:%Y%m%d}-{store_id}-{sku_id}",
                            "sales_date": sales_date,
                            "store_id": store_id,
                            "sku_id": sku_id,
                            "units_sold": units,
                            "gross_sales_usd": gross,
                            "discount_usd": discount,
                            "net_sales_usd": round(gross - discount, 2),
                            "updated_at": updated_at,
                        }
                    )
    return rows
