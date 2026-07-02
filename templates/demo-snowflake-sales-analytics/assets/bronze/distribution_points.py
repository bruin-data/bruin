"""@bruin

name: bronze.distribution_points
description: Daily SKU distribution and display compliance by store.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - distribution
domains:
  - sales
meta:
  asset_grain: One row per date, store, and SKU distribution observation extract record.
  source_system: Synthetic retail execution table stand-in.

materialization:
  type: table
  strategy: append
  cluster_by:
    - activity_date
    - sku_id

depends:
  - bronze.products
  - bronze.stores
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: distribution_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: activity_date
    type: DATE
  - name: store_id
    type: VARCHAR
  - name: sku_id
    type: VARCHAR
  - name: authorized
    type: BOOLEAN
  - name: in_distribution
    type: BOOLEAN
  - name: display_compliant
    type: BOOLEAN
  - name: shelf_facings
    type: INTEGER
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime, timedelta

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "grocery", 0.91, date(2024, 1, 1)),
    ("RTL-GLOBAL-MASS", "mass", 0.93, date(2024, 1, 1)),
    ("RTL-GLOBAL-CONVENIENCE", "convenience", 0.78, date(2024, 1, 1)),
    ("RTL-GLOBAL-CASH-CARRY", "club", 0.85, date(2024, 1, 1)),
    ("RTL-GLOBAL-ECOM", "ecommerce", 0.92, date(2024, 1, 1)),
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
    "North America": 1.04,
    "Latin America & Caribbean": 0.99,
    "Europe": 1.03,
    "Middle East & North Africa": 0.98,
    "Sub-Saharan Africa": 0.90,
    "South Asia": 0.92,
    "East Asia": 0.96,
    "Southeast Asia & Oceania": 1.00,
    "Central Asia": 0.86,
}

SKUS = [
    ("RB-084-ORG", date(1987, 4, 1), None, "core"),
    ("RB-120-ORG", date(1997, 1, 1), None, "core"),
    ("RB-084-SF", date(1999, 1, 1), None, "core"),
    ("RB-120-SF", date(2003, 1, 1), None, "core"),
    ("RB-120-ZERO", date(2022, 1, 1), None, "core"),
    ("RB-084-WM", date(2020, 3, 1), None, "edition"),
    ("RB-120-WM", date(2020, 3, 1), None, "edition"),
    ("RB-120-TROP", date(2014, 3, 1), None, "edition"),
    ("RB-120-AMBER", date(2022, 4, 1), None, "edition"),
    ("RB-120-SEA", date(2023, 6, 1), None, "edition"),
    ("RB-120-COCONUT", date(2018, 4, 1), None, "edition"),
    ("RB-120-PEACH", date(2019, 3, 1), None, "edition"),
    ("RB-120-CURUBA", date(2024, 4, 1), None, "new_edition"),
    ("RB-120-PINK", date(2025, 1, 15), None, "new_edition"),
    ("RB-120-BLUE", date(2012, 3, 1), date(2025, 12, 31), "retiring"),
    ("RB-120-DRG", date(2021, 4, 1), date(2024, 10, 31), "retiring"),
    ("RB-120-IVB", date(2024, 10, 15), None, "new_edition"),
    ("RB-120-WPCH", date(2025, 4, 1), date(2025, 9, 30), "seasonal"),
    ("RB-084-SF-SEA", date(2024, 3, 1), date(2025, 2, 28), "seasonal"),
    ("RB-120-SF-CUR", date(2024, 4, 1), date(2025, 9, 30), "seasonal"),
    ("RB-120-CHSAK", date(2026, 2, 23), date(2026, 6, 30), "seasonal"),
    ("RB-120-SF-CHSAK", date(2026, 2, 23), date(2026, 6, 30), "seasonal"),
    ("RB-120-SUDACHI", date(2026, 4, 1), date(2026, 9, 30), "seasonal"),
    ("RB-120-SF-SUDACHI", date(2026, 4, 1), date(2026, 9, 30), "seasonal"),
]


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
            rows.append((region, country, country, country[:3].upper(), REGION_WEIGHTS[region]))
    return rows


def is_active(activity_date, launch_date, planned_end_date):
    return launch_date <= activity_date and (planned_end_date is None or activity_date <= planned_end_date)


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


def sku_distribution_target(sku_type, activity_date, launch_date):
    if sku_type == "core":
        return 0.96
    if sku_type == "edition":
        return 0.84
    if sku_type == "new_edition":
        days_since_launch = (activity_date - launch_date).days
        return min(0.86, 0.42 + max(days_since_launch, 0) / 120)
    if sku_type == "seasonal":
        days_since_launch = (activity_date - launch_date).days
        return min(0.78, 0.34 + max(days_since_launch, 0) / 100)
    if sku_type == "retiring":
        return 0.55 if activity_date >= date(2024, 8, 1) else 0.72
    return 0.70


def materialize():
    start, end = get_interval()
    rows = []
    for retailer_index, (_retailer_id, channel, retailer_distribution, retailer_opened_date) in enumerate(RETAILERS, start=1):
        for market_index, (_region, country, market, _country_code, market_distribution) in enumerate(global_markets(), start=1):
            opened_date = retailer_opened_date
            store_id = f"STORE-{retailer_index:02d}-{market_index:03d}"
            for activity_date in daterange(start, end):
                if activity_date < opened_date:
                    continue
                updated_at = datetime.combine(activity_date, datetime.min.time())
                day_number = activity_date.timetuple().tm_yday
                for sku_index, (sku_id, launch_date, planned_end_date, sku_type) in enumerate(SKUS):
                    if not is_active(activity_date, launch_date, planned_end_date):
                        continue
                    authorized = is_in_assortment(channel, sku_id, market_index)
                    if not authorized:
                        continue
                    target = sku_distribution_target(sku_type, activity_date, launch_date)
                    score = ((retailer_index * 19 + market_index * 13 + sku_index * 11 + day_number) % 100) / 100
                    in_distribution = score <= min(0.98, target * retailer_distribution * market_distribution)
                    display_score = ((retailer_index * 7 + market_index * 5 + sku_index * 3 + day_number) % 100) / 100
                    display_compliant = in_distribution and display_score <= (0.80 if channel in ("mass", "grocery", "club") else 0.55)
                    if sku_type == "core":
                        shelf_facings = 4 if channel != "convenience" else 3
                    elif sku_type in ("new_edition", "seasonal"):
                        shelf_facings = 2 if in_distribution else 0
                    else:
                        shelf_facings = 1 if in_distribution else 0
                    rows.append(
                        {
                            "distribution_id": f"DIST-{activity_date:%Y%m%d}-{store_id}-{sku_id}",
                            "activity_date": activity_date,
                            "store_id": store_id,
                            "sku_id": sku_id,
                            "authorized": authorized,
                            "in_distribution": in_distribution,
                            "display_compliant": display_compliant,
                            "shelf_facings": shelf_facings,
                            "updated_at": updated_at,
                        }
                    )
    return rows
