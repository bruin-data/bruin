"""@bruin

name: bronze.sku_market_availability
description: |
  Country-level SKU launch and retirement windows for the Energy Drink Off Premise
  demo. This keeps product identity separate from market rollout timing so
  global seasonal products do not pretend to launch everywhere on the same day.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - product_catalog
  - market_availability
domains:
  - sales
meta:
  asset_grain: One row per SKU and country availability extract record.
  pipeline_role: Bronze demo source table for country-level product rollout windows.
  source_system: Synthetic product availability stand-in.

materialization:
  type: table
  strategy: append

depends:
  - bronze.products
  - bronze.stores
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: availability_id
    type: VARCHAR
    description: Stable SKU-country availability identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: sku_id
    type: VARCHAR
    description: SKU identifier.
  - name: country
    type: VARCHAR
    description: Country market.
  - name: region
    type: VARCHAR
    description: Commercial region.
  - name: market_launch_date
    type: DATE
    description: Modeled first availability date in the country.
  - name: market_end_date
    type: DATE
    description: Modeled last availability date in the country when applicable.
  - name: availability_status
    type: VARCHAR
    description: active, phaseout, retired, or planned.
  - name: rollout_tier
    type: VARCHAR
    description: Modeled global rollout tier for launch staggering.
  - name: source_confidence
    type: VARCHAR
    description: Whether the metadata is based on official public product pages or modeled demo assumptions.
  - name: updated_at
    type: TIMESTAMP
    description: Source extract timestamp.

@bruin"""

import os
from datetime import date, datetime, timedelta

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

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

SKU_WINDOWS = [
    ("RB-120-BLUE", date(2012, 3, 1), date(2025, 12, 31), "phaseout", "modeled_demo"),
    ("RB-120-DRG", date(2021, 4, 1), date(2024, 10, 31), "retired", "modeled_demo"),
    ("RB-120-IVB", date(2024, 10, 15), None, "made_permanent", "official_public_lineup"),
    ("RB-120-WPCH", date(2025, 4, 1), date(2025, 9, 30), "retired", "modeled_demo"),
    ("RB-084-SF-SEA", date(2024, 3, 1), date(2025, 2, 28), "retired", "modeled_demo"),
    ("RB-120-SF-CUR", date(2024, 4, 1), date(2025, 9, 30), "retired", "modeled_demo"),
    ("RB-120-CHSAK", date(2026, 2, 23), date(2026, 6, 30), "active", "official_public_lineup"),
    ("RB-120-SF-CHSAK", date(2026, 2, 23), date(2026, 6, 30), "active", "official_public_lineup"),
    ("RB-120-SUDACHI", date(2026, 4, 1), date(2026, 9, 30), "active", "official_public_lineup"),
    ("RB-120-SF-SUDACHI", date(2026, 4, 1), date(2026, 9, 30), "active", "official_public_lineup"),
]


def get_interval():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", DEFAULT_START_DATE.isoformat()))
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", DEFAULT_END_DATE.isoformat()))
    if end <= start:
        return start, start
    return max(start, DEFAULT_START_DATE), min(end, DEFAULT_END_DATE)


def global_markets():
    rows = []
    for region, countries in COUNTRIES_BY_REGION.items():
        for country in countries.split("|"):
            rows.append((region, country))
    return rows


def rollout_tier(region, country_index):
    if region in {"North America", "Europe", "Southeast Asia & Oceania"}:
        return "tier_1"
    if region in {"Latin America & Caribbean", "East Asia", "Middle East & North Africa"}:
        return "tier_2" if country_index % 3 else "tier_1"
    return "tier_3" if country_index % 2 else "tier_2"


def launch_offset_days(tier):
    return {"tier_1": 0, "tier_2": 21, "tier_3": 42}[tier]


def should_emit(launch_date, end_date, interval_start, interval_end):
    if interval_start <= DEFAULT_START_DATE:
        return launch_date < interval_end and (end_date or date.max) >= interval_start
    return interval_start <= launch_date < interval_end


def materialize():
    interval_start, interval_end = get_interval()
    updated_at = datetime.combine(interval_start, datetime.min.time())
    rows = []
    for market_index, (region, country) in enumerate(global_markets(), start=1):
        tier = rollout_tier(region, market_index)
        offset = launch_offset_days(tier)
        for sku_id, launch_date, end_date, status, source_confidence in SKU_WINDOWS:
            market_launch_date = launch_date + timedelta(days=offset)
            market_end_date = end_date + timedelta(days=offset) if end_date else None
            if not should_emit(market_launch_date, market_end_date, interval_start, interval_end):
                continue
            rows.append(
                {
                    "availability_id": f"AVAIL-{sku_id}-{country.replace(' ', '_').upper()}",
                    "sku_id": sku_id,
                    "country": country,
                    "region": region,
                    "market_launch_date": market_launch_date,
                    "market_end_date": market_end_date,
                    "availability_status": status,
                    "rollout_tier": tier,
                    "source_confidence": source_confidence,
                    "updated_at": updated_at,
                }
            )
    return rows
