"""@bruin

name: bronze.stores
description: Store and country-market master for Off Premise sales analysis.
connection: snowflake-default
tags:
  - energy_drink
  - bronze
  - stores
domains:
  - sales
meta:
  asset_grain: One row per retailer channel and country market extract record.
  source_system: Synthetic internal store master stand-in.

materialization:
  type: table
  strategy: append

depends:
  - bronze.retailers
image: python:3.11

parameters:
  enforce_schema: true

columns:
  - name: store_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
  - name: retailer_id
    type: VARCHAR
  - name: store_name
    type: VARCHAR
  - name: country
    type: VARCHAR
  - name: region
    type: VARCHAR
  - name: market
    type: VARCHAR
  - name: state
    type: VARCHAR
  - name: channel
    type: VARCHAR
  - name: updated_at
    type: TIMESTAMP

@bruin"""

import os
from datetime import date, datetime

DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2026, 7, 1)

RETAILERS = [
    ("RTL-GLOBAL-GROCERY", "Modern Grocery", "grocery", date(2024, 1, 1)),
    ("RTL-GLOBAL-MASS", "Mass & Hypermarket", "mass", date(2024, 1, 1)),
    ("RTL-GLOBAL-CONVENIENCE", "Convenience & Fuel", "convenience", date(2024, 1, 1)),
    ("RTL-GLOBAL-CASH-CARRY", "Cash & Carry / Club", "club", date(2024, 1, 1)),
    ("RTL-GLOBAL-ECOM", "Digital Commerce", "ecommerce", date(2024, 1, 1)),
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


def global_markets():
    rows = []
    for region, countries in COUNTRIES_BY_REGION.items():
        for country in countries.split("|"):
            rows.append((region, country, country, country[:3].upper()))
    return rows


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
    for r_index, (retailer_id, retailer_name, channel, retailer_opened_date) in enumerate(RETAILERS, start=1):
        for m_index, (region, country, market, country_code) in enumerate(global_markets(), start=1):
            if not should_emit(retailer_opened_date, start, end):
                continue
            rows.append(
                {
                    "store_id": f"STORE-{r_index:02d}-{m_index:03d}",
                    "retailer_id": retailer_id,
                    "store_name": f"{retailer_name} {country}",
                    "country": country,
                    "region": region,
                    "market": market,
                    "state": country_code,
                    "channel": channel,
                    "updated_at": updated_at,
                }
            )
    return rows
