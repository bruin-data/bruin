"""@bruin

name: bronze.seed_salesforce_demo_data
description: |
  Generates deterministic, realistic credit union demo CRM records for the Bruin
  interval and upserts them into Salesforce standard objects. This asset has no
  Python materialization; it writes to Salesforce only and logs per-object
  create/update counts for ingestion verification.
tags:
  - finance
  - credit_union
  - crm
  - salesforce
  - seed
  - daily_batch
domains:
  - crm
  - member_relationships
meta:
  asset_grain: No warehouse table; side-effect asset that upserts Salesforce demo records for the Bruin interval.
  full_refresh_behavior: Historical full refreshes generate every business date in the Bruin interval.
  load_pattern: Deterministic interval-driven Salesforce upsert by natural keys.
  pipeline_role: Source-system demo data generator.
  refresh_cadence: Daily batch pipeline.
  source_system: Synthetic credit union demo generator.
  storage_limit_behavior: Updates existing demo rows and skips new creates when Salesforce reports storage exhaustion; set CREDIT_UNION_FAIL_ON_STORAGE_LIMIT=1 for strict failure.
  target_system: Salesforce Sales Cloud
image: python:3.11

secrets:
  - key: salesforce
    inject_as: SALESFORCE_CONNECTION

@bruin"""

import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from simple_salesforce import Salesforce

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEMO_START_DATE = date(2015, 1, 1)
DEFAULT_ACCOUNTS_PER_DAY = 1
DEFAULT_CONTACTS_PER_ACCOUNT = 1
DEFAULT_OPPORTUNITIES_PER_ACCOUNT = 1
DEFAULT_TASKS_PER_OPPORTUNITY = 1
DEFAULT_LEADS_PER_DAY = 1
DEFAULT_EVENTS_PER_OPPORTUNITY = 1

BRANCH_MARKETS = [
    {
        "branch": "Dublin",
        "city": "Dublin",
        "county": "Alameda",
        "area": "Tri-Valley",
        "phone_area": "925",
    },
    {
        "branch": "Fremont",
        "city": "Fremont",
        "county": "Alameda",
        "area": "East Bay",
        "phone_area": "510",
    },
    {
        "branch": "Walnut Creek",
        "city": "Walnut Creek",
        "county": "Contra Costa",
        "area": "Contra Costa",
        "phone_area": "925",
    },
    {
        "branch": "San Jose",
        "city": "San Jose",
        "county": "Santa Clara",
        "area": "South Bay",
        "phone_area": "408",
    },
    {
        "branch": "Sacramento",
        "city": "Sacramento",
        "county": "Sacramento",
        "area": "Sacramento",
        "phone_area": "916",
    },
    {
        "branch": "Stockton",
        "city": "Stockton",
        "county": "San Joaquin",
        "area": "Central Valley",
        "phone_area": "209",
    },
    {
        "branch": "San Mateo",
        "city": "San Mateo",
        "county": "San Mateo",
        "area": "Peninsula",
        "phone_area": "650",
    },
    {
        "branch": "San Diego",
        "city": "San Diego",
        "county": "San Diego",
        "area": "Southern California",
        "phone_area": "619",
    },
]

ACCOUNT_PROFILES = [
    ("Household", "Customer - Direct", "Banking"),
    ("Small Business", "Customer - Direct", "Finance"),
    ("Community Nonprofit", "Customer - Direct", "Education"),
    ("Employer Group", "Customer - Direct", "Healthcare"),
    ("Indirect Dealer Partner", "Channel Partner", "Automotive"),
    ("Prospective Member", "Prospect", "Banking"),
]

CONTACT_ROLES = [
    "Primary Member",
    "Joint Owner",
    "Authorized User",
    "Business Principal",
    "Trustee",
]

FIRST_NAMES = [
    "Maya",
    "Jordan",
    "Avery",
    "Sofia",
    "Noah",
    "Isabella",
    "Ethan",
    "Amara",
    "Lucas",
    "Priya",
    "Mateo",
    "Camila",
    "Arjun",
    "Leila",
    "Diego",
    "Nina",
]

LAST_NAMES = [
    "Patel",
    "Nguyen",
    "Garcia",
    "Johnson",
    "Kim",
    "Martinez",
    "Singh",
    "Chen",
    "Robinson",
    "Lopez",
    "Shah",
    "Rivera",
    "Tran",
    "Miller",
    "Kaur",
    "Davis",
]

PRODUCTS = [
    {
        "code": "AUTO-REFI",
        "name": "Auto Loan Refinance",
        "family": "Auto lending",
        "base_amount": 28_000,
        "term_months": 72,
        "lead_source": "Phone Inquiry",
    },
    {
        "code": "HELOC",
        "name": "Home Equity Line of Credit",
        "family": "Home lending",
        "base_amount": 95_000,
        "term_months": 120,
        "lead_source": "Web",
    },
    {
        "code": "CARD-BT",
        "name": "Credit Card Balance Transfer",
        "family": "Cards",
        "base_amount": 9_500,
        "term_months": 18,
        "lead_source": "Other",
    },
    {
        "code": "BUS-CHK",
        "name": "Business Checking Expansion",
        "family": "Business banking",
        "base_amount": 35_000,
        "term_months": 36,
        "lead_source": "Partner Referral",
    },
    {
        "code": "MTG-PRE",
        "name": "Mortgage Preapproval",
        "family": "Home lending",
        "base_amount": 640_000,
        "term_months": 360,
        "lead_source": "Web",
    },
    {
        "code": "WELLNESS",
        "name": "Financial Wellness Package",
        "family": "Financial wellness",
        "base_amount": 4_000,
        "term_months": 12,
        "lead_source": "Other",
    },
    {
        "code": "CD-LADDER",
        "name": "Certificate Ladder",
        "family": "Deposits",
        "base_amount": 45_000,
        "term_months": 24,
        "lead_source": "Phone Inquiry",
    },
    {
        "code": "PERS-LOAN",
        "name": "Debt Consolidation Personal Loan",
        "family": "Consumer lending",
        "base_amount": 18_000,
        "term_months": 48,
        "lead_source": "Branch Referral",
    },
    {
        "code": "NEW-MEMBER",
        "name": "New Member Checking Bundle",
        "family": "Deposits",
        "base_amount": 2_500,
        "term_months": 1,
        "lead_source": "Community Event",
    },
]

CAMPAIGN_THEMES = [
    ("Auto Refinance Outreach", "Email", "Auto lending"),
    ("First-Time Homebuyer Webinar", "Webinar", "Home lending"),
    ("Community Financial Wellness", "Community Event", "Financial wellness"),
    ("Member Credit Card Review", "Direct Mail", "Cards"),
    ("Small Business Banking Clinic", "Referral Program", "Business banking"),
    ("Certificate Rate Renewal", "Email", "Deposits"),
]

STAGES = [
    ("Prospecting", 10),
    ("Qualification", 20),
    ("Needs Analysis", 35),
    ("Proposal/Price Quote", 55),
    ("Negotiation/Review", 75),
    ("Closed Won", 100),
    ("Closed Lost", 0),
]

TASK_TEMPLATES = [
    ("Call", "Confirm member goals and preferred next step"),
    ("Email", "Send checklist for required documents"),
    ("Meeting", "Schedule branch or video consultation"),
    ("Other", "Review underwriting or banker notes"),
]


@dataclass(frozen=True)
class SeedConfig:
    start_date: date
    end_date: date
    accounts_per_day: int
    contacts_per_account: int
    opportunities_per_account: int
    tasks_per_opportunity: int
    leads_per_day: int
    events_per_opportunity: int
    full_refresh: bool
    dry_run: bool


def _parse_date(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _env_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _config() -> SeedConfig:
    today = datetime.now(timezone.utc).date()
    start_date = max(_parse_date(os.environ.get("BRUIN_START_DATE"), today), DEMO_START_DATE)
    raw_end_date = _parse_date(os.environ.get("BRUIN_END_DATE"), start_date)
    end_date = raw_end_date - timedelta(days=1) if raw_end_date > start_date else raw_end_date
    if end_date < start_date:
        raise ValueError(f"BRUIN_END_DATE {end_date} is before BRUIN_START_DATE {start_date}")

    return SeedConfig(
        start_date=start_date,
        end_date=end_date,
        accounts_per_day=_env_int("CREDIT_UNION_ACCOUNTS_PER_DAY", DEFAULT_ACCOUNTS_PER_DAY),
        contacts_per_account=_env_int("CREDIT_UNION_CONTACTS_PER_ACCOUNT", DEFAULT_CONTACTS_PER_ACCOUNT),
        opportunities_per_account=_env_int(
            "CREDIT_UNION_OPPORTUNITIES_PER_ACCOUNT",
            DEFAULT_OPPORTUNITIES_PER_ACCOUNT,
        ),
        tasks_per_opportunity=_env_int("CREDIT_UNION_TASKS_PER_OPPORTUNITY", DEFAULT_TASKS_PER_OPPORTUNITY),
        leads_per_day=_env_int("CREDIT_UNION_LEADS_PER_DAY", DEFAULT_LEADS_PER_DAY),
        events_per_opportunity=_env_int("CREDIT_UNION_EVENTS_PER_OPPORTUNITY", DEFAULT_EVENTS_PER_OPPORTUNITY),
        full_refresh=os.environ.get("BRUIN_FULL_REFRESH") == "1",
        dry_run=os.environ.get("CREDIT_UNION_DRY_RUN", "").lower() in {"1", "true", "yes"},
    )


def _salesforce() -> Salesforce:
    raw = os.environ["SALESFORCE_CONNECTION"]
    conn = json.loads(raw)
    domain = conn.get("domain") or "login"

    if conn.get("access_token"):
        instance_url = _salesforce_base_url(domain)
        return Salesforce(instance_url=instance_url, session_id=conn["access_token"])

    if conn.get("client_id") and conn.get("client_secret"):
        instance_url = _salesforce_base_url(domain)
        response = requests.post(
            f"{instance_url}/services/oauth2/token",
            data={
                "grant_type": conn.get("grant_type", "client_credentials"),
                "client_id": conn["client_id"],
                "client_secret": conn["client_secret"],
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return Salesforce(instance_url=payload["instance_url"], session_id=payload["access_token"])

    username = conn["username"]
    password = conn["password"]
    token = conn["token"]

    if domain.startswith("http"):
        parsed = urlparse(domain)
        hostname = parsed.netloc
        candidates = [hostname]
        if hostname.endswith(".my.salesforce.com"):
            candidates.append(hostname[: -len(".my.salesforce.com")])
        if hostname.endswith(".salesforce.com"):
            candidates.append(hostname[: -len(".salesforce.com")])
        candidates.extend(["login", "test"])
    else:
        candidates = [domain, "login", "test"]

    last_error: Exception | None = None
    for candidate in dict.fromkeys(candidates):
        try:
            return Salesforce(
                username=username,
                password=password,
                security_token=token,
                domain=candidate,
            )
        except Exception as exc:
            last_error = exc
            logger.warning("Salesforce login failed for configured domain candidate %s", candidate)

    raise last_error or RuntimeError("Salesforce login failed")


def _salesforce_base_url(domain: str) -> str:
    domain = domain.rstrip("/")
    if domain.startswith(("http://", "https://")):
        return domain
    if domain.endswith(".salesforce.com"):
        return f"https://{domain}"
    return f"https://{domain}.salesforce.com"


def _date_range(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(days)]


def _seed_dates(config: SeedConfig) -> list[date]:
    return _date_range(config.start_date, config.end_date)


def _rng_for(day: date, salt: str) -> random.Random:
    return random.Random(f"credit-union-salesforce-demo-{day:%Y%m%d}-{salt}")


def _business_day_from_account_number(account_number: str) -> date:
    return datetime.strptime(account_number.split("-")[-2], "%Y%m%d").date()


def _sequence_from_account_number(account_number: str) -> int:
    return int(account_number.split("-")[-1])


def _global_account_index(business_day: date, account_seq: int, config: SeedConfig) -> int:
    days_since_start = max((business_day - DEMO_START_DATE).days, 0)
    return days_since_start * config.accounts_per_day + account_seq - 1


def _chunks(values: list[str], size: int = 80):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _query_existing(sf: Salesforce, object_name: str, key_field: str, keys: list[str]) -> dict[str, str]:
    existing: dict[str, str] = {}
    if not keys:
        return existing

    for chunk in _chunks(keys):
        in_list = ",".join(f"'{_quote(key)}'" for key in chunk)
        query = f"SELECT Id, {key_field} FROM {object_name} WHERE {key_field} IN ({in_list})"
        result = sf.query_all(query)
        for record in result.get("records", []):
            existing[str(record[key_field])] = record["Id"]
    return existing


def _query_records_by_ids(
    sf: Salesforce,
    object_name: str,
    filter_field: str,
    ids: list[str],
    select_fields: list[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for chunk in _chunks([value for value in ids if value]):
        in_list = ",".join(f"'{_quote(value)}'" for value in chunk)
        query = f"SELECT {', '.join(select_fields)} FROM {object_name} WHERE {filter_field} IN ({in_list})"
        records.extend(sf.query_all(query).get("records", []))
    return records


def _standard_pricebook_id(sf: Salesforce) -> str:
    result = sf.query("SELECT Id, IsActive FROM Pricebook2 WHERE IsStandard = true LIMIT 1")
    if not result.get("records"):
        raise RuntimeError("Salesforce standard price book was not found")
    record = result["records"][0]
    if not record.get("IsActive"):
        sf.Pricebook2.update(record["Id"], {"IsActive": True})
    return record["Id"]


def _is_object_createable(sf: Salesforce, object_name: str) -> bool:
    return bool(getattr(sf, object_name).describe().get("createable"))


def _fail_on_storage_limit() -> bool:
    return os.environ.get("CREDIT_UNION_FAIL_ON_STORAGE_LIMIT", "").lower() in {"1", "true", "yes"}


def _write_records(
    sf_object: Any,
    records: list[dict[str, Any]],
    key_field: str,
    existing: dict[str, str],
) -> dict[str, int]:
    stats = {"created_count": 0, "updated_count": 0, "error_count": 0, "skipped_count": 0}
    create_headers = {"Sforce-Duplicate-Rule-Header": "allowSave=true"}
    storage_limit_reached = False
    for record in records:
        key = str(record[key_field])
        payload = {k: v for k, v in record.items() if k != key_field and v is not None}
        try:
            if key in existing:
                sf_object.update(existing[key], payload)
                stats["updated_count"] += 1
            elif storage_limit_reached:
                stats["skipped_count"] += 1
            else:
                result = sf_object.create(record, headers=create_headers)
                existing[key] = result["id"]
                stats["created_count"] += 1
        except Exception as exc:
            if "STORAGE_LIMIT_EXCEEDED" in str(exc):
                if _fail_on_storage_limit():
                    raise RuntimeError(
                        "Salesforce storage limit exceeded while writing demo data; "
                        "free Salesforce storage or rerun with smaller CREDIT_UNION_* volumes."
                    ) from exc
                storage_limit_reached = True
                stats["skipped_count"] += 1
                logger.warning(
                    "Salesforce storage limit reached while creating %s=%s; "
                    "skipping new creates for this object and continuing ingestion.",
                    key_field,
                    key,
                )
                continue
            logger.exception("Salesforce write failed for %s=%s", key_field, key)
            stats["error_count"] += 1
    return stats


def _write_composite_records(
    sf_object: Any,
    records: list[dict[str, Any]],
    key_field: str,
    existing: dict[str, str],
) -> dict[str, int]:
    stats = {"created_count": 0, "updated_count": 0, "error_count": 0, "skipped_count": 0}
    storage_limit_reached = False
    for record in records:
        key = str(record[key_field])
        payload = {k: v for k, v in record.items() if k != key_field and v is not None}
        try:
            if key in existing:
                update_payload = {
                    k: v
                    for k, v in payload.items()
                    if k
                    not in {
                        "CampaignId",
                        "ContactId",
                        "LeadId",
                        "OpportunityId",
                        "Pricebook2Id",
                        "PricebookEntryId",
                        "Product2Id",
                    }
                }
                if update_payload:
                    sf_object.update(existing[key], update_payload)
                stats["updated_count"] += 1
            elif storage_limit_reached:
                stats["skipped_count"] += 1
            else:
                result = sf_object.create(payload)
                existing[key] = result["id"]
                stats["created_count"] += 1
        except Exception as exc:
            if "STORAGE_LIMIT_EXCEEDED" in str(exc):
                if _fail_on_storage_limit():
                    raise RuntimeError(
                        "Salesforce storage limit exceeded while writing demo data; "
                        "free Salesforce storage or rerun with smaller CREDIT_UNION_* volumes."
                    ) from exc
                storage_limit_reached = True
                stats["skipped_count"] += 1
                logger.warning(
                    "Salesforce storage limit reached while creating composite key=%s; "
                    "skipping new creates for this object and continuing ingestion.",
                    key,
                )
                continue
            logger.exception("Salesforce composite write failed for key=%s", key)
            stats["error_count"] += 1
    return stats


def _account_rows(config: SeedConfig) -> list[dict[str, Any]]:
    rows = []
    for business_day in _seed_dates(config):
        rng = _rng_for(business_day, "accounts")
        for seq in range(1, config.accounts_per_day + 1):
            market = BRANCH_MARKETS[(business_day.toordinal() + seq) % len(BRANCH_MARKETS)]
            profile, account_type, industry = ACCOUNT_PROFILES[(business_day.toordinal() + seq) % len(ACCOUNT_PROFILES)]
            relationship_tenure = rng.choice(["new member", "3-year member", "7-year member", "15-year member"])
            digital_tier = rng.choice(["mobile-first", "branch-assisted", "advisor-led", "digital plus branch"])
            account_number = f"CREDIT-UNION-DEMO-{business_day:%Y%m%d}-{seq:03d}"
            rows.append(
                {
                    "AccountNumber": account_number,
                    "Name": f"Credit Union Demo {market['branch']} {profile} {business_day:%Y%m%d}-{seq:03d}",
                    "Type": account_type,
                    "Industry": industry,
                    "BillingCity": market["city"],
                    "BillingState": "California",
                    "BillingCountry": "United States",
                    "Phone": f"{market['phone_area']}-555-{1000 + seq:04d}",
                    "Website": "https://demo.creditunion.example",
                    "Description": (
                        f"Synthetic credit union {profile.lower()} relationship in {market['county']} County. "
                        f"Generated business date {business_day.isoformat()}; branch market {market['area']}; "
                        f"relationship profile {relationship_tenure}; engagement tier {digital_tier}."
                    ),
                }
            )
    return rows


def _contact_rows(accounts: list[dict[str, Any]], account_ids: dict[str, str], config: SeedConfig) -> list[dict[str, Any]]:
    rows = []
    for account_idx, account in enumerate(accounts):
        account_number = account["AccountNumber"]
        account_id = account_ids.get(account_number)
        if not account_id:
            continue
        business_day = _business_day_from_account_number(account_number)
        account_seq = _sequence_from_account_number(account_number)
        account_global_idx = _global_account_index(business_day, account_seq, config)
        for contact_idx in range(1, config.contacts_per_account + 1):
            seq = account_global_idx * config.contacts_per_account + contact_idx
            first = FIRST_NAMES[seq % len(FIRST_NAMES)]
            last = LAST_NAMES[(seq // len(FIRST_NAMES)) % len(LAST_NAMES)]
            role = CONTACT_ROLES[(seq + contact_idx) % len(CONTACT_ROLES)]
            lead_source = ["Web", "Phone Inquiry", "Partner Referral", "Other"][seq % 4]
            rows.append(
                {
                    "Email": f"credit.union.demo.{account_number.lower()}.{contact_idx}@example.com",
                    "FirstName": first,
                    "LastName": last,
                    "Title": role,
                    "Phone": f"510-555-{2000 + (seq % 7000):04d}",
                    "MailingCity": account["BillingCity"],
                    "MailingState": "California",
                    "MailingCountry": "United States",
                    "LeadSource": lead_source,
                    "AccountId": account_id,
                    "Description": (
                        f"Synthetic credit union demo contact for {role.lower()} relationship. "
                        f"Generated from account {account_number} for interval-driven CRM ingestion testing."
                    ),
                }
            )
    return rows


def _campaign_rows(config: SeedConfig) -> list[dict[str, Any]]:
    rows = []
    months = sorted({date(day.year, day.month, 1) for day in _seed_dates(config)})
    for month_idx, month_start in enumerate(months):
        theme, campaign_type, product_family = CAMPAIGN_THEMES[month_idx % len(CAMPAIGN_THEMES)]
        campaign_name = f"Credit Union Demo {theme} {month_start:%Y-%m}"
        rows.append(
            {
                "Name": campaign_name,
                "Type": campaign_type,
                "Status": "In Progress" if month_start >= date.today().replace(day=1) else "Completed",
                "StartDate": month_start.isoformat(),
                "EndDate": (month_start + timedelta(days=27)).isoformat(),
                "Description": (
                    f"Synthetic credit union {product_family.lower()} campaign for branch, digital, "
                    f"and community member growth demos. Month {month_start:%Y-%m}."
                ),
            }
        )
    return rows


def _lead_rows(config: SeedConfig) -> list[dict[str, Any]]:
    rows = []
    statuses = ["Open - Not Contacted", "Working - Contacted", "Closed - Converted", "Closed - Not Converted"]
    ratings = ["Hot", "Warm", "Cold"]
    for business_day in _seed_dates(config):
        market = BRANCH_MARKETS[business_day.toordinal() % len(BRANCH_MARKETS)]
        rng = _rng_for(business_day, "leads")
        for lead_idx in range(1, config.leads_per_day + 1):
            seq = (business_day - DEMO_START_DATE).days * config.leads_per_day + lead_idx
            first = FIRST_NAMES[(seq + 3) % len(FIRST_NAMES)]
            last = LAST_NAMES[(seq + 5) % len(LAST_NAMES)]
            product = PRODUCTS[(seq + business_day.month) % len(PRODUCTS)]
            rows.append(
                {
                    "Email": f"credit.union.lead.{business_day:%Y%m%d}.{lead_idx}@example.com",
                    "FirstName": first,
                    "LastName": last,
                    "Company": f"Credit Union Demo {market['branch']} Member Prospect",
                    "Title": rng.choice(["Prospective Member", "Community Partner", "Small Business Owner"]),
                    "Phone": f"{market['phone_area']}-555-{3000 + (seq % 6000):04d}",
                    "City": market["city"],
                    "State": "California",
                    "Country": "United States",
                    "LeadSource": product["lead_source"],
                    "Status": statuses[seq % len(statuses)],
                    "Rating": ratings[seq % len(ratings)],
                    "Description": (
                        f"Synthetic credit union lead interested in {product['name']}. "
                        f"Generated business date {business_day.isoformat()} for {market['area']}."
                    ),
                }
            )
    return rows


def _product_rows() -> list[dict[str, Any]]:
    rows = []
    for product in PRODUCTS:
        rows.append(
            {
                "ProductCode": f"CU-{product['code']}",
                "Name": f"Credit Union Demo {product['name']}",
                "Family": product["family"],
                "IsActive": True,
                "Description": (
                    f"Synthetic credit union product catalog item for {product['family'].lower()} CRM demos. "
                    f"Typical term {product['term_months']} months."
                ),
            }
        )
    return rows


def _stage_for(business_day: date, account_idx: int, opp_idx: int) -> tuple[str, int]:
    stage_idx = (business_day.toordinal() + account_idx + opp_idx) % len(STAGES)
    return STAGES[stage_idx]


def _opportunity_rows(
    accounts: list[dict[str, Any]],
    account_ids: dict[str, str],
    config: SeedConfig,
    pricebook_id: str | None = None,
) -> list[dict[str, Any]]:
    rows = []
    interval_days = max((config.end_date - config.start_date).days, 0)
    for account_idx, account in enumerate(accounts):
        account_number = account["AccountNumber"]
        account_id = account_ids.get(account_number)
        if not account_id:
            continue
        business_day = _business_day_from_account_number(account_number)
        account_seq = _sequence_from_account_number(account_number)
        account_global_idx = _global_account_index(business_day, account_seq, config)
        rng = _rng_for(business_day, f"opportunities-{account_global_idx}")
        for opp_idx in range(1, config.opportunities_per_account + 1):
            product = PRODUCTS[(business_day.toordinal() + account_global_idx + opp_idx) % len(PRODUCTS)]
            stage_name, probability = _stage_for(business_day, account_global_idx, opp_idx)
            close_offset = rng.randint(0, max(interval_days, 14))
            if stage_name not in {"Closed Won", "Closed Lost"}:
                close_offset += rng.randint(7, 45)
            close_date = min(config.end_date + timedelta(days=45), business_day + timedelta(days=close_offset))
            amount = round(product["base_amount"] * rng.uniform(0.65, 1.55), 2)
            rows.append(
                {
                    "Name": f"Credit Union Demo {account_number} {product['name']} {opp_idx:02d}",
                    "AccountId": account_id,
                    "Pricebook2Id": pricebook_id,
                    "Type": "Existing Customer - Upgrade",
                    "LeadSource": product["lead_source"],
                    "StageName": stage_name,
                    "Amount": amount,
                    "Probability": probability,
                    "CloseDate": close_date.isoformat(),
                    "NextStep": [
                        "Collect application documents",
                        "Send member rate options",
                        "Schedule banker follow-up",
                        "Verify collateral and income",
                    ][opp_idx % 4],
                    "Description": (
                        f"Synthetic credit union {product['family'].lower()} opportunity for {product['name']}. "
                        f"Generated business date {business_day.isoformat()}; proposed term "
                        f"{product['term_months']} months."
                    ),
                }
            )
    return rows


def _opportunity_contact_role_rows(
    opportunities: list[dict[str, Any]],
    opportunity_ids: dict[str, str],
    contacts: list[dict[str, Any]],
    contact_ids: dict[str, str],
) -> list[dict[str, Any]]:
    contact_by_account: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        contact_by_account.setdefault(contact["AccountId"], []).append(contact)

    rows = []
    for opp_idx, opportunity in enumerate(opportunities):
        opportunity_id = opportunity_ids.get(opportunity["Name"])
        if not opportunity_id:
            continue
        account_contacts = contact_by_account.get(opportunity["AccountId"], [])
        if not account_contacts:
            continue
        contact = account_contacts[opp_idx % len(account_contacts)]
        contact_id = contact_ids.get(contact["Email"])
        if not contact_id:
            continue
        role = "Decision Maker" if opp_idx % 3 == 0 else "Influencer"
        key = f"{opportunity_id}:{contact_id}:{role}"
        rows.append(
            {
                "_CompositeKey": key,
                "OpportunityId": opportunity_id,
                "ContactId": contact_id,
                "Role": role,
                "IsPrimary": opp_idx % 3 == 0,
            }
        )
    return rows


def _opportunity_contact_role_existing(sf: Salesforce, opportunity_ids: list[str]) -> dict[str, str]:
    existing = {}
    records = _query_records_by_ids(
        sf,
        "OpportunityContactRole",
        "OpportunityId",
        opportunity_ids,
        ["Id", "OpportunityId", "ContactId", "Role"],
    )
    for record in records:
        existing[f"{record['OpportunityId']}:{record['ContactId']}:{record.get('Role') or ''}"] = record["Id"]
    return existing


def _pricebook_entry_rows(product_ids: dict[str, str], pricebook_id: str) -> list[dict[str, Any]]:
    rows = []
    for product in PRODUCTS:
        product_code = f"CU-{product['code']}"
        product_id = product_ids.get(product_code)
        if not product_id:
            continue
        rows.append(
            {
                "_CompositeKey": f"{pricebook_id}:{product_id}",
                "Pricebook2Id": pricebook_id,
                "Product2Id": product_id,
                "UnitPrice": float(product["base_amount"]),
                "IsActive": True,
                "UseStandardPrice": False,
            }
        )
    return rows


def _pricebook_entry_existing(sf: Salesforce, pricebook_id: str, product_ids: list[str]) -> dict[str, str]:
    existing = {}
    records = _query_records_by_ids(
        sf,
        "PricebookEntry",
        "Product2Id",
        product_ids,
        ["Id", "Pricebook2Id", "Product2Id"],
    )
    for record in records:
        if record.get("Pricebook2Id") == pricebook_id:
            existing[f"{record['Pricebook2Id']}:{record['Product2Id']}"] = record["Id"]
    return existing


def _opportunity_line_item_rows(
    opportunities: list[dict[str, Any]],
    opportunity_ids: dict[str, str],
    pricebook_entry_ids: dict[str, str],
) -> list[dict[str, Any]]:
    rows = []
    product_by_name = {product["name"]: product for product in PRODUCTS}
    for opportunity in opportunities:
        opportunity_id = opportunity_ids.get(opportunity["Name"])
        if not opportunity_id:
            continue
        product = next((item for name, item in product_by_name.items() if name in opportunity["Name"]), None)
        if not product:
            continue
        pricebook_entry_id = pricebook_entry_ids.get(product["code"])
        if not pricebook_entry_id:
            continue
        rows.append(
            {
                "_CompositeKey": f"{opportunity_id}:{pricebook_entry_id}",
                "OpportunityId": opportunity_id,
                "PricebookEntryId": pricebook_entry_id,
                "Quantity": 1,
                "UnitPrice": float(opportunity["Amount"]),
                "Description": f"Demo line item for {product['name']} tied to credit union CRM pipeline analytics.",
            }
        )
    return rows


def _opportunity_line_item_existing(sf: Salesforce, opportunity_ids: list[str]) -> dict[str, str]:
    existing = {}
    records = _query_records_by_ids(
        sf,
        "OpportunityLineItem",
        "OpportunityId",
        opportunity_ids,
        ["Id", "OpportunityId", "PricebookEntryId"],
    )
    for record in records:
        existing[f"{record['OpportunityId']}:{record['PricebookEntryId']}"] = record["Id"]
    return existing


def _campaign_member_rows(
    campaigns: list[dict[str, Any]],
    campaign_ids: dict[str, str],
    contacts: list[dict[str, Any]],
    contact_ids: dict[str, str],
    leads: list[dict[str, Any]],
    lead_ids: dict[str, str],
) -> list[dict[str, Any]]:
    rows = []
    if not campaigns:
        return rows
    campaign_names = [campaign["Name"] for campaign in campaigns]

    for idx, contact in enumerate(contacts):
        campaign_name = campaign_names[idx % len(campaign_names)]
        campaign_id = campaign_ids.get(campaign_name)
        contact_id = contact_ids.get(contact["Email"])
        if campaign_id and contact_id:
            rows.append(
                {
                    "_CompositeKey": f"{campaign_id}:contact:{contact_id}",
                    "CampaignId": campaign_id,
                    "ContactId": contact_id,
                    "Status": "Responded" if idx % 2 == 0 else "Sent",
                }
            )

    for idx, lead in enumerate(leads):
        campaign_name = campaign_names[(idx + 2) % len(campaign_names)]
        campaign_id = campaign_ids.get(campaign_name)
        lead_id = lead_ids.get(lead["Email"])
        if campaign_id and lead_id:
            rows.append(
                {
                    "_CompositeKey": f"{campaign_id}:lead:{lead_id}",
                    "CampaignId": campaign_id,
                    "LeadId": lead_id,
                    "Status": "Responded" if idx % 3 == 0 else "Sent",
                }
            )
    return rows


def _campaign_member_existing(sf: Salesforce, campaign_ids: list[str]) -> dict[str, str]:
    existing = {}
    records = _query_records_by_ids(
        sf,
        "CampaignMember",
        "CampaignId",
        campaign_ids,
        ["Id", "CampaignId", "ContactId", "LeadId"],
    )
    for record in records:
        if record.get("ContactId"):
            existing[f"{record['CampaignId']}:contact:{record['ContactId']}"] = record["Id"]
        if record.get("LeadId"):
            existing[f"{record['CampaignId']}:lead:{record['LeadId']}"] = record["Id"]
    return existing


def _task_rows(
    opportunities: list[dict[str, Any]],
    opportunity_ids: dict[str, str],
    config: SeedConfig,
) -> list[dict[str, Any]]:
    rows = []
    interval_days = max((config.end_date - config.start_date).days, 0)
    for opp_idx, opportunity in enumerate(opportunities):
        what_id = opportunity_ids.get(opportunity["Name"])
        if not what_id:
            continue
        rng = _rng_for(config.start_date, f"tasks-{opp_idx}")
        for task_idx in range(1, config.tasks_per_opportunity + 1):
            task_type, task_action = TASK_TEMPLATES[(opp_idx + task_idx) % len(TASK_TEMPLATES)]
            activity_date = config.start_date + timedelta(days=rng.randint(0, interval_days))
            status = ["Completed", "Completed", "In Progress", "Not Started"][(opp_idx + task_idx) % 4]
            priority = ["Normal", "Normal", "High"][(opp_idx + task_idx) % 3]
            rows.append(
                {
                    "Subject": f"Credit Union Demo {opportunity['Name']} Task {task_idx:02d}",
                    "WhatId": what_id,
                    "ActivityDate": activity_date.isoformat(),
                    "Status": status,
                    "Priority": priority,
                    "Description": (
                        f"Synthetic banker {task_type.lower()} activity. {task_action}. "
                        f"Generated for interval {config.start_date.isoformat()} to {config.end_date.isoformat()}."
                    ),
                }
            )
    return rows


def _event_rows(
    opportunities: list[dict[str, Any]],
    opportunity_ids: dict[str, str],
    contacts: list[dict[str, Any]],
    contact_ids: dict[str, str],
    config: SeedConfig,
) -> list[dict[str, Any]]:
    contact_by_account: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        contact_by_account.setdefault(contact["AccountId"], []).append(contact)

    rows = []
    interval_days = max((config.end_date - config.start_date).days, 0)
    for opp_idx, opportunity in enumerate(opportunities):
        what_id = opportunity_ids.get(opportunity["Name"])
        if not what_id:
            continue
        account_contacts = contact_by_account.get(opportunity["AccountId"], [])
        who_id = None
        if account_contacts:
            contact = account_contacts[opp_idx % len(account_contacts)]
            who_id = contact_ids.get(contact["Email"])
        rng = _rng_for(config.start_date, f"events-{opp_idx}")
        for event_idx in range(1, config.events_per_opportunity + 1):
            event_date = config.start_date + timedelta(days=rng.randint(0, interval_days))
            start_dt = datetime.combine(event_date, datetime.min.time()).replace(hour=10 + (opp_idx % 5))
            end_dt = start_dt + timedelta(minutes=45)
            rows.append(
                {
                    "Subject": f"Credit Union Demo {opportunity['Name']} Event {event_idx:02d}",
                    "WhatId": what_id,
                    "WhoId": who_id,
                    "StartDateTime": f"{start_dt.isoformat()}Z",
                    "EndDateTime": f"{end_dt.isoformat()}Z",
                    "DurationInMinutes": 45,
                    "Location": "credit union branch or video appointment",
                    "Description": (
                        "Synthetic member appointment for loan, deposit, or financial wellness "
                        f"pipeline follow-up in interval {config.start_date.isoformat()} to {config.end_date.isoformat()}."
                    ),
                }
            )
    return rows


def _log_generation_plan(config: SeedConfig, accounts: list[dict[str, Any]]) -> None:
    expected_contacts = len(accounts) * config.contacts_per_account
    expected_opportunities = len(accounts) * config.opportunities_per_account
    expected_tasks = expected_opportunities * config.tasks_per_opportunity
    expected_events = expected_opportunities * config.events_per_opportunity
    expected_leads = len(_seed_dates(config)) * config.leads_per_day
    expected_campaigns = len({date(day.year, day.month, 1) for day in _seed_dates(config)})
    interval_days = len(_date_range(config.start_date, config.end_date))
    generated_days = len(_seed_dates(config))
    logger.info(
        "Credit Union Salesforce demo generation plan: %s",
        json.dumps(
            {
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "interval_days": interval_days,
                "generated_days": generated_days,
                "accounts": len(accounts),
                "contacts": expected_contacts,
                "campaigns": expected_campaigns,
                "leads": expected_leads,
                "products": len(PRODUCTS),
                "pricebook_entries": len(PRODUCTS),
                "opportunities": expected_opportunities,
                "opportunity_contact_roles": expected_opportunities,
                "opportunity_line_items": expected_opportunities,
                "tasks": expected_tasks,
                "events": expected_events,
                "full_refresh": config.full_refresh,
                "dry_run": config.dry_run,
            },
            sort_keys=True,
        ),
    )


def main() -> None:
    config = _config()
    accounts = _account_rows(config)
    _log_generation_plan(config, accounts)

    if config.dry_run:
        logger.info("CREDIT_UNION_DRY_RUN is enabled; skipping Salesforce writes.")
        return

    sf = _salesforce()
    audit_rows = []

    account_existing = _query_existing(sf, "Account", "AccountNumber", [row["AccountNumber"] for row in accounts])
    account_stats = _write_records(sf.Account, accounts, "AccountNumber", account_existing)
    audit_rows.append({"object": "account", "generated": len(accounts), **account_stats})
    logger.info("Seeded accounts: %s", json.dumps(audit_rows[-1], sort_keys=True))

    contacts = _contact_rows(accounts, account_existing, config)
    contact_existing = _query_existing(sf, "Contact", "Email", [row["Email"] for row in contacts])
    contact_stats = _write_records(sf.Contact, contacts, "Email", contact_existing)
    audit_rows.append({"object": "contact", "generated": len(contacts), **contact_stats})
    logger.info("Seeded contacts: %s", json.dumps(audit_rows[-1], sort_keys=True))

    campaigns = _campaign_rows(config)
    campaign_existing = _query_existing(sf, "Campaign", "Name", [row["Name"] for row in campaigns])
    if _is_object_createable(sf, "Campaign"):
        campaign_stats = _write_records(sf.Campaign, campaigns, "Name", campaign_existing)
    else:
        campaign_stats = {
            "created_count": 0,
            "updated_count": len(campaign_existing),
            "error_count": 0,
            "skipped_count": max(len(campaigns) - len(campaign_existing), 0),
        }
        logger.warning("Salesforce Campaign is not createable in this org; skipping new demo campaigns.")
    audit_rows.append({"object": "campaign", "generated": len(campaigns), **campaign_stats})
    logger.info("Seeded campaigns: %s", json.dumps(audit_rows[-1], sort_keys=True))

    leads = _lead_rows(config)
    lead_existing = _query_existing(sf, "Lead", "Email", [row["Email"] for row in leads])
    lead_stats = _write_records(sf.Lead, leads, "Email", lead_existing)
    audit_rows.append({"object": "lead", "generated": len(leads), **lead_stats})
    logger.info("Seeded leads: %s", json.dumps(audit_rows[-1], sort_keys=True))

    products = _product_rows()
    product_existing = _query_existing(sf, "Product2", "ProductCode", [row["ProductCode"] for row in products])
    product_stats = _write_records(sf.Product2, products, "ProductCode", product_existing)
    audit_rows.append({"object": "product", "generated": len(products), **product_stats})
    logger.info("Seeded products: %s", json.dumps(audit_rows[-1], sort_keys=True))

    pricebook_id = _standard_pricebook_id(sf)
    pricebook_entries = _pricebook_entry_rows(product_existing, pricebook_id)
    pricebook_entry_existing = _pricebook_entry_existing(sf, pricebook_id, list(product_existing.values()))
    pricebook_entry_stats = _write_composite_records(
        sf.PricebookEntry,
        pricebook_entries,
        "_CompositeKey",
        pricebook_entry_existing,
    )
    audit_rows.append({"object": "pricebook_entry", "generated": len(pricebook_entries), **pricebook_entry_stats})
    logger.info("Seeded pricebook entries: %s", json.dumps(audit_rows[-1], sort_keys=True))

    opportunities = _opportunity_rows(accounts, account_existing, config, pricebook_id)
    opportunity_existing = _query_existing(sf, "Opportunity", "Name", [row["Name"] for row in opportunities])
    opportunity_stats = _write_records(sf.Opportunity, opportunities, "Name", opportunity_existing)
    audit_rows.append({"object": "opportunity", "generated": len(opportunities), **opportunity_stats})
    logger.info("Seeded opportunities: %s", json.dumps(audit_rows[-1], sort_keys=True))

    opportunity_contact_roles = _opportunity_contact_role_rows(
        opportunities,
        opportunity_existing,
        contacts,
        contact_existing,
    )
    ocr_existing = _opportunity_contact_role_existing(sf, list(opportunity_existing.values()))
    ocr_stats = _write_composite_records(
        sf.OpportunityContactRole,
        opportunity_contact_roles,
        "_CompositeKey",
        ocr_existing,
    )
    audit_rows.append({"object": "opportunity_contact_role", "generated": len(opportunity_contact_roles), **ocr_stats})
    logger.info("Seeded opportunity contact roles: %s", json.dumps(audit_rows[-1], sort_keys=True))

    campaign_members = _campaign_member_rows(
        campaigns,
        campaign_existing,
        contacts,
        contact_existing,
        leads,
        lead_existing,
    )
    if campaign_members and _is_object_createable(sf, "CampaignMember"):
        campaign_member_existing = _campaign_member_existing(sf, list(campaign_existing.values()))
        campaign_member_stats = _write_composite_records(
            sf.CampaignMember,
            campaign_members,
            "_CompositeKey",
            campaign_member_existing,
        )
    else:
        campaign_member_stats = {
            "created_count": 0,
            "updated_count": 0,
            "error_count": 0,
            "skipped_count": len(campaign_members),
        }
        if campaign_members:
            logger.warning("Salesforce CampaignMember is not createable in this org; skipping new demo campaign members.")
    audit_rows.append({"object": "campaign_member", "generated": len(campaign_members), **campaign_member_stats})
    logger.info("Seeded campaign members: %s", json.dumps(audit_rows[-1], sort_keys=True))

    product_id_to_code = {
        product_id: product_code.removeprefix("CU-")
        for product_code, product_id in product_existing.items()
    }
    pricebook_entry_ids_by_product_code = {
        product_id_to_code[row["Product2Id"]]: pricebook_entry_existing[row["_CompositeKey"]]
        for row in pricebook_entries
        if row["_CompositeKey"] in pricebook_entry_existing
    }
    opportunity_line_items = _opportunity_line_item_rows(
        opportunities,
        opportunity_existing,
        pricebook_entry_ids_by_product_code,
    )
    oli_existing = _opportunity_line_item_existing(sf, list(opportunity_existing.values()))
    oli_stats = _write_composite_records(
        sf.OpportunityLineItem,
        opportunity_line_items,
        "_CompositeKey",
        oli_existing,
    )
    audit_rows.append({"object": "opportunity_line_item", "generated": len(opportunity_line_items), **oli_stats})
    logger.info("Seeded opportunity line items: %s", json.dumps(audit_rows[-1], sort_keys=True))

    tasks = _task_rows(opportunities, opportunity_existing, config)
    task_existing = _query_existing(sf, "Task", "Subject", [row["Subject"] for row in tasks])
    task_stats = _write_records(sf.Task, tasks, "Subject", task_existing)
    audit_rows.append({"object": "task", "generated": len(tasks), **task_stats})
    logger.info("Seeded tasks: %s", json.dumps(audit_rows[-1], sort_keys=True))

    events = _event_rows(opportunities, opportunity_existing, contacts, contact_existing, config)
    event_existing = _query_existing(sf, "Event", "Subject", [row["Subject"] for row in events])
    event_stats = _write_records(sf.Event, events, "Subject", event_existing)
    audit_rows.append({"object": "event", "generated": len(events), **event_stats})
    logger.info("Seeded events: %s", json.dumps(audit_rows[-1], sort_keys=True))

    logger.info(
        "Credit Union Salesforce demo seed summary: %s",
        json.dumps(
            {
                "run_id": f"credit-union-demo-{config.start_date:%Y%m%d}-{config.end_date:%Y%m%d}",
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "objects": audit_rows,
            },
            sort_keys=True,
        ),
    )

    total_errors = sum(row["error_count"] for row in audit_rows)
    if total_errors:
        raise RuntimeError(f"Salesforce demo seed completed with {total_errors} write errors")


if __name__ == "__main__":
    main()
