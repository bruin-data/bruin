#!/usr/bin/env python3
"""Seed a Chargebee test site with deterministic B2B SaaS demo data.

The script uses Chargebee's subscription-import API so the imported resources
retain a useful six-month lifecycle: cohorts, cancellations, and reactivations
are represented by historical subscription timestamps. It intentionally uses
offline collection and fake example.com addresses, so it does not attempt to
charge a payment method or send mail to a real customer.

Examples:
    python3 scripts/seed_chargebee_demo.py --dry-run
    python3 scripts/seed_chargebee_demo.py --apply --yes

Environment:
    CHARGEBEE_SITE: Chargebee site name, with or without .chargebee.com.
    CHARGEBEE_API_KEY: Test-site API key.
"""

from __future__ import annotations

import argparse
import base64
import calendar
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from os import environ
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_CURRENCIES = ("USD", "EUR", "GBP")
PRICE_POINTS = (100, 150, 200, 300, 400, 500, 650, 800, 1000)
RETRYABLE_STATUS_CODES = {409, 429, 500, 502, 503, 504}
CURRENCY_EXCHANGE_RATES = {"EUR": "1.08", "GBP": "1.27", "CAD": "0.74"}


class ChargebeeAPIError(RuntimeError):
    """An error returned by Chargebee's API."""

    def __init__(self, status: int, message: str, path: str):
        super().__init__(f"Chargebee {status} for {path}: {message}")
        self.status = status
        self.message = message
        self.path = path


@dataclass(frozen=True)
class Price:
    currency: str
    amount_major: int
    item_id: str
    item_price_id: str


@dataclass(frozen=True)
class SubscriptionPlan:
    customer_id: str
    subscription_id: str
    currency: str
    amount_major: int
    item_price_id: str
    status: str
    started_at: date
    activated_at: date | None = None
    current_term_start: date | None = None
    current_term_end: date | None = None
    cancelled_at: date | None = None
    lifecycle: str = "active"
    cohort_month: str = ""
    churn_month: str = ""
    reactivation_month: str = ""


class ChargebeeAPI:
    def __init__(self, site: str, api_key: str, timeout: int = 45):
        normalized_site = site.strip().rstrip("/")
        for prefix in ("https://", "http://"):
            if normalized_site.startswith(prefix):
                normalized_site = normalized_site[len(prefix) :]
        if normalized_site.endswith(".chargebee.com"):
            normalized_site = normalized_site[: -len(".chargebee.com")]
        if not normalized_site:
            raise ValueError("CHARGEBEE_SITE cannot be empty")
        self.site = normalized_site
        self.api_key = api_key
        self.timeout = timeout
        token = base64.b64encode(f"{api_key}:".encode()).decode()
        self.auth_header = f"Basic {token}"

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        form: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        query = f"?{urlencode(params or {})}" if params else ""
        url = f"https://{self.site}.chargebee.com/api/v2{path}{query}"
        body = urlencode(
            {
                key: str(value).lower() if isinstance(value, bool) else value
                for key, value in (form or {}).items()
            }
        ).encode()
        headers = {
            "Authorization": self.auth_header,
            "Accept": "application/json",
            "User-Agent": "bruin-chargebee-demo-seeder/1.0",
        }
        if form is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if idempotency_key:
            headers["chargebee-idempotency-key"] = idempotency_key[:100]

        for attempt in range(5):
            request = Request(
                url,
                data=body if method != "GET" else None,
                headers=headers,
                method=method,
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                return json.loads(raw) if raw else {}
            except HTTPError as error:
                raw = error.read().decode("utf-8", errors="replace")
                detail = self._error_detail(raw)
                if error.code not in RETRYABLE_STATUS_CODES or attempt == 4:
                    raise ChargebeeAPIError(error.code, detail, path) from error
                retry_after = error.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(2**attempt, 16)
                time.sleep(delay)
            except (TimeoutError, URLError) as error:
                if attempt == 4:
                    raise RuntimeError(
                        f"Chargebee request failed for {path}: {error}"
                    ) from error
                time.sleep(min(2**attempt, 16))
        raise AssertionError("unreachable")

    @staticmethod
    def _error_detail(raw: str) -> str:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return raw[:500] or "unknown API error"
        error = payload.get("error", payload)
        if isinstance(error, dict):
            return str(error.get("message") or error.get("error_msg") or error)
        return str(error)

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("GET", path, params=params)

    def post(
        self, path: str, form: dict[str, Any], idempotency_key: str
    ) -> dict[str, Any]:
        return self.request("POST", path, form=form, idempotency_key=idempotency_key)

    def post_or_get(
        self, path: str, form: dict[str, Any], resource_path: str, key: str
    ) -> dict[str, Any]:
        try:
            return self.post(path, form, key)
        except ChargebeeAPIError as error:
            if error.status not in {400, 409, 422}:
                raise
            try:
                return self.get(resource_path)
            except ChargebeeAPIError:
                raise error


def add_months(month: date, months: int) -> date:
    month_index = month.year * 12 + month.month - 1 + months
    year, month_number = divmod(month_index, 12)
    return date(year, month_number + 1, 1)


def as_timestamp(day: date) -> int:
    return int(
        datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc).timestamp()
    )


def month_label(day: date) -> str:
    return day.strftime("%Y-%m")


def safe_day(month: date, preferred_day: int, today: date) -> date:
    last_day = calendar.monthrange(month.year, month.month)[1]
    day = min(preferred_day, last_day)
    if month.year == today.year and month.month == today.month:
        day = min(day, max(1, today.day - 1))
    return date(month.year, month.month, max(1, day))


def make_plan(
    rng: random.Random,
    customer_id: str,
    subscription_number: int,
    price: Price,
    current_month: date,
    months: int,
    lifecycle: str,
    today: date,
) -> SubscriptionPlan:
    cohort_offset = rng.randrange(months)
    if lifecycle == "cancelled" and months > 1:
        cohort_offset = rng.randint(1, months - 1)
    cohort = add_months(current_month, -cohort_offset)
    started = safe_day(cohort, rng.randint(2, 20), today)
    subscription_id = f"{customer_id}-sub-{subscription_number}"

    if lifecycle == "cancelled":
        # Keep the cancellation in the generated six-month window and after
        # activation. Imported subscriptions preserve this history directly.
        cancel_offset = rng.randint(1, max(1, cohort_offset)) if cohort_offset else 0
        cancellation_month = add_months(cohort, cancel_offset)
        if cancellation_month > current_month:
            cancellation_month = current_month
        cancelled = safe_day(cancellation_month, rng.randint(8, 24), today)
        if cancelled <= started:
            cancelled = min(started + timedelta(days=7), today - timedelta(days=1))
        return SubscriptionPlan(
            customer_id,
            subscription_id,
            price.currency,
            price.amount_major,
            price.item_price_id,
            "cancelled",
            started,
            activated_at=started,
            cancelled_at=cancelled,
            lifecycle=lifecycle,
            cohort_month=month_label(cohort),
            churn_month=month_label(cancelled),
        )

    term_start = max(started, current_month)
    term_end = add_months(term_start.replace(day=1), 1)
    return SubscriptionPlan(
        customer_id,
        subscription_id,
        price.currency,
        price.amount_major,
        price.item_price_id,
        "non_renewing" if lifecycle == "non_renewing" else "active",
        started,
        activated_at=started,
        current_term_start=term_start,
        current_term_end=term_end,
        lifecycle=lifecycle,
        cohort_month=month_label(cohort),
    )


def generate_data(
    count: int, months: int, seed: int, run_id: str, currencies: tuple[str, ...]
) -> tuple[list[dict[str, Any]], list[Price], list[SubscriptionPlan]]:
    rng = random.Random(seed)
    today = date.today()
    current_month = today.replace(day=1)
    prices = [
        Price(
            currency,
            amount,
            f"{run_id}-plan-{currency.lower()}-{amount}",
            f"{run_id}-price-{currency.lower()}-{amount}-monthly",
        )
        for currency in currencies
        for amount in PRICE_POINTS
    ]
    customers: list[dict[str, Any]] = []
    subscriptions: list[SubscriptionPlan] = []
    first_names = ("Avery", "Jordan", "Maya", "Noah", "Riley", "Sam", "Taylor", "Zoe")
    last_names = (
        "Morgan",
        "Patel",
        "Chen",
        "Rivera",
        "Murphy",
        "Kowalski",
        "Sato",
        "Brown",
    )

    for number in range(1, count + 1):
        customer_id = f"{run_id}-customer-{number:04d}"
        currency = rng.choice(currencies)
        first_name = rng.choice(first_names)
        last_name = rng.choice(last_names)
        customers.append(
            {
                "id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{customer_id}@example.com",
                "company": f"{last_name} Systems {number:04d}",
                "preferred_currency_code": currency,
                "auto_collection": "off",
                "demo_number": number,
            }
        )

        price = rng.choice([item for item in prices if item.currency == currency])
        lifecycle_roll = rng.random()
        if lifecycle_roll < 0.62:
            lifecycle = "active"
        elif lifecycle_roll < 0.74:
            lifecycle = "non_renewing"
        elif lifecycle_roll < 0.90:
            lifecycle = "cancelled"
        else:
            lifecycle = "reactivated"

        if lifecycle == "reactivated" and months >= 4:
            # An old cancelled episode plus a later active episode gives the
            # source tables two real subscriptions for the same customer.
            old_cohort = add_months(current_month, -rng.randint(3, months - 1))
            old_started = safe_day(old_cohort, rng.randint(2, 12), today)
            old_cancel_month = add_months(old_cohort, rng.randint(1, 2))
            old_cancelled = safe_day(old_cancel_month, rng.randint(8, 20), today)
            if old_cancelled >= current_month:
                old_cancelled = current_month - timedelta(days=15)
            old_plan = SubscriptionPlan(
                customer_id,
                f"{customer_id}-sub-1",
                currency,
                price.amount_major,
                price.item_price_id,
                "cancelled",
                old_started,
                activated_at=old_started,
                cancelled_at=old_cancelled,
                lifecycle="reactivated_cancelled_episode",
                cohort_month=month_label(old_cohort),
                churn_month=month_label(old_cancelled),
                reactivation_month=month_label(current_month),
            )
            reactivation_start = safe_day(current_month, rng.randint(2, 8), today)
            new_price = rng.choice(
                [item for item in prices if item.currency == currency]
            )
            active_plan = SubscriptionPlan(
                customer_id,
                f"{customer_id}-sub-2",
                currency,
                new_price.amount_major,
                new_price.item_price_id,
                "active",
                reactivation_start,
                activated_at=reactivation_start,
                current_term_start=reactivation_start,
                current_term_end=add_months(reactivation_start.replace(day=1), 1),
                lifecycle="reactivated",
                cohort_month=month_label(old_cohort),
                churn_month=month_label(old_cancelled),
                reactivation_month=month_label(current_month),
            )
            subscriptions.extend((old_plan, active_plan))
        else:
            if lifecycle == "reactivated":
                lifecycle = "active"
            subscriptions.append(
                make_plan(
                    rng, customer_id, 1, price, current_month, months, lifecycle, today
                )
            )

    return customers, prices, subscriptions


def active_currencies(api: ChargebeeAPI) -> set[str]:
    payload = api.get("/currencies/list", params={"limit": 100})
    currencies = set()
    for entry in payload.get("list", []):
        currency = entry.get("currency", entry)
        if currency.get("enabled", True):
            currencies.add(currency.get("currency_code", "").upper())
    return currencies


def add_currencies(api: ChargebeeAPI, currencies: set[str], run_id: str) -> None:
    for currency in sorted(currencies):
        if currency not in CURRENCY_EXCHANGE_RATES:
            raise ValueError(
                f"No demo exchange rate is configured for {currency}; add it to "
                "CURRENCY_EXCHANGE_RATES before using --add-missing-currencies."
            )
        print(f"Adding {currency} to the Chargebee test site...")
        api.post(
            "/currencies",
            {
                "currency_code": currency,
                "forex_type": "manual",
                "manual_exchange_rate": CURRENCY_EXCHANGE_RATES[currency],
            },
            f"{run_id}:currency:{currency}",
        )


def ensure_item_family(api: ChargebeeAPI, run_id: str) -> str:
    payload = api.get("/item_families", params={"limit": 100})
    for entry in payload.get("list", []):
        family = entry.get("item_family", entry)
        if family.get("status", "active") == "active" and family.get("id"):
            return family["id"]

    family_id = f"{run_id}-family"
    api.post_or_get(
        "/item_families",
        {
            "id": family_id,
            "name": f"Bruin Demo Family {run_id}",
            "description": "Synthetic catalog family created by the Bruin Chargebee demo seeder.",
        },
        f"/item_families/{family_id}",
        f"{run_id}:item-family:{family_id}",
    )
    return family_id


def ensure_catalog(api: ChargebeeAPI, prices: list[Price], run_id: str) -> None:
    print(f"Ensuring {len(prices)} demo plan prices exist...")
    item_family_id = ensure_item_family(api, run_id)
    for index, price in enumerate(prices, start=1):
        api.post_or_get(
            "/items",
            {
                "id": price.item_id,
                "name": f"Bruin Demo {price.currency} {price.amount_major} Monthly Plan",
                "external_name": f"Demo {price.amount_major} {price.currency}/month",
                "type": "plan",
                "item_family_id": item_family_id,
                "description": "Synthetic plan created by the Bruin Chargebee demo seeder.",
                "enabled_for_checkout": False,
            },
            f"/items/{price.item_id}",
            f"{run_id}:item:{price.item_id}",
        )
        api.post_or_get(
            "/item_prices",
            {
                "id": price.item_price_id,
                "name": f"Bruin Demo {price.currency} {price.amount_major} Monthly Price",
                "external_name": f"Demo {price.amount_major} {price.currency}/month",
                "item_id": price.item_id,
                "currency_code": price.currency,
                "pricing_model": "flat_fee",
                "price": price.amount_major * 100,
                "period": 1,
                "period_unit": "month",
            },
            f"/item_prices/{price.item_price_id}",
            f"{run_id}:item-price-v2:{price.item_price_id}",
        )
        if index % 10 == 0 or index == len(prices):
            print(f"  catalog {index}/{len(prices)}")


def customer_form(customer: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in customer.items() if key != "demo_number"}


def subscription_form(
    plan: SubscriptionPlan, run_id: str, create_invoice: bool
) -> dict[str, Any]:
    metadata = {
        "demo_seed": "bruin_chargebee_demo",
        "demo_run_id": run_id,
        "demo_lifecycle": plan.lifecycle,
        "demo_cohort_month": plan.cohort_month,
        "demo_churn_month": plan.churn_month,
        "demo_reactivation_month": plan.reactivation_month,
    }
    form: dict[str, Any] = {
        "id": plan.subscription_id,
        "status": plan.status,
        "auto_collection": "off",
        "subscription_items[item_price_id][0]": plan.item_price_id,
        "subscription_items[quantity][0]": 1,
    }
    form.update({f"meta_data[{key}]": value for key, value in metadata.items()})
    for field, value in (
        ("started_at", plan.started_at),
        ("activated_at", plan.activated_at),
        ("current_term_start", plan.current_term_start),
        ("current_term_end", plan.current_term_end),
        ("cancelled_at", plan.cancelled_at),
    ):
        if value is not None:
            form[field] = as_timestamp(value)
    if create_invoice and plan.status in {"active", "non_renewing"}:
        form["create_current_term_invoice"] = True
    return form


def apply_data(
    api: ChargebeeAPI,
    customers: list[dict[str, Any]],
    prices: list[Price],
    subscriptions: list[SubscriptionPlan],
    run_id: str,
    create_invoice: bool,
) -> None:
    ensure_catalog(api, prices, run_id)
    print(f"Creating {len(customers)} customers...")
    for index, customer in enumerate(customers, start=1):
        customer_id = customer["id"]
        api.post_or_get(
            "/customers",
            customer_form(customer),
            f"/customers/{customer_id}",
            f"{run_id}:customer:{customer_id}",
        )
        if index % 25 == 0 or index == len(customers):
            print(f"  customers {index}/{len(customers)}")

    print(f"Importing {len(subscriptions)} historical subscription episodes...")
    for index, plan in enumerate(subscriptions, start=1):
        api.post_or_get(
            f"/customers/{plan.customer_id}/import_for_items",
            subscription_form(plan, run_id, create_invoice),
            f"/subscriptions/{plan.subscription_id}",
            f"{run_id}:subscription-v5:{plan.subscription_id}",
        )
        if index % 25 == 0 or index == len(subscriptions):
            print(f"  subscriptions {index}/{len(subscriptions)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--count",
        type=int,
        default=500,
        help="Number of customers to generate (default: 500).",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of cohort months to cover (default: 6).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for repeatable data (default: 42).",
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime("bruin-demo-%Y%m%d"),
        help="ID prefix; reuse to make retries idempotent.",
    )
    parser.add_argument(
        "--currencies",
        default=",".join(DEFAULT_CURRENCIES),
        help="Comma-separated active currencies (default: USD,EUR,GBP).",
    )
    parser.add_argument(
        "--apply", action="store_true", help="Write the generated data to Chargebee."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt when using --apply.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and validate the plan without making API calls.",
    )
    parser.add_argument(
        "--no-invoices",
        action="store_true",
        help="Do not create the current-term invoice for active subscriptions.",
    )
    parser.add_argument(
        "--add-missing-currencies",
        action="store_true",
        help="Add missing EUR/GBP/CAD currencies with fixed demo exchange rates.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1 or args.count > 10_000:
        raise ValueError("--count must be between 1 and 10000")
    if args.months < 1 or args.months > 24:
        raise ValueError("--months must be between 1 and 24")
    if args.apply == args.dry_run:
        raise ValueError("Choose exactly one of --dry-run or --apply")
    currencies = tuple(
        dict.fromkeys(
            item.strip().upper() for item in args.currencies.split(",") if item.strip()
        )
    )
    if not currencies:
        raise ValueError("--currencies must contain at least one ISO currency code")
    if any(len(currency) != 3 for currency in currencies):
        raise ValueError("--currencies must contain three-letter ISO currency codes")

    customers, prices, subscriptions = generate_data(
        args.count, args.months, args.seed, args.run_id, currencies
    )
    status_counts: dict[str, int] = {}
    for plan in subscriptions:
        status_counts[plan.status] = status_counts.get(plan.status, 0) + 1
    currency_counts = {
        currency: sum(
            1
            for customer in customers
            if customer["preferred_currency_code"] == currency
        )
        for currency in currencies
    }
    print(
        f"Generated {len(customers)} customers and {len(subscriptions)} subscription episodes across {args.months} months."
    )
    print(f"Customer currencies: {currency_counts}")
    print(f"Subscription statuses: {status_counts}")
    print(
        f"Monthly price points: {PRICE_POINTS[0]}-{PRICE_POINTS[-1]} in each native currency"
    )

    if args.dry_run:
        sample = {
            "customer": customers[0],
            "subscription": subscription_form(
                subscriptions[0], args.run_id, not args.no_invoices
            ),
        }
        print("Dry run sample:")
        print(json.dumps(sample, indent=2, default=str))
        return 0

    site = environ.get("CHARGEBEE_SITE", "")
    api_key = environ.get("CHARGEBEE_API_KEY", "")
    if not site or not api_key:
        raise ValueError("--apply requires CHARGEBEE_SITE and CHARGEBEE_API_KEY")
    normalized_site = (
        site.replace("https://", "").replace("http://", "").split(".", 1)[0]
    )
    if "-test" not in normalized_site and not args.yes:
        raise ValueError(
            "Refusing to write to a non-test-looking site; use a test site or pass --yes explicitly"
        )
    if not args.yes:
        answer = input(
            f"Write {len(customers)} customers to Chargebee site '{normalized_site}'? Type 'yes': "
        )
        if answer.strip().lower() != "yes":
            print("Aborted.")
            return 1

    api = ChargebeeAPI(site, api_key)
    configured = active_currencies(api)
    missing = sorted(set(currencies) - configured)
    if missing and args.add_missing_currencies:
        add_currencies(api, set(missing), args.run_id)
        configured = active_currencies(api)
        missing = sorted(set(currencies) - configured)
    if missing:
        raise ValueError(
            f"Currencies not active on {api.site}: {', '.join(missing)}. "
            "Enable Chargebee multi-currency and add them, then rerun."
        )
    apply_data(api, customers, prices, subscriptions, args.run_id, not args.no_invoices)
    print("Chargebee demo data seeded successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ChargebeeAPIError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
