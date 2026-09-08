"""@bruin

name: payments.transactions
description: Generates fake card authorizations into the PostgreSQL system of record so the CDC path runs with no external database. Every row is a pure function of the run window and a row index, so reruns are idempotent; a deterministic slice of earlier windows is re-emitted with a later status to produce genuine UPDATE events for change data capture.
connection: postgres-default
image: python:3.13
owner: risk-platform@example.com

tags:
  - layer:source
  - domain:payments
  - runtime:python
  - demo-seed
domains:
  - payments
meta:
  data_classification: confidential
  grain: one row per card authorization
  source_system: demo-generator
  system_of_record: postgres
  restatement_behaviour: approved rows from earlier windows are re-emitted as refunded or chargeback with a bumped updated_at
  determinism: row values are seeded from (window_start_epoch, row_index) so the asset is idempotent across reruns

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: transaction_id
    type: bigint
    description: Authorization identifier, derived from the run window and row index so it is stable across reruns.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: created_at
    type: timestamptz
    description: Authorization time. This is the event time every downstream rollup buckets on.
    checks:
      - name: not_null
  - name: updated_at
    type: timestamptz
    description: Version cursor for change data capture; bumps whenever the status is restated.
    checks:
      - name: not_null
  - name: merchant_id
    type: bigint
    description: Merchant that submitted the authorization.
    checks:
      - name: not_null
  - name: card_id
    type: bigint
    description: Tokenized card reference. Drawn from a bounded pool so unique-card counts stay well below transaction counts.
    checks:
      - name: not_null
  - name: amount_cents
    type: bigint
    description: Authorization amount in USD minor units. Money is carried as an integer end to end so every rollup sum is exact.
    checks:
      - name: positive
  - name: currency
    type: text
    description: ISO currency code. Always USD in this single-currency demo.
    checks:
      - name: accepted_values
        value:
          - USD
  - name: status
    type: text
    description: Current authorization state.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - approved
          - declined
          - refunded
          - chargeback
  - name: decline_reason
    type: text
    description: Issuer decline reason; null for anything that was not declined.
  - name: card_network
    type: text
    description: Card scheme that routed the authorization.
    checks:
      - name: accepted_values
        value:
          - visa
          - mastercard
          - amex
          - discover
  - name: merchant_category
    type: text
    description: Merchant category used as a reporting dimension.
    checks:
      - name: not_null
  - name: country
    type: text
    description: ISO-2 country of the transaction.
    checks:
      - name: not_null
  - name: is_fraud
    type: boolean
    description: Fraud label supplied by the source. The pipeline monitors this label; it does not compute it.
    checks:
      - name: not_null
  - name: auth_latency_ms
    type: integer
    description: Authorization round-trip latency in milliseconds; the source of the P95 latency KPI.
    checks:
      - name: positive

@bruin"""

import hashlib
import json
import math
import os
import random
from datetime import datetime, timedelta, timezone

# Weighted vocabularies. Weights are relative, not required to sum to one.
CARD_NETWORKS = (("visa", 45), ("mastercard", 32), ("amex", 15), ("discover", 8))

MERCHANT_CATEGORIES = (
    ("grocery", 22),
    ("digital_goods", 18),
    ("travel", 14),
    ("gaming", 12),
    ("restaurants", 12),
    ("electronics", 10),
    ("subscription", 7),
    ("fuel", 5),
)

COUNTRIES = (
    ("US", 48),
    ("GB", 12),
    ("DE", 10),
    ("FR", 7),
    ("NL", 5),
    ("CA", 5),
    ("AU", 4),
    ("BR", 4),
    ("JP", 3),
    ("IN", 2),
)

DECLINE_REASONS = (
    ("insufficient_funds", 45),
    ("do_not_honor", 28),
    ("fraud_suspected", 17),
    ("expired_card", 10),
)

# Typical ticket size per category, in USD.
CATEGORY_TICKET = {
    "grocery": 62.0,
    "digital_goods": 24.0,
    "travel": 540.0,
    "gaming": 18.0,
    "restaurants": 48.0,
    "electronics": 320.0,
    "subscription": 14.0,
    "fuel": 55.0,
}

# Bounded entity pools. Unique cards and merchants must stay far below the
# transaction count, otherwise the non-additive metric lesson in kpi_txn_daily
# has nothing to show.
CARD_POOL = 4_000
MERCHANT_POOL = 260

DECLINE_SHARE = 0.16
BASE_FRAUD_RATE = 0.004
FRAUD_SUSPECTED_FRAUD_RATE = 0.75

# Where an approved authorization lands on its first restatement, and how often a
# refund is subsequently escalated to a chargeback one window later. The two-step
# path is what makes the approved -> refunded -> chargeback lifecycle observable.
RESTATEMENT_OUTCOMES = (("refunded", 78), ("chargeback", 22))
CHARGEBACK_AFTER_REFUND_RATE = 0.18
CHARGEBACK_FRAUD_RATE = 0.62

# transaction_id = window_start_epoch * ID_STRIDE + row_index, so ids stay
# stable across reruns and never collide between windows.
ID_STRIDE = 100_000


def _weighted(rnd, choices):
    total = sum(weight for _, weight in choices)
    threshold = rnd.random() * total
    running = 0.0
    for value, weight in choices:
        running += weight
        if threshold <= running:
            return value
    return choices[-1][0]


def _row_random(window_epoch, index, salt=""):
    """Deterministic RNG for one logical row, independent of iteration order."""
    digest = hashlib.sha256(f"{window_epoch}:{index}:{salt}".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _variables():
    raw = os.environ.get("BRUIN_VARS", "{}")
    try:
        parsed = json.loads(raw) or {}
    except json.JSONDecodeError:
        parsed = {}
    return {
        "txns_per_minute": int(parsed.get("txns_per_minute", 40)),
        "restatement_rate": float(parsed.get("restatement_rate", 0.06)),
        "restatement_windows": int(parsed.get("restatement_windows", 3)),
        "max_seed_rows": int(parsed.get("max_seed_rows", 20_000)),
    }


def _parse_bound(datetime_key, date_key, fallback):
    """Read one interval bound as pipeline wall-clock time.

    Bruin hands the interval over in the wall clock of the machine running the
    pipeline, and renders the same wall clock into the `{{ start_timestamp }}`
    strings the SQL assets use. This pipeline reports in UTC, so the bound is
    taken at face value and labelled UTC rather than converted -- converting here
    would offset the seed from every downstream rollup window.
    """
    raw = os.environ.get(datetime_key) or ""
    if raw:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc)
    raw = os.environ.get(date_key) or ""
    if raw:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    return fallback


def _window():
    """Resolve the run window from Bruin's interval variables."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = _parse_bound("BRUIN_START_DATETIME", "BRUIN_START_DATE", now - timedelta(minutes=1))
    end = _parse_bound("BRUIN_END_DATETIME", "BRUIN_END_DATE", now)
    if end <= start:
        end = start + timedelta(minutes=1)
    return start, end


def _authorization(window_epoch, window_span, index):
    """Build the immutable, as-authorized form of one transaction.

    Pure in (window_epoch, index): a restatement can regenerate the original row
    exactly and change only the fields a status transition would change.
    """
    rnd = _row_random(window_epoch, index)

    # Timestamps stop one second short of the window end. Bruin truncates the
    # inclusive interval end to whole seconds, so anything generated in that final
    # second would fall past this run's capture range and before the next run's,
    # and would never be picked up.
    created_at = datetime.fromtimestamp(
        window_epoch + rnd.random() * (window_span - 1), tz=timezone.utc
    ).replace(microsecond=(rnd.randrange(1000) * 1000))

    # A merchant belongs to exactly one category and country, so both are derived
    # from the merchant rather than drawn independently per transaction.
    merchant_index = rnd.randrange(MERCHANT_POOL)
    merchant_id = 900_000 + merchant_index
    merchant_rnd = _row_random(0, merchant_id, salt="merchant")
    category = _weighted(merchant_rnd, MERCHANT_CATEGORIES)
    country = _weighted(merchant_rnd, COUNTRIES)
    network = _weighted(rnd, CARD_NETWORKS)

    amount = CATEGORY_TICKET[category] * rnd.lognormvariate(0.0, 0.55)
    amount_cents = int(round(min(max(amount, 1.0), 25_000.0) * 100))

    declined = rnd.random() < DECLINE_SHARE
    decline_reason = _weighted(rnd, DECLINE_REASONS) if declined else None
    status = "declined" if declined else "approved"

    fraud_rate = (
        FRAUD_SUSPECTED_FRAUD_RATE
        if decline_reason == "fraud_suspected"
        else BASE_FRAUD_RATE
    )
    is_fraud = rnd.random() < fraud_rate

    # Travel and amex authorizations sit behind slower issuer paths, which gives
    # the P95 latency KPI something to actually discriminate on.
    latency_scale = 1.0 + (0.9 if category == "travel" else 0.0) + (0.5 if network == "amex" else 0.0)
    latency = int(min(max(38.0 * latency_scale * rnd.lognormvariate(0.0, 0.6), 8), 2_500))

    return {
        "transaction_id": window_epoch * ID_STRIDE + index,
        "created_at": created_at,
        "updated_at": created_at,
        "merchant_id": merchant_id,
        "card_id": 5_000_000 + rnd.randrange(CARD_POOL),
        "amount_cents": amount_cents,
        "currency": "USD",
        "status": status,
        "decline_reason": decline_reason,
        "card_network": network,
        "merchant_category": category,
        "country": country,
        "is_fraud": is_fraud,
        "auth_latency_ms": latency,
    }


def _restate(txn, outcome, restated_at, fraud_roll):
    """Move an authorization to a later state in its lifecycle.

    Only the fields a real status transition touches are changed: the
    authorization facts (amount, the authorization timestamp, the dimensions) are
    preserved, exactly as an operational UPDATE would behave.
    """
    txn = dict(txn)
    txn["status"] = outcome
    txn["updated_at"] = restated_at
    if outcome == "chargeback":
        txn["is_fraud"] = txn["is_fraud"] or fraud_roll < CHARGEBACK_FRAUD_RATE
    return txn


def materialize():
    variables = _variables()
    start, end = _window()

    # Bruin hands over an inclusive end truncated to whole seconds, so a
    # ten-minute interval measures 599s rather than 600s. The span is snapped up
    # to the schedule grain because it also defines the id namespace of earlier
    # windows: an off-by-one-second span would point every restatement at a
    # namespace that was never seeded, silently turning restatements into inserts.
    minutes = max(1, math.ceil((end - start).total_seconds() / 60))
    window_span = minutes * 60
    window_epoch = int(start.timestamp())

    count = min(variables["max_seed_rows"], minutes * variables["txns_per_minute"])
    if count >= ID_STRIDE:
        raise ValueError(
            f"seed window would generate {count} rows, which exceeds the {ID_STRIDE}-row "
            "id stride and would collide with the next window; lower txns_per_minute "
            "or max_seed_rows, or shorten the run interval"
        )

    rows = [_authorization(window_epoch, window_span, index) for index in range(count)]

    # Restatements: re-emit a deterministic slice of the authorizations from the
    # previous N windows with a later status. Each selected authorization moves
    # exactly once at its own deterministic distance, and some refunds escalate to
    # a chargeback one window later, which is what produces a genuine
    # approved -> refunded -> chargeback lifecycle in the change log.
    #
    # Rows whose original window was never seeded still land here as late
    # arrivals, which the rollup lookback is responsible for folding into their
    # already-sealed minute.
    rate = variables["restatement_rate"]
    windows = variables["restatement_windows"]
    for back in range(1, windows + 1):
        prior_epoch = window_epoch - back * window_span
        if prior_epoch <= 0:
            continue
        for index in range(count):
            picker = _row_random(prior_epoch, index, salt="restate-pick")
            if picker.random() >= rate:
                continue
            original = _authorization(prior_epoch, window_span, index)
            if original["status"] != "approved":
                continue

            first_back = 1 + picker.randrange(windows)
            mutator = _row_random(prior_epoch, index, salt="restate-apply")
            first_outcome = _weighted(mutator, RESTATEMENT_OUTCOMES)
            escalates = (
                first_outcome == "refunded"
                and first_back < windows
                and mutator.random() < CHARGEBACK_AFTER_REFUND_RATE
            )

            if back == first_back:
                outcome = first_outcome
            elif escalates and back == first_back + 1:
                outcome = "chargeback"
            else:
                continue

            restated_at = datetime.fromtimestamp(
                window_epoch + mutator.random() * (window_span - 1), tz=timezone.utc
            )
            rows.append(
                _restate(original, outcome, restated_at, fraud_roll=mutator.random())
            )

    return rows
