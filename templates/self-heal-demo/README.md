# Bruin Self-Heal Demo

This project is a local DuckDB sandbox for testing Bruin agent troubleshooting skills. It has `demo-seed` to load realistic raw tables, then `demo-pipeline` with normal SQL assets and four intentionally failing branches.

Install the agent skills separately if you want an agent to use them:

```shell
bruin ai skills all
bruin init self-heal-demo
```

## What Is What

Load the raw tables first:

| Pipeline | Assets | Purpose |
| --- | --- | --- |
| `self-heal-demo/demo-seed` | `raw.orders`, `raw.order_status_history`, `raw.order_adjustments`, `raw.fulfillment_events`, `raw.product_catalog` | Seeds DuckDB source tables used by the demo. |
| `self-heal-demo/demo-pipeline` | `staging.orders`, `orders.status_snapshot`, `finance.order_margin`, `fulfillment.daily_activity`, `catalog.product_prices` | Runs the intentionally failing analytics branches. |

The main pipeline uses normal business asset names. Scenario names appear only as tags:

| Scenario tag | Assets | Failure symptom |
| --- | --- | --- |
| `duplicate-investigate` | `staging.orders`, `orders.status_snapshot` | `orders.status_snapshot` fails the uniqueness check on `order_id`. |
| `quality-check-investigate` | `staging.orders`, `finance.order_margin` | `finance.order_margin` fails the positive check on `net_amount`. |
| `freshness-check` | `fulfillment.daily_activity` | `fulfillment.daily_activity` is missing the latest source activity date. |
| `schema-drift-check` | `catalog.product_prices` | `catalog.product_prices` fails because the raw catalog schema changed. |

## Run

From the generated project root, the directory that contains `.bruin.yml` and the `self-heal-demo/` folder:

```shell
bruin run self-heal-demo/demo-seed
bruin validate self-heal-demo/demo-pipeline
```

Validation should pass after seeding because the broken scenarios are runtime data or warehouse issues.

Run one scenario branch at a time:

```shell
bruin run --tag duplicate-investigate self-heal-demo/demo-pipeline || true
bruin run --tag quality-check-investigate self-heal-demo/demo-pipeline || true
bruin run --tag freshness-check self-heal-demo/demo-pipeline || true
bruin run --tag schema-drift-check self-heal-demo/demo-pipeline || true
```

## Proof Queries

After running a branch, use the DuckDB connection to inspect the specific bad row, partition, or schema.

```shell
bruin query --connection self-heal-demo --query "SELECT order_id, count(*) AS row_count FROM orders.status_snapshot GROUP BY 1 HAVING count(*) > 1;"
bruin query --connection self-heal-demo --query "SELECT order_id, gross_amount, adjustment_amount, net_amount FROM finance.order_margin WHERE net_amount <= 0;"
bruin query --connection self-heal-demo --query "SELECT max(event_date) AS modeled_max_date FROM fulfillment.daily_activity;"
bruin query --connection self-heal-demo --query "SELECT max(CAST(event_timestamp AS DATE)) AS raw_max_date FROM raw.fulfillment_events;"
bruin query --connection self-heal-demo --query "DESCRIBE raw.product_catalog;"
```

Expected results:

- The duplicate query returns `order_id = 1002` with `row_count = 2`.
- The quality query returns `order_id = 1003` with a negative `net_amount`.
- The modeled fulfillment max date is `2025-01-02`, while the raw fulfillment max date is `2025-01-03`.
- The catalog description shows `listed_price`; the failing model still expects the previous pricing field.

## Expected Diagnosis And Fix

| Scenario tag | Diagnosis | Recommended fix |
| --- | --- | --- |
| `duplicate-investigate` | Status snapshot joins all status history rows, duplicating multi-status orders. | Filter status history to the current status before joining. |
| `quality-check-investigate` | Adjustments are grouped by customer, applying another order's refund incorrectly. | Aggregate and join adjustments by `order_id`, not `customer_id`. |
| `freshness-check` | Fulfillment model excludes latest delivered activity through narrow event filtering. | Include delivered events or align filtering with activity coverage. |
| `schema-drift-check` | Product price model references removed `unit_price` after source rename. | Select `listed_price AS price` from `raw.product_catalog`. |
