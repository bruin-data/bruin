# Bruin Self-Heal Demo

This pipeline is a local DuckDB sandbox for testing Bruin agent troubleshooting skills. It contains four independent scenario branches with intentional data problems.

Install the agent skills separately if you want an agent to use them:

```shell
bruin ai skills all
bruin init self-heal-demo
```

## What Is What

All scenarios read from the same clean source table, `source.orders`. Each scenario writes to its own schema so the asset table names can stay normal:

| Scenario tag | Assets | Intentional issue |
| --- | --- | --- |
| `duplicate-investigate` | `duplicate.silver_orders`, `duplicate.gold_order_report` | The silver table repeats `order_id = 1002`, causing the gold table uniqueness check to fail. |
| `quality-check-investigate` | `quality.silver_orders`, `quality.gold_order_report` | The silver table makes `order_id = 1003` negative, causing the gold table positive amount check to fail. |
| `freshness-check` | `freshness.silver_orders`, `freshness.gold_order_report` | The silver table filters out `transaction_date = DATE '2025-01-03'`, causing the gold latest-partition check to fail. |
| `schema-drift-check` | `schema_drift.bronze_orders`, `schema_drift.silver_orders` | The bronze table emits `gross_amount`, while the silver table still selects `orders.amount`. |

The files keep the scenario name in the filename so each branch is easy to find:

| File | Asset |
| --- | --- |
| `assets/source_orders.sql` | `source.orders` |
| `assets/duplicate_silver_orders.sql` | `duplicate.silver_orders` |
| `assets/duplicate_gold_order_report.sql` | `duplicate.gold_order_report` |
| `assets/quality_silver_orders.sql` | `quality.silver_orders` |
| `assets/quality_gold_order_report.sql` | `quality.gold_order_report` |
| `assets/freshness_silver_orders.sql` | `freshness.silver_orders` |
| `assets/freshness_gold_order_report.sql` | `freshness.gold_order_report` |
| `assets/schema_drift_bronze_orders.sql` | `schema_drift.bronze_orders` |
| `assets/schema_drift_silver_orders.sql` | `schema_drift.silver_orders` |

## Run

From the generated project root, the directory that contains `.bruin.yml` and the `self-heal-demo/` folder:

```shell
bruin validate self-heal-demo
```

Validation should pass because the broken scenarios are runtime data or warehouse issues.

Run one scenario branch at a time:

```shell
bruin run --tag duplicate-investigate self-heal-demo || true
bruin run --tag quality-check-investigate self-heal-demo || true
bruin run --tag freshness-check self-heal-demo || true
bruin run --tag schema-drift-check self-heal-demo || true
```

## Proof Queries

After running a branch, use the DuckDB connection to inspect the specific bad row, partition, or schema.

```shell
bruin query --connection self-heal-demo --query "SELECT order_id, count(*) AS row_count FROM duplicate.gold_order_report GROUP BY 1 HAVING count(*) > 1;"
bruin query --connection self-heal-demo --query "SELECT order_id, amount FROM quality.gold_order_report WHERE order_id = 1003;"
bruin query --connection self-heal-demo --query "SELECT max(transaction_date) AS max_transaction_date FROM freshness.gold_order_report;"
bruin query --connection self-heal-demo --query "DESCRIBE schema_drift.bronze_orders;"
```

Expected results:

- The duplicate query returns `order_id = 1002` with `row_count = 2`.
- The quality query returns `order_id = 1003` with a negative amount.
- The freshness query returns `2025-01-02` instead of `2025-01-03`.
- The schema description shows `gross_amount`, while `schema_drift.silver_orders` fails because it still references `amount`.
