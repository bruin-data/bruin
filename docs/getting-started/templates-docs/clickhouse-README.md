# Bruin + ClickHouse feature showcase

This pipeline is a compact tour of Bruin on ClickHouse. It combines SQL transformations, a Python materialization, versioned seed data, a PostgreSQL source and sensor, ingestr replication, lineage, governance metadata, quality checks, and a SQL unit test.

The deterministic core runs against the endpoint configured as **clickhouse-default**. The PostgreSQL branch uses **postgres-default** and is tagged **requires-postgres-default**.

## Template directory

~~~text
clickhouse/
├── .bruin.yml
├── README.md
├── pipeline.yml
└── assets/
    ├── data_definitions/
    │   ├── country_targets.asset.yml
    │   ├── country_targets.csv
    │   └── order_events_contract.sql
    ├── ingestion/
    │   ├── postgres_order_daily_monitor.sql
    │   ├── postgres_orders_sensor.asset.yml
    │   ├── postgres_orders_source.asset.yml
    │   └── raw_postgres_orders.asset.yml
    ├── materialization_types/
    │   ├── country_revenue.sql
    │   ├── country_revenue_leaderboard.sql
    │   ├── customer_order_summary.sql
    │   ├── daily_order_snapshot.sql
    │   ├── order_change_log.sql
    │   ├── pipeline_daily_snapshot.sql
    │   ├── raw_customers.sql
    │   └── raw_orders.sql
    └── python/
        └── customer_regions.py
~~~

> The tree above shows the template's own files. After `bruin init`, the `.bruin.yml` is merged into the `.bruin.yml` at your **project root** (never inside the pipeline folder). See [Project configuration](../../core-concepts/project.md).

## Pipeline at a glance

~~~text
Optional PostgreSQL branch

pg.source -> pg.sensor.query -> ingestr -> ClickHouse view

Deterministic ClickHouse core

SQL raw assets + seed + Python asset
  -> time_interval staging model
  -> delete+insert customer mart
  -> country revenue table and view
  -> operational snapshot
~~~

## Asset types and features

| Area | Example assets | Features demonstrated |
| --- | --- | --- |
| SQL materializations | materialization_types/raw_customers.sql, materialization_types/daily_order_snapshot.sql, materialization_types/country_revenue.sql | Table and view materialization; create+replace, time_interval, delete+insert, append, truncate+insert, and view strategies. |
| Python and seed | python/customer_regions.py, data_definitions/country_targets.asset.yml | A Python function that returns rows and version-controlled CSV reference data with an enforced schema. |
| Source and sensor | ingestion/postgres_orders_source.asset.yml, ingestion/postgres_orders_sensor.asset.yml | An external source definition and a readiness gate before ingestion. |
| Ingestr | ingestion/raw_postgres_orders.asset.yml | Incremental PostgreSQL-to-ClickHouse replication using merge and an explicit high-water mark. |
| DDL and physical layout | data_definitions/order_events_contract.sql | Explicit ClickHouse DDL with a partition key and composite ClickHouse sorting key. |
| Governance | Most assets | Owners, tags, domains, metadata, column descriptions, and classification labels. |
| Quality and testing | materialization_types/country_revenue.sql, materialization_types/customer_order_summary.sql | Built-in and custom quality checks, including a non-blocking check for customer-summary countries missing from the target seed, plus a mocked SQL unit test. |
| Lineage | All dependent assets | Execution ordering and upstream/downstream inspection through Bruin lineage. |

## Operating ClickHouse writes

### Retry behavior

**order_change_log** deliberately uses append materialization: rerunning the same interval adds another batch. It is useful for seeing append semantics, but it is not an idempotent retry pattern.

**raw_postgres_orders** demonstrates an ingestr merge into a ReplacingMergeTree table. That engine's eventual row reconciliation is separate from protecting an individual insert retry. Before enabling automatic retries in a production pipeline, decide which writes must be idempotent and configure the destination accordingly.

ClickHouse can deduplicate a retried insert for MergeTree-family tables when deduplication is enabled. The retry needs the same input and settings, and it must happen before the deduplication window expires. Check the query-level **insert_deduplicate=1** setting and the table's deduplication-window settings for your service. See ClickHouse's [deduplicating inserts on retries](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries) guide for the configuration and limitations.

## Connections

### ClickHouse Cloud

Configure **clickhouse-default** in your project .bruin.yml for the intended Cloud service and database. The run commands work unchanged. Choose the appropriate Bruin environment and do not run a full refresh against production by default.

### Optional PostgreSQL branch

Configure **postgres-default** before running the external-source path. Its source definition, sensor condition, and ingestion mapping live with the implementation in assets/ingestion/.

## Run the pipeline

Run these commands from the project root, after initializing the clickhouse template. Substitute another target configuration with --config-file when needed.

Validate the showcase:

~~~bash
bruin validate clickhouse --fast
~~~

Run the complete showcase when both connections are configured. A full refresh rebuilds destination tables, so use an explicitly intended non-production environment:

~~~bash
bruin run clickhouse/pipeline.yml --environment default --full-refresh --start-date 2024-04-01 --end-date 2024-04-15
~~~

If PostgreSQL is not available, bootstrap only the deterministic ClickHouse core:

~~~bash
bruin run clickhouse/pipeline.yml --environment default --exclude-tag requires-postgres-default --full-refresh --start-date 2024-04-01 --end-date 2024-04-15
~~~

Run a normal incremental window and its downstream models:

~~~bash
bruin run clickhouse/assets/materialization_types/daily_order_snapshot.sql --downstream --environment default --start-date 2024-04-16 --end-date 2024-04-30
~~~

Run data-quality checks without rebuilding the model:

~~~bash
bruin run clickhouse/assets/materialization_types/country_revenue.sql --only checks --environment default
~~~

Run the SQL unit test:

~~~bash
bruin unit-test clickhouse/assets/materialization_types/country_revenue.sql --environment default
~~~

The unit-test command resolves its connection from .bruin.yml and does not accept --config-file.

Inspect the deterministic-core and PostgreSQL lineage branches:

~~~bash
bruin lineage clickhouse/assets/materialization_types/country_revenue.sql --full
bruin lineage clickhouse/assets/ingestion/postgres_order_daily_monitor.sql --full
~~~
