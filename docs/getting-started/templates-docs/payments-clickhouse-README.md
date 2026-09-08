# Payments ClickHouse

A self-contained demo template for near-real-time payments authorization and fraud
monitoring. It is built around the pattern ClickHouse is best at: an operational
PostgreSQL database is the system of record, its changes are captured into ClickHouse, and
Bruin folds them into pre-aggregated serving tables that a Dashboard-as-Code dashboard
reads. The pipeline is scheduled every minute, so the dashboard trails the source by about
a minute.

It runs end to end against two Docker containers — no cloud account, no credentials to
fill in.

```shell
bruin init payments-clickhouse
```

The template uses the `bruin_payments` ClickHouse database and the connection names
`clickhouse-default` and `postgres-default`. Rename them consistently if they do not fit
your project.

| | |
|---|---|
| Source | PostgreSQL `payments.transactions` (operational OLTP) |
| Destination | ClickHouse `bruin_payments` |
| Cadence | every minute (`schedule: "* * * * *"`) |
| Currency | single-currency USD; money carried as integer minor units |
| Timezone | UTC throughout |
| Fraud labels | supplied by the source and monitored, never modelled |

## Quick start

One command starts the containers, replays traffic through the pipeline, and opens the
dashboard:

```shell
bruin init payments-clickhouse
./payments-clickhouse/demo.sh
```

`bruin init` does not set the executable bit, so if that reports "permission denied", run
`chmod +x payments-clickhouse/demo.sh` first, or call it as `bash payments-clickhouse/demo.sh`.
The demo needs `docker`, `bruin` and `python3` on `PATH`, plus `dac` for the dashboard.

| Command | What it does |
|---|---|
| `demo.sh` or `demo.sh up [minutes]` | Everything: start containers, replay, serve |
| `demo.sh replay [minutes]` | Replay only — containers already running, no dashboard |
| `demo.sh serve` | Dashboard only |
| `demo.sh status` | What is running, and what is in the warehouse |
| `demo.sh down` | Stop and delete the containers and their data |

## Project structure

```text
payments-clickhouse/
├── pipeline.yml
├── README.md
├── demo.sh                              # one entry point: start, replay, serve
├── assets/
│   ├── ingestion/
│   │   ├── transactions_seed.py         # writes demo traffic into PostgreSQL
│   │   └── raw_transaction_changes.asset.yml  # ingestr CDC into ClickHouse
│   ├── staging/
│   │   └── stg_transaction_changes.sql  # typed, non-null projection of the change log
│   ├── rollups/
│   │   ├── rollup_txn_1m.sql            # per-minute pre-aggregate (time_interval + 15m lookback)
│   │   └── rollup_txn_1h.sql            # hourly grain, summed from the minute grain
│   ├── kpi/
│   │   └── kpi_txn_daily.sql            # daily KPIs; where additivity stops being free
│   └── serving/
│       └── serving_realtime_risk.sql    # today live ∪ sealed history, for the dashboard
├── dashboards/payments_risk.yml         # Dashboard-as-Code dashboard
├── semantic/payments_risk.yml           # semantic model the dashboard reads
├── docker/                              # PostgreSQL + ClickHouse stack and local config
└── docs/                                # ingestion boundaries and the Mode 2 analysis
```

## Architecture

```text
PostgreSQL · payments.transactions          ← system of record (Python seed writes here)
     │  ingestr, cursor on updated_at, append-only
     ▼
bruin_payments.raw_transaction_changes      ← append-only change log, MergeTree
     ▼
bruin_payments.stg_transaction_changes      ← typed, non-null, LowCardinality view
     ▼
bruin_payments.rollup_txn_1m                ← per-minute pre-aggregate, all measures additive
     │                        ╲
     ▼                          ▼
rollup_txn_1h                  kpi_txn_daily  ← additive sums + re-derived uniques and P95
     └──────────────┬───────────┘
                    ▼
bruin_payments.serving_realtime_risk        ← today live ∪ sealed history + derived rates
                    ▼
     dashboards/payments_risk.yml + semantic/payments_risk.yml  →  dac serve
```

## The two ideas worth taking away

1. **Late-arriving restatements and the lookback window.** A payment is not immutable — an
   authorization is approved, then refunded, then charged back — and each transition is
   captured in a later run than the one that first recorded it. Every rollup buckets on
   `created_at` (the authorization time), and `rollup_txn_1m` declares
   `interval_modifiers: { start: -15m }` so each run reprocesses the previous 15 minutes
   and rewrites the minute a restatement belongs to. The lookback is the whole correction
   budget: widen it to buy a longer window and pay for it in recompute on every run.

2. **Additive vs non-additive measures.** Counts and volumes survive being summed, so the
   hourly and daily grains are plain `sum()` queries over the minute rollup — which is why
   a rollup cascade is cheap. Unique cards and P95 latency do not survive it, so
   `kpi_txn_daily` re-reads the change log for exactly those two columns and
   `serving_realtime_risk` returns `NULL` for them on the live day rather than a plausibly
   wrong number.

The template README covers the full architecture, the governance checks (81 across the
pipeline), the ingestion boundaries that shaped it, and a `docs/mode2-aggregating-mergetree.md`
analysis of what it would take to make the two non-additive KPIs additive.
