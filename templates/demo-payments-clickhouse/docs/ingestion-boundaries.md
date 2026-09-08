# Ingestion boundaries: what Bruin and ingestr can capture into ClickHouse today

The requirements for this pipeline called for log-based change data capture from
PostgreSQL into ClickHouse, in batch mode, using the `merge` strategy. Two of those
three are not available for a ClickHouse destination in the version of the toolchain
this pipeline was built against. This page records exactly what was tried, what the
tools said, and what the pipeline does instead.

Versions under test: Bruin CLI `v0.11.728`, ingestr `1.1.33`, ClickHouse `24.8`,
PostgreSQL `16`.

## 1. Log-based CDC cannot target ClickHouse

Bruin exposes PostgreSQL logical replication through `parameters.cdc: "true"` on an
`ingestr` asset. Omitting `stream` gives the bounded, batch-mode behaviour this
pipeline wants: read to the current WAL position and exit.

That configuration is rejected before any data moves:

```
Error: destination scheme "clickhouse" cannot safely run managed CDC:
destination-managed state with fencing, pruning, and truncation is not supported
```

ingestr carries a family of these guards, each naming a capability the destination
must provide before it will run managed CDC:

- `destination-managed state with fencing, pruning, and truncation is not supported`
- `atomic destination-table claims are not supported`
- `CDC-aware merge is not supported`
- `preserving unchanged TOAST columns is not supported`

The guard is destination-specific, not a misconfiguration here. The same source URI
against DuckDB works, including the initial snapshot:

```bash
ingestr ingest \
  --source-uri "postgres+cdc://bruin:bruin@127.0.0.1:5432/payments?sslmode=disable" \
  --source-table payments.transactions \
  --dest-uri "duckdb:///probe.duckdb" \
  --dest-table raw.transactions \
  --incremental-strategy merge
# -> Total Rows 471, Ingestion completed successfully
```

ClickHouse has no `UPDATE`/`DELETE` in the transactional sense and no atomic
multi-statement claim, which is what those guards are asking for. This is a genuine
architectural gap rather than an oversight, and it is the reason the pipeline captures
changes with a timestamp cursor instead.

`docker/postgres-init.sql` still sets `wal_level=logical`, `REPLICA IDENTITY FULL` and
a publication. Those are the documented prerequisites, they cost nothing, and they are
what you would already have in place on the day the ClickHouse destination becomes
supported.

## 2. The `merge` strategy loses rows on ClickHouse

Cursor-based capture with `materialization.strategy: merge` runs, reports success, and
silently drops almost everything:

| | |
|---|---|
| Rows extracted from PostgreSQL | 927 |
| Rows reported by ingestr | 927 |
| Rows present in ClickHouse afterwards | **1** |

The surviving row was the last one in the batch. The same load with
`strategy: create+replace` (a `--full-refresh` run) landed all 927 correctly, so the
extract side is fine; the defect is in the ClickHouse merge path. ClickHouse `ALTER
TABLE ... DELETE` is an asynchronous mutation, and a delete-then-insert merge built on
it will race its own insert.

Because a silent 99.9% data loss is worse than an error, this pipeline does not use
`merge` for ingestion.

## 3. Schema evolution rejects PostgreSQL `numeric` on the second run

With a `numeric(18,2)` amount column, the first run creates the ClickHouse table as
`Nullable(Decimal(18, 2))` and succeeds. Every subsequent incremental run fails:

```
Error: schema evolution failed: failed to compare schemas: column amount:
decimal widening requires precision 40 (integer digits 38 + scale 2),
maximum supported precision is 38
```

The comparison re-infers the incoming column at the maximum precision (38 integer
digits) and then tries to widen to accommodate scale 2, asking for precision 40. None
of `enforce_schema`, `schema_contract: freeze`, or `sql_backend: sqlalchemy` avoids it.

The pipeline stores money as `bigint` **minor units** (`amount_cents`) instead. That
sidesteps the defect, and it is independently the better choice: Stripe, Adyen and most
card processors represent amounts in minor units precisely so that no decimal or
floating-point rounding can enter the ledger. Every rollup sums exact integers, and USD
is derived once at the serving boundary.

## What the pipeline actually does

`assets/ingestion/raw_transaction_changes.asset.yml` is an **append-only change log**:

- cursor-based capture, `incremental_key: updated_at`, so each run collects every row
  whose version cursor moved inside the run interval, new authorizations and
  restatements alike;
- `strategy: append` with `engine: merge_tree`, so ClickHouse is only ever inserted
  into and never mutated;
- consumers collapse to the latest version at read time with
  `argMax(..., updated_at) GROUP BY transaction_id`.

That is the canonical ClickHouse pattern for replicated mutable sources, and it is what
the requirements' own open question O-5 anticipated. What it costs, relative to
log-based CDC:

| Property | Log-based CDC | This pipeline |
|---|---|---|
| New rows | captured | captured |
| Status restatements | captured | captured, because `updated_at` moves |
| Hard `DELETE`s | captured | **not captured** — a deleted row simply stops being updated |
| Changes within one interval | every intermediate version | only the last version in that interval |
| Source requirement | replication slot | an indexed, reliably-bumped `updated_at` |

For a payments ledger, rows are not hard-deleted, so the first gap is not material
here. The second is: if a transaction moves `approved -> refunded -> chargeback` inside
a single one-minute interval, only `chargeback` is captured. Since the pipeline reports
current state rather than a transition audit trail, that is the correct outcome; a
pipeline that needed the full transition history would need log-based CDC.

### Replaying an interval duplicates rows

`append` has no key, so re-running a completed interval appends rows the table already
holds. This is harmless — every consumer goes through `argMax ... GROUP BY
transaction_id`, so duplicate copies of one version collapse to that version, and the
reconciliation checks stay green — but it costs storage. To see how much:

```sql
SELECT count() - uniqExact((transaction_id, updated_at)) AS duplicate_rows
FROM bruin_payments.raw_transaction_changes
```

`bruin run ... --full-refresh` over the full history clears them.

Using `engine: replacing_merge_tree` would *not* fix this: ingestr sets the sorting key
from the primary key alone, so ClickHouse would deduplicate on `transaction_id` and
discard genuine earlier versions. `merge_tree` plus read-time collapse is deliberate.

## Product feedback

1. **A supported ClickHouse path for log-based CDC.** The blocking capabilities are
   destination-managed CDC state, atomic table claims, CDC-aware merge and TOAST
   passthrough. An append-only CDC mode that writes a versioned change log and leaves
   collapsing to the reader would fit ClickHouse natively and needs none of them.
2. **`merge` on ClickHouse should fail loudly or be withdrawn.** Reporting 927 rows
   loaded while persisting 1 is the worst possible failure mode.
3. **Decimal re-inference on incremental runs.** The widening comparison should reuse
   the destination's existing precision and scale instead of re-deriving the maximum.
4. **First-class `aggregating_merge_tree` and a `materialized_view` strategy** — see
   [mode2-aggregating-mergetree.md](mode2-aggregating-mergetree.md).
