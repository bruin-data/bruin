# Schema Evolution

Schemas rarely stay fixed for long. SaaS APIs add fields, upstream databases widen integer or decimal columns, and SQL transformations gain new metrics over time.

Assets that rebuild their target table from scratch on every run (`create+replace`) pick up schema changes automatically because the old table is replaced. Incremental tables (`append`, `merge`, `delete+insert`, `time_interval`, `truncate+insert`, SCD2, and `ddl`) keep the existing destination table across runs, so Bruin needs a clear policy for reconciling the incoming data with the table that is already in the warehouse.

Bruin provides two distinct schema evolution mechanisms depending on the asset type, because raw data ingestion and warehouse SQL transformations have different failure tolerances:

- **[Ingestr & Seed Assets](./ingestr.md):** Designed for moving data across external systems where you often do not control the upstream schema. Schema evolution is **enabled by default** (`schema_contract: evolve`) and favors keeping loads running, including relaxing nullability and widening conflicting types to `STRING` or `JSON` when the destination allows it.
- **[SQL Assets](./sql.md):** Designed for analytical tables inside a warehouse (`duckdb.sql` and `bq.sql`) where downstream models depend on exact SQL types. Schema evolution is **opt-in** (`schema_evolution.enabled: true`) and applies only lossless, backwards-compatible `ALTER TABLE` changes, failing before any data is written when a change would lose precision or break existing constraints.

## At a Glance

| Aspect | Ingestr & Seed Assets | SQL Assets (`duckdb.sql`, `bq.sql`) |
| --- | --- | --- |
| **Default behavior** | Enabled (`parameters.schema_contract: evolve`) | Disabled (`schema_evolution.enabled: false`) |
| **How to configure** | `parameters.schema_contract` and `parameters.enforce_schema` | `schema_evolution.enabled: true` on the asset or pipeline `default` |
| **Type system** | Cross-platform Arrow logical types | Native warehouse SQL types (`STRUCT`, `ARRAY`, `MAP`, `NUMERIC`, `ENUM`, etc.) |
| **New columns** | Added automatically as nullable columns (`evolve` and `discard_value`) | Added via `ALTER TABLE ... ADD COLUMN` |
| **Removed / omitted columns** | Kept in destination table; new rows write `NULL` | Kept in destination table; `INSERT` targets remaining columns by name |
| **Type widening** | Widens integers, floats, decimals, dates, and string lengths; falls back to `STRING` or `JSON` on cross-type conflicts | Widens only lossless, engine-supported paths (for example `INT64` to `NUMERIC`, `INT` to `BIGINT`, `STRING(10)` to `STRING(50)`) |
| **Incompatible type changes** | Widens to `STRING`/`JSON` under `evolve`, or rejects/filters under `freeze`, `discard_row`, and `discard_value` | Fails before altering or writing to the table |

## Shared Safety Rule: Columns Are Never Auto-Dropped

Neither `ingestr` nor SQL schema evolution ever runs `ALTER TABLE ... DROP COLUMN` automatically when a column disappears from a source payload, `SELECT` query, or `columns` list.

Dropping a column on an incremental table permanently deletes historical values that may no longer exist in the upstream window and breaks downstream queries reading that table. Instead:

1. The historical column remains on the destination table with all prior rows intact.
2. Subsequent incremental runs omit that column so new rows receive `NULL` (or the column's `DEFAULT` value).
3. When you intentionally want to remove obsolete columns or apply a breaking type change (such as narrowing `BIGINT` to `INT` or converting `STRING` to `INT64`), rebuild the table explicitly with `bruin run --full-refresh`.
