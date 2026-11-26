# Schema Evolution in Ingestr & Seed Assets

[`ingestr`](../assets/ingestr.md) and [`seed`](../assets/seed.md) assets move data from external databases, SaaS APIs, message brokers, and files into destination warehouses. Because upstream sources often change without warning, `ingestr` handles schema drift at runtime before loading each batch.

## Default Behavior (`evolve`)

When you do not set `parameters.schema_contract`, `ingestr` defaults to `evolve`. Before writing data to an existing destination table, `ingestr` compares the incoming source schema (normalized through Apache Arrow types) against the destination table schema:

1. **New columns:** Any column present in the source that does not exist in the destination is added as a nullable column via `ALTER TABLE ... ADD COLUMN` (batched into a single statement on warehouses like BigQuery that support multi-clause `ALTER TABLE`).
2. **Removed source columns:** Columns that exist in the destination table but disappear from the source are kept on the destination table. Future loads write `NULL` into those columns. Internal metadata columns (`_ingestr_loaded_at` and `_ingestr_run_id`) are ignored during removal checks.
3. **Nullability relaxation:** If a non-primary-key column is `NOT NULL` in the destination and arrives as nullable from the source, `ingestr` relaxes the constraint (`ALTER COLUMN ... DROP NOT NULL`) on destinations that support it.
4. **Type widening:** When an existing column's incoming type differs from its destination type, `ingestr` computes a common wider type:
   - Numeric promotions: `int8` -> `int16` -> `int32` -> `int64` -> `float64` -> `decimal`, `float32` -> `float64`, and decimal `(precision, scale)` expansion preserving both integer digits and fractional scale (up to precision 76).
   - String length expansion: bounded strings widen to larger lengths or unbounded `STRING`/`VARCHAR`, and never shrink.
   - Temporal promotions: `date` -> `timestamp_ntz` / `timestamp`.
   - Cross-family fallback: if two types do not share a direct numeric or temporal path (for example, an API field that previously returned integers starts returning strings), `ingestr` widens the logical type to `STRING` or `JSON`. If the destination database supports altering the column to that target type, `ingestr` runs `ALTER TABLE ... ALTER COLUMN`; if the destination rejects that `ALTER COLUMN` conversion in DDL, `ingestr` stops with an error explaining which column and type change could not be applied.

## Controlling Drift with `schema_contract`

Set `parameters.schema_contract` on an `ingestr` asset when you want stricter handling than automatic evolution:

```yaml
name: raw.stripe_invoices
type: ingestr
parameters:
  source_connection: stripe-prod
  source_table: invoices
  destination: bigquery
  schema_contract: freeze
```

| Mode | New columns | Type changes | Source column removed / nullability relaxed | Row behavior |
| --- | --- | --- | --- | --- |
| `evolve` (default) | Added as nullable | Widened via `ALTER TABLE` when supported | Allowed (kept as `NULL` / relaxed) | All rows loaded |
| `freeze` | Fails run | Fails run | Fails run | Fails before loading violating batch (internal `_ingestr_*` columns remain allowed) |
| `discard_row` | Rejected (row dropped) | Rejected (row dropped) | Allowed | Drops rows that contain new columns or non-conforming values |
| `discard_value` | Added as nullable | Rejected (value nulled) | Allowed | Sets individual non-conforming values to `NULL` and loads the rest of the row |

Use `freeze` when feeding regulated or strictly modeled raw tables where any upstream schema change should page an engineer rather than alter the table. Use `discard_value` or `discard_row` on noisy event streams where malformed field values appear occasionally and should not halt ingestion or widen typed columns to `STRING`.

## Pinning Types with `enforce_schema` and `columns`

By default on `ingestr` assets, `columns` entries only supply primary keys (`--primary-key`), masking rules (`--mask`), and `date` hints on incremental keys. Source type inference still decides the remaining column types.

To force specific destination types, set `parameters.enforce_schema: true` alongside `columns`:

```yaml
name: raw.users
type: ingestr
parameters:
  source_connection: mongodb-prod
  source_table: prod.users
  destination: snowflake
  enforce_schema: true
materialization:
  type: table
  strategy: merge
  incremental_key: updated_at

columns:
  - name: _id
    type: string
    primary_key: true
  - name: email
    type: varchar(255)
  - name: balance
    type: decimal(18,4)
  - name: updated_at
    type: timestamp
```

> [!NOTE]
> On `seed` assets (`type: seed`), `enforce_schema` defaults to `true` whenever `columns` are declared. Set `parameters.enforce_schema: "false"` on a seed asset if you prefer type inference over declared column types.

How `enforce_schema: true` interacts with schema evolution:

- **New tables and new columns** use the exact type, `length` (for example `varchar(255)`), and `precision`/`scale` (for example `decimal(18,4)`) declared in `columns`.
- **Existing tables** compare the declared override type against the current destination column:
  - For `decimal` overrides, `ingestr` merges integer digits and scale so an existing wider decimal column is not truncated.
  - For `string`/`varchar(n)` overrides, `ingestr` widens the column length when the override is larger and leaves the column unchanged if the existing destination column is already wider.
  - For other overridden types, `ingestr` emits a type override change (`ALTER COLUMN`) when the destination supports altering the column type.

## Related Ingestion Options

- **Skipping unwanted source columns (`sql_exclude_columns`):** Exclude problematic or sensitive upstream columns during SQL extraction before schema comparison runs:
  ```yaml
  parameters:
    sql_exclude_columns: "legacy_blob,debug_payload"
  ```
- **Schema-less sources without inference (`no_inference: true`):** For CSV, JSONL, or document sources where sampling can guess wrong types across batches, set `no_inference: true` with `enforce_schema: true` so `columns` acts as the fixed source schema.
- **Column renaming (`source_column`):** Map an upstream column name onto a different destination name by setting `source_column` on an entry in `columns` with `enforce_schema: true`. See [Column name mapping](../assets/columns.md#column-name-mapping-ingestr-assets).
