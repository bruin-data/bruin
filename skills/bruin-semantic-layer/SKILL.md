---
name: bruin-semantic-layer
description: Use when creating, editing, reviewing, or troubleshooting Bruin semantic layer models, semantic query CLI usage, metric and dimension definitions, joins, segments, filters, windows, semantic quality checks, or semantic-layer tests and docs in a Bruin repository.
---

# Bruin Semantic Layer

## Workflow

1. Find the repository root and inspect `semantic/` before editing. Bruin loads every `.yml` and `.yaml` model under the repository-level `semantic/` directory next to `.bruin.yml`.
2. Use local source of truth before guessing: `docs/core-concepts/semantic-layer.md`, `docs/commands/query.md`, `docs/commands/semantic.md`, `semantic-engine/model.go`, `semantic-engine/engine.go`, `semantic-engine/graph.go`, and `semantic-engine/checks.go`.
3. Keep model names unique across the semantic catalog. New models should set `schema: v1`, although omitted schema defaults to `v1`.
4. Prefer reusable, business-named metrics, dimensions, and segments. Avoid putting dashboard-specific logic into one large SQL query.
5. Validate with `bruin semantic validate`, run `bruin semantic check` when a connection is available, then run the repository-required final checks before finishing.

## Model Pattern

Create or edit files under `semantic/`:

```yaml
schema: v1
name: orders
label: Orders
description: Revenue and order metrics

source:
  table: analytics.orders
  connection: warehouse

primary_key: order_id

joins:
  - name: customers
    relationship: many_to_one
    foreign_key: customer_id

dimensions:
  - name: order_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: amount
    type: number
  - name: order_date
    type: time
    expression: created_at
    granularities:
      day: date_trunc('day', created_at)
      month: date_trunc('month', created_at)
  - name: country
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value: [US, DE]
  - name: is_first_order
    type: boolean
    expression: customer_order_number = 1

metrics:
  - name: revenue
    expression: sum(amount)
    format:
      type: currency
      currency: USD
      decimals: 2
    checks:
      - name: positive
  - name: order_count
    expression: count(distinct order_id)
  - name: avg_order_value
    expression: "{revenue} / {order_count}"
  - name: completed_revenue
    expression: sum(amount)
    filter: "status = 'completed'"
  - name: running_revenue
    expression: "{revenue}"
    window:
      type: running_total
      order_by: order_date
      partition_by:
        - country

segments:
  - name: completed
    filter: "status = 'completed'"

checks:
  - name: completed_revenue_matches_finance
    query:
      metrics: [revenue]
      segments: [completed]
    value: 730
  - name: no_negative_amounts
    query:
      dimensions: [order_id]
      filters:
        - dimension: amount
          operator: lt
          value: 0
    count: 0
```

## Default Model Behavior

- Exactly one of `source.table` or `source.query` is required.
- `source.table` can be a relation name or a parenthesized SQL subquery with an alias.
- `source.query` is compiled as `(query) AS <model_name>`.
- `source.connection` is optional. `bruin semantic validate`, `bruin semantic check`, and `bruin query --pipeline` use it when `--connection` is not passed.
- `label`, `description`, `group`, `hidden`, and `format` metadata help consumers but do not change SQL generation.
- Dimension `expression` defaults to the dimension `name`.
- Dimension `type` can be `string`, `number`, `boolean`, or `time`; only `time` dimensions can use granularities.
- Query time dimensions as `name:granularity`, for example `order_date:month`.
- `hidden: true` hides a dimension from UI-style consumers but does not make it unqueryable.
- Metrics, dimensions, and segments share a model-level namespace; duplicate names are invalid.
- Metric, dimension, and segment names should be stable API names, not display labels.

## Metric Behavior

- Base metrics are SQL aggregate expressions such as `sum(amount)` or `count(distinct order_id)`.
- Derived metrics use `{metric_name}` references. References must resolve and cannot form cycles.
- Division by a referenced metric is guarded with `NULLIF(..., 0)` during SQL generation.
- Metric `filter` wraps the metric aggregation. For example, `sum(amount)` with a filter becomes a conditional aggregate.
- A metric can mix raw aggregation and `{refs}` for simple queries, but do not put that mixed metric in a window metric dependency chain.
- Supported format metadata types are `number`, `currency`, `percentage`, and `decimal`.

## Window Metrics

Window metrics calculate after an inner grouped query and must use `expression: "{base_metric}"`.

- Supported `window.type` values: `running_total`, `lag`, `lead`, `rank`, and `percent_of_total`.
- `running_total`, `lag`, `lead`, and `rank` require `window.order_by` referencing a dimension.
- `lag` and `lead` default `offset` to `1` when omitted or set to zero.
- `partition_by` entries must reference dimensions.
- `percent_of_total` does not require `order_by`; it can use `partition_by`.
- Filters and segments are applied inside the inner query before the window expression runs.
- Window metrics cannot have `checks`, because they return one row per `order_by` group. Use a model check instead.

## Filters And Segments

- Segments are named SQL filters and are applied with `--segment`.
- Structured filters use JSON with `dimension`, `operator`, and optional `value`.
- Supported operators: `equals`, `not_equals`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `between`, `is_null`, `is_not_null`.
- `between` accepts a two-item array or an object with `start` and `end`.
- Filters can also use raw `expression`; use this sparingly because it bypasses structured validation.
- Filters or segments that reference metrics or aggregates compile into `HAVING`; dimension-only filters compile into `WHERE`.
- Filter values are SQL-formatted by type; strings are single-quoted and escaped.

## Joins

- Join `name` is the relation prefix used in queries, such as `customers.country`.
- If `model` is omitted, Bruin uses the join name as the target model name.
- Valid relationships are `one_to_one`, `many_to_one`, `one_to_many`, and `many_to_many`.
- Only `one_to_one` and `many_to_one` are automatically traversed in semantic queries because they avoid fanout.
- A join needs either `foreign_key` or custom `sql`.
- For `foreign_key` joins, Bruin joins the current model's `foreign_key` to the target model's `target_key`; if `target_key` is omitted, the target model must define `primary_key`.
- Custom join SQL can reference aliases such as `{orders}`, `{customers}`, or the join name placeholder.

## Quality Checks

- Dimension checks (`dimensions[].checks`) work like column checks: `not_null`, `unique`, `positive`, `non_negative`, `negative`, `min`, `max`, `accepted_values`, and `pattern`. They test every row of the model source, and all of them except `not_null` ignore nulls.
- Metric checks (`metrics[].checks`) test the metric computed over the whole model: `not_null`, `positive`, `non_negative`, `negative`, `min`, `max`, and `equals`. A null metric fails every metric check.
- Model checks (top-level `checks`) need a unique `name` and a `query`. The query is a semantic query with `dimensions`, `metrics`, `filters`, `segments`, `sort`, and `limit`, and accepts the `name:granularity` and `name:direction` shorthands. There is no raw SQL option.
- To assert that rows should not exist, select a dimension, filter down to the bad rows, and set `count: 0`. To find unmatched join rows, filter on a null joined dimension.
- A model check sets either `value` or `count`, never both. `count` wraps the query in `SELECT count(*)`. `value` can be:
  - a scalar, for a single-column, single-row result;
  - a mapping, for exactly one row;
  - a list, for the full result set, with one mapping per row keyed by dimension or metric name. Single-column queries can use bare values.
- Only the listed columns are compared, and rows are compared in order only when the query has a `sort`. Without `value` or `count`, the check expects `0`.
- `min`, `max`, and `equals` require a value, `accepted_values` requires a non-empty list, and `pattern` requires a string. Other checks reject a value. A check name can appear only once per dimension or metric.
- `bruin semantic validate` validates definitions, and dry-runs the compiled SQL when it finds a connection (`--connection`, then `source.connection`). A model without a usable connection only gets structural validation and a warning.
- `bruin semantic check` runs the checks and exits non-zero on any failure. Use `--model` to limit it to specific models and `--output json` for machine-readable results.

## Query Pattern

Use an anchor SQL asset when Bruin should infer the pipeline, connection, and dialect:

```bash
bruin query \
  --asset ./pipelines/daily-orders/assets/orders.sql \
  --semantic-model orders \
  --dimension order_date:month \
  --metric revenue \
  --filter '{"dimension":"country","operator":"equals","value":"US"}' \
  --segment completed \
  --sort revenue:desc \
  --output json
```

Use a pipeline path when there is no anchor asset. Pass the connection explicitly, or leave out `--connection` if the model sets `source.connection`:

```bash
bruin query \
  --pipeline ./pipelines/daily-orders \
  --connection warehouse \
  --semantic-model orders \
  --dimension customers.country \
  --metric revenue \
  --sort customers.country:asc
```

Semantic query mode requires at least one dimension or metric and cannot be combined with `--query`. Sort direction defaults to `asc`; `--limit` applies only when greater than zero.

## Validation Notes

- Required model fields: `name` and exactly one of `source.table` or `source.query`.
- `bruin validate` loads `semantic/` next to `.bruin.yml` (`semantic-layer-valid`, fast) and dry-runs query sources (`semantic-query-dry-run`).
- Required item fields: dimension `name`, metric `name` and `expression`, segment `name` and `filter`.
- Window metrics must reference exactly one metric, for example `expression: "{revenue}"`.
- Window `order_by` and `partition_by` values must reference dimensions on the model.
- Joined dimensions must resolve through a safe join path.
- Unknown metrics, dimensions, segments, filter operators, sort fields, and granularities fail semantic query compilation.

For behavior changes, update the implementation, tests, and user-facing docs together: `semantic-engine/`, `pkg/semanticcheck/`, `cmd/semantic.go`, `docs/core-concepts/semantic-layer.md`, `docs/commands/query.md`, and `docs/commands/semantic.md`.
