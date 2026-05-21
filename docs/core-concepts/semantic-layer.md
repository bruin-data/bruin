# Semantic Layer

Bruin's semantic layer lets you define reusable business metrics, dimensions, segments, and safe joins in YAML. Semantic models are defined once at the repository level, compile into SQL, and can be queried through the `bruin query` command.

Semantic models live in a `semantic` directory at the repository root, next to `.bruin.yml`:

```text
my-repo/
├─ .bruin.yml
├─ pipelines/
│  └─ daily-orders/
│     ├─ pipeline.yml
│     └─ assets/
│        └─ orders.sql
└─ semantic/
   ├─ orders.yml
   └─ customers.yml
```

Bruin loads every `.yml` and `.yaml` file in this directory tree when a semantic query runs. Model names must be unique across the repository.

## Example

```yaml
schema: v1
name: orders
label: Orders
description: Revenue and order metrics

source:
  table: analytics.orders

primary_key: order_id

joins:
  - name: customers
    relationship: many_to_one
    foreign_key: customer_id

dimensions:
  - name: order_date
    type: time
    granularities:
      day: date_trunc('day', order_date)
      month: date_trunc('month', order_date)
  - name: country
    type: string
  - name: status
    type: string

metrics:
  - name: revenue
    expression: sum(amount)
  - name: order_count
    expression: count(distinct order_id)
  - name: avg_order_value
    expression: "{revenue} / {order_count}"
  - name: completed_revenue
    expression: sum(amount)
    filter: "status = 'completed'"

segments:
  - name: completed
    filter: "status = 'completed'"
```

A joined model can define its own source and dimensions:

```yaml
schema: v1
name: customers

source:
  table: analytics.customers

primary_key: customer_id

dimensions:
  - name: country
    type: string
  - name: segment
    type: string
```

## Querying

Use `bruin query` with `--semantic-model` and select dimensions, metrics, filters, segments, and sorting.

When you have an anchor SQL asset, Bruin uses the asset to find the pipeline, connection, and SQL dialect. The semantic model still comes from the repository-level `semantic` directory:

```bash
bruin query \
  --asset ./pipelines/daily-orders/assets/orders.sql \
  --semantic-model orders \
  --dimension order_date:month \
  --metric revenue \
  --metric avg_order_value \
  --filter '{"dimension":"country","operator":"equals","value":"US"}' \
  --segment completed \
  --sort order_date:asc
```

When you query from a pipeline directory directly, pass the connection explicitly:

```bash
bruin query \
  --pipeline ./pipelines/daily-orders \
  --connection warehouse \
  --semantic-model orders \
  --dimension customers.country \
  --metric revenue \
  --sort revenue:desc \
  --output csv
```

Semantic query mode cannot be combined with `--query`.

## Model Fields

| Field | Required | Description |
|-------|----------|-------------|
| `schema` | No | Schema version. Use `v1`; omitted models default to `v1`. |
| `name` | Yes | Unique model name inside the repository. |
| `label` | No | Human-readable display label. |
| `description` | No | Longer model description. |
| `source.table` | Yes | Table, view, or SQL subquery used as the model source. |
| `primary_key` | No | Primary key used as the default target key for joins into this model. |
| `joins` | No | Relationships from this model to other semantic models. |
| `dimensions` | No | Groupable fields. |
| `metrics` | No | Aggregations and derived metrics. |
| `segments` | No | Reusable filters. |

`source.table` can be a relation name or a parenthesized query:

```yaml
source:
  table: |
    (
      select *
      from analytics.orders
      where deleted_at is null
    ) as orders
```

## Dimensions

Dimensions describe fields that can be selected, grouped, filtered, sorted, or used by windows.

```yaml
dimensions:
  - name: order_date
    type: time
    expression: created_at
    granularities:
      day: date_trunc('day', created_at)
      month: date_trunc('month', created_at)
  - name: country
    type: string
  - name: is_first_order
    type: boolean
    expression: customer_order_number = 1
```

Supported dimension types are `string`, `number`, `boolean`, and `time`. The `type` is optional, but time granularities only apply to dimensions with `type: time`.

If `expression` is omitted, Bruin uses the dimension name as the SQL expression. For example, `country` compiles as `country`.

Use `name:granularity` in queries for time grains:

```bash
bruin query --asset ./pipelines/daily-orders/assets/orders.sql --semantic-model orders --dimension order_date:month --metric revenue
```

## Metrics

Metrics describe aggregations and calculations.

```yaml
metrics:
  - name: revenue
    expression: sum(amount)
  - name: order_count
    expression: count(distinct order_id)
  - name: avg_order_value
    expression: "{revenue} / {order_count}"
```

Use `{metric_name}` references to build derived metrics from other metrics. Bruin expands references when it generates SQL and guards division references with `NULLIF(..., 0)` when the reference appears after `/`.

Metrics can include a SQL filter:

```yaml
metrics:
  - name: completed_revenue
    expression: sum(amount)
    filter: "status = 'completed'"
```

Metric metadata is optional:

```yaml
metrics:
  - name: revenue
    label: Revenue
    group: Finance
    expression: sum(amount)
    format:
      type: currency
      currency: USD
      decimals: 2
```

Supported format types are `number`, `currency`, `percentage`, and `decimal`.

## Window Metrics

Window metrics calculate over the result of a grouped query.

```yaml
metrics:
  - name: revenue
    expression: sum(amount)
  - name: running_revenue
    expression: "{revenue}"
    window:
      type: running_total
      order_by: order_date
      partition_by:
        - country
```

Supported window types:

| Type | Description |
|------|-------------|
| `running_total` | Running sum of the referenced metric. Requires `order_by`. |
| `lag` | Previous value of the referenced metric. Requires `order_by`; `offset` defaults to `1`. |
| `lead` | Next value of the referenced metric. Requires `order_by`; `offset` defaults to `1`. |
| `rank` | SQL `RANK()` over the window. Requires `order_by`. |
| `percent_of_total` | Referenced metric divided by the window total. |

`order_by` and `partition_by` values refer to dimensions on the model.

## Segments

Segments are named filters that can be reused in semantic queries.

```yaml
segments:
  - name: completed
    filter: "status = 'completed'"
  - name: high_value
    filter: "amount >= 100"
```

Use them with `--segment`:

```bash
bruin query --asset ./pipelines/daily-orders/assets/orders.sql --semantic-model orders --metric revenue --segment completed
```

## Filters

Structured filters are passed as JSON:

```bash
bruin query \
  --asset ./pipelines/daily-orders/assets/orders.sql \
  --semantic-model orders \
  --metric revenue \
  --filter '{"dimension":"country","operator":"in","value":["US","DE"]}'
```

Supported operators:

| Operator | Value |
|----------|-------|
| `equals` | Scalar value |
| `not_equals` | Scalar value |
| `gt` | Scalar value |
| `gte` | Scalar value |
| `lt` | Scalar value |
| `lte` | Scalar value |
| `in` | Array value |
| `not_in` | Array value |
| `between` | Two-item array or object with `start` and `end` |
| `is_null` | No value required |
| `is_not_null` | No value required |

Filters can also target joined dimensions:

```bash
bruin query \
  --pipeline ./pipelines/daily-orders \
  --connection warehouse \
  --semantic-model orders \
  --metric revenue \
  --filter '{"dimension":"customers.country","operator":"equals","value":"US"}'
```

## Joins

Joins connect semantic models. The join `name` is also the relation name used in qualified dimensions such as `customers.country`.

```yaml
joins:
  - name: customers
    relationship: many_to_one
    foreign_key: customer_id
```

If `model` is omitted, Bruin uses the join name as the target model name. You can use a different relation name with `model`:

```yaml
joins:
  - name: billing_country
    model: countries
    relationship: many_to_one
    foreign_key: billing_country_id
```

For `foreign_key` joins, Bruin joins `foreign_key` on the current model to `target_key` on the target model. If `target_key` is omitted, Bruin uses the target model's `primary_key`.

```yaml
joins:
  - name: customers
    relationship: many_to_one
    foreign_key: buyer_email
    target_key: email
```

For custom join logic, use `sql`:

```yaml
joins:
  - name: customer_tiers
    relationship: many_to_one
    sql: "{orders}.customer_id = {customer_tiers}.customer_id and {orders}.order_date between {customer_tiers}.valid_from and {customer_tiers}.valid_to"
```

Custom SQL can reference `{source_model_name}`, `{target_model_name}`, and `{join_name}` placeholders. Bruin replaces them with the generated table aliases.

Bruin only traverses `one_to_one` and `many_to_one` joins for semantic queries, because those relationships avoid fanout. `one_to_many` and `many_to_many` are valid relationship values, but they are not automatically used for metric queries.

## CLI Reference

Semantic query flags are part of `bruin query`:

| Flag | Description |
|------|-------------|
| `--semantic-model` | Semantic model name to query. Required for semantic query mode. |
| `--pipeline` | Pipeline directory. Use when no anchor asset is provided. Bruin still loads semantic models from the repository root. |
| `--asset` | SQL asset path used to find the pipeline, connection, and dialect. |
| `--connection` | Connection name. Required with `--pipeline`; optional with `--asset`. |
| `--metric` | Metric to select. Can be passed multiple times. |
| `--dimension` | Dimension to select. Use `name:granularity` for time dimensions. Can be passed multiple times. |
| `--filter` | Structured filter JSON. Can be passed multiple times. |
| `--segment` | Segment to apply. Can be passed multiple times. |
| `--sort` | Sort field. Use `name:asc` or `name:desc`. Can be passed multiple times. |

General `query` flags such as `--output`, `--limit`, `--timeout`, and `--export` also apply.

## Validation Rules

Bruin validates semantic models when it loads the repository semantic catalog:

- `name` and `source.table` are required.
- Metric names, dimension names, and segment names must be unique within a model.
- Metrics require `expression`.
- Segments require `filter`.
- Derived metric references such as `{revenue}` must resolve to known metrics and cannot form cycles.
- Window metrics must reference exactly one metric, for example `expression: "{revenue}"`.
- Joined dimensions must resolve through a safe join path.

Invalid semantic models cause semantic query compilation to fail, so fix validation errors before querying the semantic layer.
