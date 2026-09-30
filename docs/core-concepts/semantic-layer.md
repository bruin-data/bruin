# Semantic Layer

Bruin's semantic layer lets you define reusable business metrics, dimensions, segments, safe joins, and quality checks in YAML. You define semantic models once at the repository level. Bruin compiles them into SQL, and you query them with the `bruin query` command. To validate models and run their quality checks, use the [`bruin semantic`](/commands/semantic) command.

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
  connection: warehouse

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
    checks:
      - name: not_null
      - name: accepted_values
        value: [US, DE]
  - name: status
    type: string

metrics:
  - name: revenue
    expression: sum(amount)
    checks:
      - name: positive
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

checks:
  - name: completed_revenue_matches_finance
    query:
      metrics: [revenue]
      segments: [completed]
    value: 730
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

When you query from a pipeline directory directly, pass the connection explicitly, or set `source.connection` on the model and leave out `--connection`:

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
| `source.connection` | No | Name of the Bruin connection that holds the source table. `bruin semantic validate`, `bruin semantic check`, and `bruin query --pipeline` use it when you don't pass `--connection`. |
| `primary_key` | No | Primary key used as the default target key for joins into this model. |
| `joins` | No | Relationships from this model to other semantic models. |
| `dimensions` | No | Groupable fields. |
| `metrics` | No | Aggregations and derived metrics. |
| `segments` | No | Reusable filters. |
| `checks` | No | Model-level [quality checks](#quality-checks). Dimensions and metrics can also have their own `checks` list. |

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

## Quality Checks

Semantic models can define quality checks, much like [asset quality checks](/quality/overview). There are three kinds:

- **Dimension checks** are built-in checks on a dimension's values, such as `not_null` or `unique`.
- **Metric checks** are built-in checks on a metric's aggregated value, such as `positive` or `max`.
- **Model checks** are custom expectations written as semantic queries.

Bruin compiles every check into SQL, runs it on the model's connection, and reports whether it passed. Set `source.connection` on the model or pass `--connection` to tell Bruin which connection to use.

Use [`bruin semantic check`](/commands/semantic#semantic-check) to run the checks. [`bruin semantic validate`](/commands/semantic#semantic-validate) validates check definitions without reading data, and dry-runs the compiled SQL when a connection is available.

### Dimension checks

Dimension checks test every row of the model source, using the dimension's expression. They work like Bruin's [column checks](/quality/available_checks).

```yaml
dimensions:
  - name: order_id
    type: number
    checks:
      - name: not_null
      - name: unique
  - name: country
    type: string
    checks:
      - name: accepted_values
        value: [US, DE]
      - name: pattern
        value: "^[A-Z]{2}$"
  - name: amount
    type: number
    checks:
      - name: non_negative
      - name: max
        value: 10000
```

| Check | Value | Passes when |
|-------|-------|-------------|
| `not_null` | none | No value is null. |
| `unique` | none | No value appears more than once. |
| `positive` | none | Every value is greater than zero. |
| `non_negative` | none | Every value is zero or greater. |
| `negative` | none | Every value is less than zero. |
| `min` | number or string | Every value is greater than or equal to `value`. |
| `max` | number or string | Every value is less than or equal to `value`. |
| `accepted_values` | list | Every value is in the list. |
| `pattern` | string | Every value matches the regular expression. Bruin uses the regex operator of the connection's platform. MSSQL, Synapse, and Fabric have no regex operator, so Bruin uses `LIKE` with the value there. |

As with column checks, every dimension check except `not_null` ignores null values.

### Metric checks

A metric check computes the metric over the whole model, without grouping, and tests the resulting value. Derived metrics and metrics with a `filter` are computed the same way as in a query.

Window metrics cannot have checks, because they return one row per `order_by` group instead of a single value. Use a model check to test them.

```yaml
metrics:
  - name: revenue
    expression: sum(amount)
    checks:
      - name: positive
      - name: max
        value: 100000000
  - name: order_count
    expression: count(distinct order_id)
    checks:
      - name: not_null
  - name: avg_order_value
    expression: "{revenue} / {order_count}"
    checks:
      - name: min
        value: 1
```

| Check | Value | Passes when |
|-------|-------|-------------|
| `not_null` | none | The metric is not null. |
| `positive` | none | The metric is greater than zero. |
| `non_negative` | none | The metric is zero or greater. |
| `negative` | none | The metric is less than zero. |
| `min` | number or string | The metric is greater than or equal to `value`. |
| `max` | number or string | The metric is less than or equal to `value`. |
| `equals` | number, string, or boolean | The metric equals `value`. |

If the metric is null, every metric check fails.

### Model checks

Model checks go under the top-level `checks` key. Each one has a `name` and a `query`, and describes the expected result with either `value` or `count`.

The `query` is a semantic query, not SQL. It takes the same options as `bruin query`: `dimensions`, `metrics`, `filters`, `segments`, `sort`, and `limit`. Joined dimensions work too, as do the `name:granularity` and `name:direction` shorthands. Bruin validates the query against the model and compiles it with the same joins and metric expansion as any other semantic query.

```yaml
checks:
  # Completed revenue must match the total reported by finance.
  - name: completed_revenue_matches_finance
    query:
      metrics: [revenue]
      segments: [completed]
    value: 730

  # Orders come from exactly two customer countries.
  - name: two_customer_countries
    query:
      dimensions: [customers.country]
    count: 2

  # No month has zero or negative revenue.
  - name: monthly_revenue_is_positive
    query:
      dimensions: [order_date:month]
      metrics: [revenue]
      filters:
        - expression: "{revenue} <= 0"
    count: 0

  # No order has a negative amount.
  - name: no_negative_amounts
    query:
      dimensions: [order_id]
      filters:
        - dimension: amount
          operator: lt
          value: 0
    count: 0

  # Every order joins to a customer.
  - name: every_order_has_a_customer
    query:
      dimensions: [order_id]
      filters:
        - dimension: customers.country
          operator: is_null
    count: 0
```

A few patterns cover most checks:

- **Rows that should not exist.** Select a dimension, filter down to the bad rows, and set `count: 0`.
- **Referential integrity.** Bruin left-joins the target model, so an order without a matching customer has a null `customers.country`. Filter on that null with `count: 0`.
- **Conditions on metrics.** Structured filters only accept dimensions. To filter on a metric, use an `expression` filter such as `"{revenue} <= 0"`. Bruin compiles it into a `HAVING` clause.

#### Expected values

`value` can take one of these forms:

- **A scalar**, such as `730`: the query must return exactly one row with one column, and that cell must equal the value.
- **A mapping**, such as `{country: US, revenue: 600}`: the query must return exactly one row that matches it.
- **A list**: the query must return exactly one row per entry. Each entry is a mapping keyed by dimension or metric name. For single-column queries, an entry can also be a bare value.

A few more rules apply when Bruin compares values:

- Only the columns you list are compared.
- If the query has a `sort`, rows are compared in order. Otherwise they are compared regardless of order.
- `null` matches a NULL cell, so `value: null` expects a single NULL.
- A check with neither `value` nor `count` expects a single `0`, as with Bruin [custom checks](/quality/custom).

```yaml
checks:
  - name: revenue_by_country
    query:
      dimensions: [country]
      metrics: [revenue, order_count]
      sort: [revenue:desc]
    value:
      - country: US
        revenue: 600
        order_count: 5
      - country: DE
        revenue: 210
        order_count: 2
  - name: only_two_customer_countries
    query:
      dimensions: [customers.country]
    value: [US, DE]
```

#### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Check name. Must be unique within the model. |
| `description` | No | What the check verifies. |
| `query` | Yes | Semantic query to run. |
| `value` | No | Expected result: a scalar, a mapping, or a list of rows. See [expected values](#expected-values). Defaults to `0`. Numbers compare numerically, with a small tolerance for floating-point rounding. Dates compare as calendar dates, timestamps as instants, booleans by truthiness, and anything else as text. |
| `count` | No | Expected number of rows. Bruin wraps the query in `SELECT count(*)`. Cannot be combined with `value`. |

### Validation rules for checks

- Dimension and metric checks must use a supported check name. `unique`, `accepted_values`, and `pattern` are for dimensions only, and `equals` is for metrics only.
- `min`, `max`, and `equals` require a `value`. `accepted_values` requires a non-empty list, and `pattern` requires a string.
- Checks that take no value, such as `not_null`, reject one.
- A check name can appear only once per dimension or metric. Model check names must be unique within the model.
- A model check's `query` must compile, so unknown metrics, dimensions, or segments fail validation.
- A scalar `value` requires a query that returns a single column.

## CLI Reference

Semantic query flags are part of `bruin query`:

| Flag | Description |
|------|-------------|
| `--semantic-model` | Semantic model name to query. Required for semantic query mode. |
| `--pipeline` | Pipeline directory. Use when no anchor asset is provided. Bruin still loads semantic models from the repository root. |
| `--asset` | SQL asset path used to find the pipeline, connection, and dialect. |
| `--connection` | Connection name. Optional with `--asset`. With `--pipeline`, required unless the model sets `source.connection`. |
| `--metric` | Metric to select. Can be passed multiple times. |
| `--dimension` | Dimension to select. Use `name:granularity` for time dimensions. Can be passed multiple times. |
| `--filter` | Structured filter JSON. Can be passed multiple times. |
| `--segment` | Segment to apply. Can be passed multiple times. |
| `--sort` | Sort field. Use `name:asc` or `name:desc`. Can be passed multiple times. |

General `query` flags such as `--output`, `--limit`, `--timeout`, and `--export` also apply.

## Validation Rules

Bruin validates semantic models whenever it loads the repository's semantic catalog. To validate every model at once, run [`bruin semantic validate`](/commands/semantic#semantic-validate). It also dry-runs the models on the warehouse when a connection is available.

The validation rules are:

- `name` and `source.table` are required.
- Metric names, dimension names, and segment names must be unique within a model.
- Metrics require `expression`.
- Segments require `filter`.
- Derived metric references such as `{revenue}` must resolve to known metrics and cannot form cycles.
- Window metrics must reference exactly one metric, for example `expression: "{revenue}"`.
- Joined dimensions must resolve through a safe join path.
- Quality checks must follow the [check validation rules](#validation-rules-for-checks).

Invalid semantic models cause semantic query compilation to fail, so fix validation errors before querying the semantic layer.
