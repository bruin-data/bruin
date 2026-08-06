---
name: create-dashboard
description: Create DAC dashboards by writing YAML or TSX dashboard definition files. Use when the user wants to create, modify, review, or understand DAC dashboards, widgets, filters, SQL queries, semantic models, or CLI validation workflows.
argument-hint: "[dashboard request]"
version: 7
---

# Create Dashboard

Use this skill to create or modify DAC dashboard projects.

DAC projects define dashboards as code and run queries through Bruin connections. Dashboards can use direct SQL or the semantic layer. Semantic widgets reference models, dimensions, metrics, and segments; DAC compiles them to SQL in the backend.

## Project Layout

```text
my-dac-project/
  .bruin.yml
  dashboards/
    sales.yml
    sales.dashboard.tsx
    queries/
      revenue.sql
  semantic/
    sales.yml
  themes/
    brand.yml
```

Use `dashboards/` for dashboard files and `semantic/` for semantic model YAML files. Regular SQL dashboards do not need semantic models.

Dashboard files:

- `*.yml` and `*.yaml` are YAML dashboards.
- `*.dashboard.tsx` files are TSX dashboards.
- Other TSX files can be helpers, but are not auto-discovered as dashboards.

## Commands

```shell
dac init my-dashboards
dac validate --dir my-dashboards
dac check --dir my-dashboards
dac serve --dir my-dashboards --open
dac query --dir my-dashboards --dashboard "Sales" --widget "Revenue"
```

Use `dac validate` after editing structure and `dac check` when query execution should be verified.

## Connection Config

DAC reads Bruin connections from `.bruin.yml`.

```yaml
default_environment: default

environments:
  default:
    connections:
      duckdb:
        - name: local_duckdb
          path: data/analytics.duckdb
          read_only: true
```

Prefer `read_only: true` for DuckDB dashboards unless the project explicitly needs writes.

## YAML Dashboard

```yaml
name: Sales
description: Revenue and customer activity
connection: local_duckdb

filters:
  - name: region
    type: select
    default: All
    options:
      values: [All, North America, Europe, APAC]
  - name: date_range
    type: date-range
    default: last_30_days

rows:
  - widgets:
      - name: Revenue
        type: metric
        sql: |
          SELECT SUM(amount) AS value
          FROM sales
          WHERE created_at >= '{{ filters.date_range.start }}'
            AND created_at <= '{{ filters.date_range.end }}'
          {% if filters.region != 'All' %}
            AND region = '{{ filters.region }}'
          {% endif %}
        value:
          field: value
          type: number
          format: "$,.2f"
        col: 3
```

Widget types are `metric`, `chart`, `table`, `text`, `divider`, and `image`.

A `table` column takes `name`, `label`, `number` (value format: `number`, `currency`, or a d3-format string), `like`, `hidden`, and `format`. `format` is an **ordered list of layers**; for each cell the **first layer that matches wins**. A scalar `format` string (e.g. `format: currency`) is also accepted as a legacy alias for `number` — prefer `number` in new dashboards.

- With `if` (+ `value`), the layer styles only the cells that match. `value` is a scalar, `[low, high]` for `is_between`/`is_not_between`, `{ column: <name> }` to compare against another column in the same row, or omitted for empty checks. Operators: `is_empty`, `is_not_empty`, `text_contains`/`text_does_not_contain`/`text_starts_with`/`text_ends_with`/`text_is_exactly`, `date_is`/`date_before`/`date_after` (by day, or exact instant with a time), `greater_than`/`greater_than_or_equal`/`less_than`/`less_than_or_equal`, `is_equal_to`/`is_not_equal_to`, `is_between`/`is_not_between`.
- With no `if`, the layer styles every cell — a **gradient** (`backgroundColor` is a list of 2+ colors; optional `range` list + `unit` = `absolute`/`percent`/`percentile`, omit `range` for auto min/max) or a **flat fill** (`backgroundColor` is a string). Put it last as the fallback.
- Styles on any layer: `backgroundColor`, `textColor`, `bold`, `italic`, `underline`, `strikethrough`.
- `like`: mirror another column's coloring, driven by that column's per-row value, while keeping this column's own `number`.
- `hidden: true`: keep the column in the result but don't render it. Optional. Coloring reads a column whether or not it's shown, so hide only to drop it from the display, e.g. a `like` source you must declare but don't want visible.

Each layer is a YAML object, so `- { backgroundColor: [red, white, green], range: [-25, 0, 25], unit: absolute }` and the same keys written as an indented block are identical — use whichever reads better.

Colors are **named** (`red green blue indigo cyan purple pink amber`, plus `white`/`black`, aliases `positive`/`negative`/`warning`) or hex. Named colors adapt to light and dark.

Worked example:

```yaml
name: Regions

rows:
  - widgets:
      - name: Regions
        type: table
        col: 12
        sql: SELECT revenue, growth, score, status, actual, target, bonus, health FROM regions
        columns:
          - name: revenue
            number: currency
            format:
              - { backgroundColor: [red, white, green] }                # gradient, auto min→max
          - name: growth
            number: number
            format:
              - { backgroundColor: [blue, white, amber], range: [-25, 0, 25], unit: absolute }   # fixed anchors; unit also percent/percentile
          - name: score
            number: number
            format:                                                     # conditions, first match wins
              - { if: greater_than_or_equal, value: 80, backgroundColor: green }
              - { if: is_between, value: [50, 79], backgroundColor: amber }
              - { if: less_than, value: 50, textColor: red, strikethrough: true }
          - name: status
            format:
              - { if: text_contains, value: urgent, backgroundColor: amber, bold: true }
              - { if: is_empty, backgroundColor: "#F3F4F6", italic: true }   # flat fill (string)
          - name: actual
            number: number
            format:                                                     # cross-column, same row
              - { if: greater_than, value: { column: target }, backgroundColor: green }
          - name: target
            hidden: true                                                # in the result for the rule above, not rendered
          - name: bonus
            number: currency
            like: score                                                 # mirror score's colors, keep own number
          - name: health
            number: number
            format:                                                     # a condition wins over the gradient base below
              - { if: is_equal_to, value: 0, backgroundColor: red, bold: true }
              - { backgroundColor: [red, white, green] }                # base, last (always matches)
```

## Filters

Dashboard filters are UI controls. SQL dashboards use filter values through Jinja templates.

Supported filter types:

- `select`
- `date-range`
- `date`
- `number`
- `text`

Date range presets include `today`, `yesterday`, `last_7_days`, `last_30_days`, `last_90_days`, `this_month`, `last_month`, `this_quarter`, `this_year`, `year_to_date`, and `all_time`.

Both single and multiple `select` filters show a searchable dropdown, so you can type to find an option quickly when the list is long.

Select filters support `multiple: true` for multi-select. The value is a list — render with `join` in Jinja and guard the empty case:

```sql
{% if filters.status and filters.status | length > 0 %}
  AND status IN ('{{ filters.status | join("','") }}')
{% endif %}
```

Filter values are kept in the URL query string, so you can share a filtered dashboard as a link. Each filter becomes one query parameter named after it, for example `?region=Europe&date_range=last_30_days`. When a select has `multiple: true` the values are comma separated, and a `date-range` is either a preset key or `start..end`. Anything read from the URL is checked against the filter's type and options, and ignored if it doesn't match.

## Current Viewer (`bruin.user_email`)

`{{ bruin.user_email }}` is the email of the signed-in user viewing the dashboard — a Bruin Cloud runtime feature that resolves per viewer, so one dashboard can show each person only their own rows:

```sql
SELECT * FROM orders WHERE owner_email = '{{ bruin.user_email }}'
```

Locally there is no signed-in user, so the value comes from the `BRUIN_USER_EMAIL` environment variable (empty if unset). To preview a user-scoped dashboard as a specific person, pass it inline: `BRUIN_USER_EMAIL=someone@example.com dac dev`. In Bruin Cloud this becomes dynamic per signed-in viewer.

## Named Queries

Use named queries when multiple widgets share the same SQL or semantic query.

```yaml
queries:
  revenue_by_region:
    sql: |
      SELECT region, SUM(amount) AS revenue
      FROM sales
      GROUP BY 1

rows:
  - widgets:
      - name: Revenue by Region
        type: chart
        chart: bar
        query: revenue_by_region
        x: { field: region }
        y: { field: [revenue] }
        col: 6
```

A chart's `x` and `y` are axis encoding objects with a required `field` (bare column names like `x: region` are invalid). `field` may be a single column or a list.

The `funnel` chart shows conversion through ordered stages: one bar per stage with its share of the top of the funnel and the step-to-step conversion. Use `label` (stage) and `value` (count), and order rows top-of-funnel first in SQL. `horizontal: true` lays the stages left-to-right, and bar labels honor `value.format` (e.g. `"$,.0f"` for a revenue funnel).

Every query is an inline `sql:` block or a named `query:` reference — YAML widgets do not take file paths. In TSX, `include("queries/revenue.sql")` reads a `.sql` file into an inline query at load time.

## Inline (Static) Data

A `metric`, `chart`, or `table` widget can carry its values inline with `data` instead of a query. A widget with `data` renders **without a connection or SQL** — `columns` are the column names and `rows` is one positional list per row. The encoding fields (`x`, `y`, `value`, `label`, `columns`) reference the column names.

```yaml
rows:
  - widgets:
      - name: Revenue by Quarter
        type: chart
        chart: bar
        col: 6
        data:
          columns: [quarter, revenue]
          rows:
            - [Q1, 12000]
            - [Q2, 15500]
            - [Q3, 14200]
            - [Q4, 18900]
        x: { field: quarter, type: category }
        y: { field: [revenue], type: number, format: "$,.0f" }
```

**Use this only when there is genuinely no data connection** — e.g. a brand-new project where `.bruin.yml` has no connections, a hardcoded illustrative example, or a layout mockup. **When a connection exists, always use `sql:`, `query:`, or a semantic widget instead.** Inline data is frozen: it never refreshes, ignores filters, and goes stale. Do not paste real query results into `data` to "cache" them, and do not present made-up numbers as real — tell the user inline values are illustrative until a warehouse is connected.

Rules:

- `data` is mutually exclusive with `sql`, `query`, and semantic fields (`model`, `dimension`, `metrics`, …). Setting both fails validation.
- Every row must have exactly one value per column.
- Not valid on `text`, `image`, or `divider` widgets.
- A dashboard built entirely from `data` widgets needs no top-level `connection`.

## Semantic Models

Semantic models live in `semantic/*.yml`.

```yaml
name: sales
label: Sales
source:
  table: marts.sales

dimensions:
  - name: created_at
    type: time
    granularities:
      month: date_trunc('month', created_at)
  - name: region
    type: string
  - name: channel
    type: string

metrics:
  - name: revenue
    expression: sum(amount)
    format:
      type: currency
      currency: USD
      decimals: 0
  - name: orders
    expression: count(*)
  - name: average_order_value
    expression: "{revenue} / nullif({orders}, 0)"

segments:
  - name: online
    filter: "channel = 'online'"
```

Metrics are aggregate SQL expressions or expressions over other metrics using `{metric_name}` references. Dimensions are the only fields valid for semantic filters.

### Joins

A model can join to other models so a query can group, filter, or sort by dimensions on a related model. Declare a `joins` block on the model and a `primary_key` on the join target, then reference joined dimensions as `relation.dimension`.

```yaml
# semantic/orders.yml
name: orders
source:
  table: marts.orders
primary_key: order_id
joins:
  - name: customers          # relation name; also the target model name unless `model:` is set
    relationship: many_to_one
    foreign_key: customer_id # column on this model pointing at customers.primary_key
dimensions:
  - name: category
    type: string
metrics:
  - name: revenue
    expression: sum(amount)
```

```yaml
# semantic/customers.yml
name: customers
source:
  table: marts.customers
primary_key: customer_id
dimensions:
  - name: country
    type: string
```

A widget or named query on `orders` then references the joined dimension by `relation.dimension`:

```yaml
- name: Revenue by Country
  type: chart
  chart: bar
  dimension: customers.country   # dimension from the joined customers model
  metrics: [revenue]
```

Relationships: `one_to_one`, `many_to_one`, `one_to_many`, `many_to_many`. Use `target_key` to override the joined column, or `sql` for a custom join condition.

## Semantic Dashboard

```yaml
name: Semantic Sales
connection: local_duckdb
model: sales

filters:
  - name: region
    type: select
    default: North America
    options:
      values: [North America, Europe, APAC]

rows:
  - widgets:
      - name: Revenue
        type: metric
        metric: revenue
        filters:
          - dimension: region
            operator: equals
            value: "{{ filters.region }}"
        value:
          field: revenue
          type: number
          format: "$,.0f"
        col: 3

      - name: Revenue by Month
        type: chart
        chart: area
        dimension: created_at
        granularity: month
        metrics: [revenue]
        sort:
          - name: created_at
            direction: asc
        col: 9
```

A widget can set `model` directly, or inherit the dashboard-level `model`. For multiple models, use a dashboard-level `models` map and reference the model alias on widgets or named queries.

Semantic filter operators include `equals`, `not_equals`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `between`, `is_null`, and `is_not_null`.

## TSX Dashboard

Use TSX when the dashboard needs variables, loops, reusable components, conditionals, or generated layouts.

```tsx
export default (
  <Dashboard name="Semantic Sales" connection="local_duckdb" model="sales">
    <Filter
      name="region"
      type="select"
      default="North America"
      options={{ values: ["North America", "Europe", "APAC"] }}
    />

    <Row>
      <Metric
        name="Revenue"
        metric="revenue"
        filters={[
          { dimension: "region", operator: "equals", value: "{{ filters.region }}" },
        ]}
        value={{ field: "revenue", type: "number", format: "$,.0f" }}
        col={3}
      />
      <Chart
        name="Revenue by Month"
        chart="area"
        dimension="created_at"
        granularity="month"
        metrics={["revenue"]}
        sort={[{ name: "created_at", direction: "asc" }]}
        col={9}
      />
    </Row>
  </Dashboard>
)
```

TSX supports the same dashboard model as YAML. Keep semantic logic declarative; do not manually compile semantic metrics to SQL in TSX.

## Deprecated Fields

These fields were removed from the DAC schema. Never emit them in new dashboards. **If you encounter any of them while reading or editing an existing dashboard, refactor them to the current form** — preserving the original column, formatting, and labels — and re-run `dac validate` to confirm the dashboard still loads.

| Deprecated | Replacement |
|---|---|
| Chart `x: col` / `y: [col]` (bare column names) | `x: { field: col }` / `y: { field: [col] }` — axis encoding objects with a required `field` |
| Widget or named-query `file: path.sql` | Inline `sql:` or a named `query:` reference. In TSX, `include("path.sql")` reads a `.sql` file into inline SQL at load time |
| Metric widget `column`, `prefix`, `suffix`, `format` (flat fields) | `value: { field: <column>, type: number, format: "<d3-format>" }` |
| Dashboard inline `semantic:` block (`source` / `metrics` / `dimensions`) | Define the model in `semantic/*.yml` and reference it with `model:` |

## Authoring Rules

- Keep dashboard files focused on presentation and query intent.
- Prefer semantic widgets when metrics or dimensions are reused.
- Use direct SQL for one-off custom queries or non-semantic dashboards.
- Use inline `data` only when there is no connection; prefer `sql`/`query`/semantic whenever one exists, since inline data never refreshes.
- Validate both YAML and TSX dashboards after changes.
- Do not require semantic models for regular SQL dashboards.
- Do not put secrets in dashboard files; use Bruin connection config.
