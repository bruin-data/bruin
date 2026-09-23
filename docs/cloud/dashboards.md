# Dashboards

A **dashboard** in Bruin Cloud is an interactive canvas of charts, tables, metrics, and text that an [AI agent](/cloud/ai-agents/overview) builds for you from a conversation. You describe what you want, the agent runs the SQL, lays out widgets on a grid, and saves the result. Your team can open the dashboard later without re-asking.

Dashboards live under **AI → Dashboards** in the top nav. You need an agent with a [connection set](/cloud/connections#connection-sets-for-ai-agents) before you can build one.

## Create a dashboard

### 1. Open Dashboards

From the **AI** menu, choose **Dashboards**.

### 2. New Dashboard

Click **New Dashboard**, pick the agent that should build it, and give it an optional name. The agent inherits its project, connection set, and CLI access from your existing configuration.

### 3. Describe what you want

A dashboard opens in **edit mode** with a chat composer at the bottom. Send the agent a prompt like:

> Build a dashboard showing daily revenue, top 10 products by revenue, and a 7-day moving average of order volume.

The agent runs queries, picks widget types, and lays them out on a row/column grid. You can iterate — ask for changes, swap chart types, reorder rows. Each message the agent sends updates the canvas in real time.

### 4. Publish

When the layout looks right, click **Publish**. The current draft becomes the published version that everyone with access sees. The dashboard exits edit mode.

You can re-enter **Edit** at any time to keep iterating. While unpublished changes exist, the header shows **Unpublished changes**; click **Discard** to roll back to the last published version.

## Widgets

The agent can place seven widget types on the canvas:

| Type | What it shows |
|---|---|
| **Chart** | A data visualisation such as a line, bar, pie, funnel, or Sankey chart |
| **Metric** | A single KPI value with optional number formatting |
| **Table** | Query results with sorting, formatting, frozen columns, and image cells |
| **Pivot table** | Query results grouped into rows, columns, subtotals, and aggregated values |
| **Text** | Markdown blocks for section headers, narrative, and links |
| **Image** | One image for each query result row, with optional titles and captions |
| **Divider** | A horizontal separator between dashboard sections |

### Charts

Set `type: chart` and choose one of the chart types supported by Cloud:

- Common charts: `line`, `bar`, `area`, `pie`, `donut`, `scatter`, `bubble`, `combo`, `histogram`, and `heatmap`.
- Specialized charts: `sparkline`, `calendar`, `boxplot`, `funnel`, `sankey`, `treemap`, `waterfall`, `xmr`, `dumbbell`, `gauge`, `radar`, `candlestick`, and `forest`.
- Custom charts: `vega-lite`, with a Vega-Lite `spec` that reads the query result from the `dac` dataset.

Most charts map result columns with `x` and `y`. Depending on the chart, you can also use fields such as `color`, `y2`, `stacked`, `normalized`, `horizontal`, `size`, `lines`, `refLines`, and `refBands`.

```yaml
- id: revenue_trend
  name: Revenue trend
  type: chart
  chart: line
  query: monthly_revenue_by_channel
  x: { field: month, type: date, format: "%b %Y" }
  y: { field: [revenue], type: number, format: "$,.0f" }
  color: { field: channel }
```

### Tables and pivot tables

A table infers its columns from the query result. Use `columns` when you need to rename, format, align, hide, freeze, or conditionally style a column. Set a column's `type` to `image` to render its URL values as thumbnails.

A `pivot_table` reshapes flat query results in the browser. Its `pivot.values` list is required; `rows` and `columns` are optional.

```yaml
- id: revenue_pivot
  name: Revenue by region and month
  type: pivot_table
  query: regional_revenue
  pivot:
    rows:
      - { field: region, showTotals: true }
    columns:
      - { field: month }
    values:
      - field: revenue
        summarize: sum
        label: Total revenue
        format:
          - { backgroundColor: ["#DBEAFE", "#2563EB"] }
```

### Images

An image widget is data-backed. `src`, `title`, `caption`, and `alt` name columns in the query result rather than containing literal display text. Only `src` is required. Each result row becomes an image; multiple rows form a horizontal gallery. `fit` can be `contain` (the default) or `cover`, and captions support Markdown.

```yaml
queries:
  property_gallery:
    sql: |
      SELECT photo_url, address, price_label, alt_text
      FROM analytics.listings
      ORDER BY featured_at DESC
      LIMIT 8

rows:
  - height: 360px
    widgets:
      - id: property_gallery
        name: Featured properties
        type: image
        col: 12
        query: property_gallery
        src: photo_url
        title: address
        caption: price_label
        alt: alt_text
        fit: cover
```

### Text and dividers

Text widgets render sanitized Markdown from `content`. A divider is a thin horizontal line used to separate dashboard sections. It does not need a query or title and is usually placed in its own full-width row between two groups of widgets.

```yaml
rows:
  - widgets:
      - id: revenue_heading
        type: text
        col: 12
        content: |
          ## Revenue performance
          Results for the selected reporting period.
  - widgets:
      - id: revenue_divider
        type: divider
        col: 12
```

For the complete field reference for each widget and chart type, see the [DAC widget documentation](https://getbruin.com/docs/dac/dashboards/widgets.html).

## Layout

Widgets are arranged in rows on a 12-column grid. Set `col` from 1 to 12; when it is omitted, a widget spans the full row. A row can set `height` with a pixel number or CSS value. Add the same `tab` name to multiple rows to group them into a tab; rows without `tab` stay visible above the tab bar.

```yaml
rows:
  - widgets:
      - { id: revenue, name: Revenue, type: metric, col: 4, query: totals, value: { field: revenue } }
      - { id: orders, name: Orders, type: metric, col: 4, query: totals, value: { field: orders } }
      - { id: customers, name: Customers, type: metric, col: 4, query: totals, value: { field: customers } }
  - tab: Trends
    height: 420px
    widgets:
      - { id: revenue_trend, name: Revenue trend, type: chart, chart: line, col: 12, query: monthly_revenue, x: { field: month }, y: { field: [revenue] } }
```

## Data sources

A data-backed widget can use inline `sql`, reference a top-level named query with `query`, run a semantic query with fields such as `model`, `dimensions`, and `metrics`, or use inline `data`. Named queries are useful when several widgets share one result. Inline data is intended for values supplied directly by the user and cannot react to dashboard filters.

The dashboard-level `connection` is used by default. A named query or widget can override it with its own `connection`.

SQL and semantic filter values support Jinja templating. Use the `bruin.user_email` template variable when a query should be scoped to the signed-in viewer.

```yaml
name: Revenue overview
connection: warehouse

queries:
  monthly_revenue:
    sql: |
      SELECT month, SUM(revenue) AS revenue
      FROM analytics.sales
      GROUP BY month

rows:
  - widgets:
      - id: revenue_table
        name: Monthly revenue
        type: table
        query: monthly_revenue
```

## Filters

Cloud supports `select`, `date-range`, `date`, `number`, and `text` filters. Select filters can use fixed values or a query, and support `multiple: true`. Reference current values in SQL with Jinja through `filters.<name>`; date ranges provide `.start` and `.end`.

```yaml
filters:
  - name: period
    type: date-range
    default: last_30_days
    options:
      presets: [last_7_days, last_30_days, last_90_days, this_year]
  - name: regions
    type: select
    multiple: true
    default: []
    options:
      query: SELECT DISTINCT region FROM analytics.sales ORDER BY region

queries:
  filtered_revenue:
    sql: |
      SELECT order_date, SUM(revenue) AS revenue
      FROM analytics.sales
      WHERE order_date BETWEEN '{{ filters.period.start }}' AND '{{ filters.period.end }}'
      {% if filters.regions %}
      AND region IN (
        {% for region in filters.regions %}
        '{{ region }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
      {% endif %}
      GROUP BY order_date
```

Applied filter values are stored in the dashboard URL, so a shared link opens with the same filter state. For all filter fields and date presets, see the [DAC filter documentation](https://getbruin.com/docs/dac/dashboards/filters.html).

## Notes

Notes add human context to dashboard data. A note can apply to the whole dashboard, a widget, a table row, or a chart point, depending on the dimensions selected when it is created. Use the **Notes** button in the dashboard toolbar to browse and filter all notes, or use a note icon on a widget to work with notes in that widget's context.

Before users can write a note, its reusable definition must exist in the dashboard YAML. Define notes at the top level and reference their IDs from the widgets where they should be available:

```yaml
name: Revenue overview
connection: warehouse

notes:
  - id: revenue_context
    dimensions:
      - name: region
        required: true
      - name: channel
        multiselect: true
  - id: channel_context
    dimensions:
      - name: channel

queries:
  revenue_by_region:
    sql: |
      SELECT region, channel, SUM(revenue) AS revenue
      FROM analytics.sales
      GROUP BY region, channel

rows:
  - widgets:
      - id: revenue_by_region
        name: Revenue by region
        type: table
        query: revenue_by_region
        notes:
          - revenue_context
          - channel_context
```

Each definition has a unique `id` and a `dimensions` list. Dimension values are optional and single-select by default. Set `required: true` when a value must be selected, and `multiselect: true` when the note may target multiple values. Note content is written in the dashboard UI.

Widgets can also reference note definitions from their resolved [semantic model](/core-concepts/semantic-layer). Define the reusable note alongside the model's dimensions and metrics:

```yaml
# semantic/sales.yml
schema: v1
name: sales

source:
  table: analytics.sales

dimensions:
  - name: region
    type: string

metrics:
  - name: revenue
    expression: sum(revenue)

notes:
  - id: region_note
    dimensions:
      - name: region
        required: true
```

Then reference its ID from a widget that resolves to that model:

```yaml
name: Sales overview
model: sales

rows:
  - widgets:
      - id: revenue_by_region
        name: Revenue by region
        type: table
        model: sales
        dimensions: [{ name: region }]
        metrics: [revenue]
        notes: [region_note]
```

The model is resolved from the named query, widget, or dashboard-level `model`. Without a resolved model, only dashboard-level note definitions are available.

Dashboard changes are saved as a draft. After adding or changing a note definition, publish the dashboard before trying to create notes with it.

## Threads and chat history

Each dashboard has a chat panel and **per-user threads**. Your conversations with the agent are private — a teammate viewing the same dashboard sees its canvas but not your chats. They have their own threads to ask follow-up questions.

Click the thread tabs above the composer to switch between past conversations. From the menu on a tab you can **Rename** or **Delete** a thread.

## Sharing and access

By default a dashboard is **private** to its creator. Open the **Share** dialog to change that.

**Visibility:**

- **Private** — only people you explicitly add can see it.
- **Team** — everyone on your team can view; only editors can publish changes.

**Per-user roles** (admins and the creator can grant):

- **Viewer** — can see the published version, ask the agent questions in their own thread.
- **Editor** — can also edit the canvas and publish.

The creator and team admins are always editors. Removing the agent's access (or losing it on your account) hides the chat sidebar but keeps the rendered widgets visible.

## Edit mode and concurrency

Only one editor can write to the draft at a time. If a teammate's agent is mid-update, sending a message returns *"{name}'s agent is currently working on this dashboard. Please wait."* The lock clears when their agent finishes.

Agents in **view mode** can still answer questions about the data — they just can't modify the canvas. Writes from a view-mode session are silently dropped.

## Limits

- **Free tier:** dashboard and thread creation count against the AI usage limits described in [Pay-as-you-go](/cloud/insights#usage). Creating a new thread when the cap is reached returns an error.
- **Attachments:** up to **5 files per message, 100 MB each**. Files upload to S3 and are referenced as message attachments.
- **One active editor per dashboard** at a time, enforced server-side.

## Delete a dashboard

Open the dashboard menu and pick **Delete**. Only the creator or a team admin can delete. Deleting removes the dashboard, all threads, and any uploaded attachments (subject to S3 lifecycle rules).

## Related

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent that builds the dashboard.
- [Chat with Agents](/cloud/ai-agents/chat) — iterate on questions outside a dashboard before committing to a layout.
- [Connection Sets](/cloud/connections#connection-sets-for-ai-agents) — control which data the agent can read while building.
