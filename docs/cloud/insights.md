# Insights

The **Insights** dropdown in the top nav bundles the analytics and observability views: **Cost Explorer**, **Pipeline Health**, **Risk Report**, and **Usage**. Each focuses on a different question:

| View | Answers |
|---|---|
| **Cost Explorer** | What's my warehouse spend by project, pipeline, user, or asset? |
| **Pipeline Health** | Are my pipelines stable? Where are the regressions? |
| **Risk Report** | Which assets lack owners, descriptions, or quality checks? |
| **Usage** | How much compute did Bruin Cloud itself use, and where am I against my limits? |

What you see depends on your permissions and plan. Cost Explorer and Pipeline Health are gated to enterprise; Risk Report and Usage are available to everyone.

## Cost Explorer

Track warehouse spend on **BigQuery** and **Snowflake**. Cost Explorer pulls billing data from those platforms and breaks it down so you can see what's driving spend.

> [!INFO]
> Cost Explorer requires the `pipeline:cost:show` permission and the **enterprise** tier. Non-enterprise teams see an upgrade prompt.

### Two views

- **Cost Explorer** (`/cost-explorer/bigquery`) — Metabase-powered dashboards for BigQuery (dashboard 74) and Snowflake (dashboard 84). Tabs switch between them.
- **New Cost Explorer** (`/new-cost-explorer`) — interactive tables with sortable columns and drill-down. Use this for ad-hoc analysis.

Both are still active. Use the new explorer for slicing data; use the Metabase view for canned visualisations.

### Breakdowns

The new explorer pivots cost across:

- **Project ID**
- **Pipeline ID**
- **User Email** (who ran the query)
- **Asset Name**
- **Query** (individual SQL statements)
- **Dashboard** (Metabase dashboards)
- **Step Type** (main task vs. column check vs. custom check)

Every breakdown table shows three metrics: **Query Count**, **Total TB Billed**, **Total Cost (USD)**.

### Time controls

- **Date range** picker, default last 30 days. Custom ranges supported.
- **Granularity** auto-adjusts from the range: ≤2 days = hour, ≤30 = day, ≤90 = week, more = month. You can override.
- **Quick presets**: Today, Yesterday, Last 2 / 7 / 30 days, Last month, Last 3 / 6 months, Last year.

## Pipeline Health

A Metabase-powered health dashboard (dashboard 83) showing pipeline stability, failure rates, and trend lines.

> [!INFO]
> Pipeline Health is **enterprise-only** and requires the `pipeline:health:show` permission. Non-enterprise teams see the upgrade prompt.

The view is a Metabase iframe — drill-downs, filters, and metric definitions are all configured inside Metabase. If you have admin access to the embedded dashboard, you can tune it there.

## Risk Report

Risk Report scores every asset against Bruin's built-in [governance rules](/cloud/governance) and highlights the assets most likely to cause trouble.

Open it from **Insights → Risk Report**. No special permission required.

### What it shows

- **KPI cards** — total assets, orphan assets (no owner), no-documentation assets, and a **Risk Score** donut.
- **Quality score buckets** — every asset is bucketed:
  - **Poor** (red) — < 45%
  - **Average** (yellow) — 45–65%
  - **Good** (blue) — 65–85%
  - **Excellent** (green) — ≥ 85%
- **Asset table** — sorted by downstream impact. Columns: name, type, level (depth in the lineage), downstream count, score with colour.

### Acting on a risk

- **Click an asset** to jump to its detail page and add the missing description, owner, or check.
- **Export to CSV** — the kebab menu has shortcuts to export **Orphan Assets** or **No Documentation Assets** as CSV (columns: name, type, level, downstream count, score).

For the rules that drive the score, see [Governance](/cloud/governance).

## Usage

The Usage page tracks **Bruin Cloud's own** compute consumption — task count, memory hours, CPU seconds. This is what you pay for on the pay-as-you-go plan and what your free-tier credit measures against.

Open it from **Insights → Usage**.

### What it tracks

- **Task count** — number of asset instances executed.
- **Memory** in GB-seconds, converted to GB-hours and (for PAYG teams) **cost** at $10/GB-hour.

### Headline card (PAYG only)

For pay-as-you-go teams the page leads with a **Memory Hours Used** card: a progress bar against the free-tier limit (default 10 hours), colour-coded (green < 80%, amber 80–99%, red ≥ 100%). The subtitle shows your free credit cap.

### Time controls

Same model as Cost Explorer — date range with quick presets, granularity (Hour / Day / Week / Month) auto-inferred from the range.

### Breakdowns

The breakdown dropdown lets you split usage by:

- **None** (totals only)
- **Asset**
- **Pipeline**
- **Project**
- **State** (success, failed, etc.)
- **Asset Type**

A stacked column chart plots memory hours over time. Below it, a table shows GB-seconds, GB-hours, and (PAYG) cost per row, with a total at the bottom.

### Free tier vs PAYG

| | Free tier | Pay-as-you-go |
|---|---|---|
| **Cost column** | Hidden | Shown ($10/GB-hour) |
| **Free credit** | 100 USD, 10 memory hours | Same defaults; usage above billed |
| **Headline card** | Hidden | Memory Hours Used |

Free-tier limits are the defaults set in `PaygoUsageLimitService`. Your team's actual cap may differ — check [Team Settings → Usage & Billing](/cloud/team-settings#usage--billing).

## Related

- [Governance](/cloud/governance) — rules that drive the Risk Report.
- [Team Settings → Usage & Billing](/cloud/team-settings#usage--billing) — change plans, set thresholds, manage Stripe.
- [Pipelines](/cloud/pipelines) — operate the pipelines whose runs feed these views.
