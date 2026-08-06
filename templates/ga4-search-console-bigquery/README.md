# Google Analytics and Search Console Reporting on BigQuery

`ga4-search-console-bigquery` builds an organic search reporting layer on top of
GA4 and Search Console data you already have in BigQuery. It does not ingest
anything: both products export to BigQuery natively, so this template starts where
those exports land, conforms them in `web_stage`, and publishes seven reports in
`web_reports`.

Every report answers a question GA4 and Search Console cannot answer themselves —
not because the data is missing, but because it is split across two products that
never join, capped at a thousand exported rows, or aggregated behind an "(other)"
bucket. The template contains 13 assets and is meant to be edited.

## Requirements

You need both exports already running and landing in BigQuery:

- The [GA4 BigQuery export](https://support.google.com/analytics/answer/9823238),
  which creates a dataset named `analytics_<property_id>` containing
  `events_YYYYMMDD` tables.
- The [Search Console bulk data export](https://support.google.com/webmasters/answer/12918484),
  which creates a dataset containing `searchdata_site_impression`,
  `searchdata_url_impression`, and `ExportLog`.

Neither export backfills. Both begin collecting from the day you enable them, so
set `start_date` in `pipeline.yml` to the earliest date your exports actually
cover.

The service account behind the `gcp-default` connection needs read access to both
export datasets and write access to the datasets this pipeline creates.

## Project structure

```text
ga4-search-console-bigquery/
├── pipeline.yml
├── README.md
├── macros/
│   ├── search.sql
│   └── url.sql
└── assets/
    ├── web_stage/
    │   ├── gsc_site_query_daily.sql
    │   ├── gsc_url_query_daily.sql
    │   ├── gsc_position_click_curve.sql
    │   ├── gsc_export_log.sql
    │   ├── ga4_sessions.sql
    │   └── ga4_page_daily.sql
    └── web_reports/
        ├── search_brand_split_weekly.sql
        ├── search_query_opportunities.sql
        ├── search_query_cannibalization.sql
        ├── search_page_trend.sql
        ├── search_new_and_lost_queries.sql
        ├── organic_landing_page_performance.sql
        └── organic_query_value.sql
```

## Configure it

All configuration lives in the `variables` block of `pipeline.yml`. At minimum,
set the two dataset names and your brand pattern:

```yaml
variables:
  ga4_dataset:
    type: string
    default: analytics_123456789      # your GA4 export dataset
  search_console_dataset:
    type: string
    default: searchconsole            # your Search Console export dataset
  brand_query_pattern:
    type: string
    default: "(acme|acmecorp|acme co)"  # your brand and its misspellings
```

| Variable | Default | What it controls |
| --- | --- | --- |
| `ga4_dataset` | `analytics_000000000` | GA4 export dataset. Qualify it with a project when the export lives outside the connection's project. |
| `search_console_dataset` | `searchconsole` | Search Console export dataset. |
| `brand_query_pattern` | `(example\|exampl\|examp1e)` | Case-insensitive RE2 pattern marking a query as branded. |
| `key_event_names` | `["purchase", "generate_lead", "sign_up"]` | GA4 events counted as conversions. Match these to the key events configured on your property. |
| `source_lookback_days` | `4` | Extra days re-read before the run window, so late and revised data heals. |
| `reporting_window_days` | `180` | Trailing days of history the reports aggregate. Bounds the bytes each report scans. |
| `trend_window_days` | `28` | Length of the current and prior comparison windows. |
| `min_query_impressions` | `100` | Minimum window impressions for a query to appear in a query-grain report. |
| `min_page_impressions` | `100` | Minimum window impressions for a page to appear in a page-grain report. |

Override any of them per run without editing the file:

```bash
bruin run --var reporting_window_days=365 ga4-search-console-bigquery
```

The brand pattern is the one setting worth getting right before you read anything,
because it splits the traffic that measures SEO work from the traffic that measures
brand awareness.

## What it creates

### `web_stage` — conformed models

| Asset | Grain | Notes |
| --- | --- | --- |
| `gsc_site_query_daily` | day × query × country × device × search type | Property-level impressions, counted once per property per query. Use this for totals that reconcile with the Search Console interface. |
| `gsc_url_query_daily` | day × URL × query × country × device × search type | The only grain carrying both the page and the query. Summing its impressions across pages overstates property totals by design. |
| `gsc_position_click_curve` | property × search type × position | The CTR your property actually earns at each position, measured from its own history. |
| `gsc_export_log` | table × reporting date | When each date was published and whether Google later restated it. |
| `ga4_sessions` | session | One row per session, rebuilt from the event export. No sampling, no "(other)" bucket. |
| `ga4_page_daily` | day × host × page × channel × source × device | Page views from any session, not only sessions that landed on the page. |

Both Search Console models keep the rows where Google withheld the query: the
impressions and clicks are real, so dropping them would quietly understate daily
totals. They carry a NULL query and a `query_brand_type` of `anonymized`, and the
query-grain reports exclude them explicitly.

`page_path` is the join key across both systems, produced by the shared
`page_path` macro in `macros/url.sql`. Search Console reports a canonical absolute
URL and GA4 reports whatever the tag collected, so both sides are lowercased and
stripped of scheme, host, query string, fragment, and trailing slashes. If your
site needs different rules — locale prefixes, pagination parameters that matter —
change the macro once and every model follows.

### `web_reports` — the seven reports

| Report | The question | Why neither product answers it |
| --- | --- | --- |
| `search_brand_split_weekly` | How much demand is branded versus non-branded? | Search Console has no concept of a brand, so its headline click trend mixes discovery with people who already knew you. |
| `search_query_opportunities` | Which queries are leaving clicks on the table, and how many? | Search Console shows CTR and position side by side but never says whether a CTR is good *for that position*. |
| `search_query_cannibalization` | Which queries do several of my own pages compete for? | The interface can filter by query or group by page, never pivot one against the other. |
| `search_page_trend` | Which pages are quietly decaying? | Period comparison works one view at a time and within a thousand exported rows, so the long tail never surfaces. |
| `search_new_and_lost_queries` | What did I start and stop ranking for? | Comparing periods shows how shared queries moved, not which ones appeared or vanished. |
| `organic_landing_page_performance` | What happened after the click? | Search Console stops at the click; GA4 never receives the query or the impression. |
| `organic_query_value` | Which queries make money? | Google deliberately never passes the query to analytics. |

## Reading the reports

### Start with `organic_landing_page_performance`

This is the join the other reports lean on. Each page carries its Search Console
visibility next to what GA4 recorded afterwards, so `revenue_per_search_click_usd`
and `revenue_per_thousand_impressions_usd` rank pages by business value rather
than by traffic.

The join is a full outer join on purpose, and `coverage_status` explains every
mismatch:

| `coverage_status` | Meaning | Usual cause |
| --- | --- | --- |
| `matched` | Both systems agree the page gets Google search traffic. | |
| `search_only` | Search Console reports clicks GA4 never recorded. | Missing or blocked tracking, a redirect chain, or bot filtering. |
| `ga4_only` | GA4 recorded organic sessions Search Console has no data for. | Traffic from a non-Google engine, or a page excluded from the property. |
| `session_shortfall` | GA4 saw fewer than half the reported clicks. | Consent-mode loss, slow tag load, or a redirect on entry. |

Session counts and click counts come from different measurement systems and will
never match exactly. `session_per_click_ratio` sits near one on healthy pages;
treat a low value as a tracking lead, not as a bug in the report.

The GA4 side is restricted to Google organic sessions, because Search Console has
no visibility into Bing or any other engine. `web_stage.ga4_sessions` exposes both
`is_organic_search_session` and the narrower `is_google_organic_session` so you can
choose deliberately.

### `organic_query_value` is modelled, not measured

Each page's GA4 outcomes are split across the queries that sent clicks to it, in
proportion to those clicks. Within a page the weights sum to one, so page totals
are preserved exactly and only the split between queries is estimated.

The assumption is that every query sending traffic to a page converts at that
page's average rate. That is wrong in the interesting direction: a high-intent
query really does convert better than the page average, so its value is understated
and a broad informational query's is overstated. Rank queries against each other
with it; do not report the revenue figure as measured.

Clicks Google withheld are left out of the denominator rather than held back, so
their share of a page's outcome is spread across that page's disclosed queries. A
page whose clicks were all withheld contributes nothing, which is why site totals
here fall short of those in `organic_landing_page_performance`.

### Opportunity sizing

`search_query_opportunities` gives two numbers, and they answer different
questions:

- `clicks_at_expected_ctr` — what fixing the title and description alone would
  return, holding position constant.
- `clicks_at_top_three_ctr` — what ranking in the top three would return, holding
  impressions constant.

Both are estimates from observed behaviour rather than forecasts; impressions move
when position moves. `is_expected_ctr_reliable` tells you whether the click-curve
bucket behind the comparison has enough impressions to trust.

## Timing, freshness, and incremental behaviour

Search Console publishes with a two-to-three day delay and revises history in
place. GA4 finalizes a daily table only after dropping its intraday table. So the
staging models widen their read window by `source_lookback_days` and replace whole
days with `delete+insert`, which makes late and restated data self-healing on the
next run. `web_stage.gsc_export_log` is where you confirm what Google actually
published: a bumped `epoch_version` means Google restated a date rather than the
site changing.

The GA4 models read the `events_*` wildcard but exclude the `events_intraday_`
tables, so a day is only ever loaded once it is final.

Two known edges, both documented on the assets:

- Sessions that cross midnight span two daily export tables. A session split by
  the window edge is completed by the following run, because the lookback re-reads
  the day and replaces it.
- Search Console dates are Pacific Time and GA4 `event_date` is stamped in the
  property's reporting timezone. Where those differ, a day's numbers are offset by
  a few hours' worth of activity between the two systems.

The reports are rebuilt in full over a trailing `reporting_window_days` window,
which bounds what each one scans. Raise it to report over more history and expect
the bytes scanned to rise with it. The trend reports need at least
`2 × trend_window_days` of staged history before they say anything useful.

## Run it

```bash
bruin init ga4-search-console-bigquery my-search-pipeline
```

Set the dataset names, brand pattern, and `start_date`, then validate:

```bash
bruin validate --fast my-search-pipeline
```

On a first load, build every table before running query validation:

```bash
bruin run --full-refresh --no-validation my-search-pipeline
```

Scope that first run with `--start-date` to the earliest date your exports cover.
After it completes, run `bruin validate my-search-pipeline` and schedule the
pipeline daily.

Reload a specific range without touching the rest of the history:

```bash
bruin run --start-date 2026-06-01 --end-date 2026-06-30 my-search-pipeline
```

## Customize it

The `web_stage` models are the interface; build your own reports on those rather
than against the raw exports. Common first changes:

- Set `key_event_names` to the key events your property actually reports.
- Replace `brand_query_pattern` with a lookup table if one regex cannot describe
  your brand.
- Adjust the `page_path` macro if your URLs carry meaningful query parameters or
  locale prefixes.
- Group queries into topics by adding a classification model beside
  `gsc_url_query_daily`, then aggregate the query-grain reports over it.

If you want to chart these tables, DAC dashboards read them directly; see the
[DAC installation guide](https://getbruin.com/docs/dac/getting-started/installation.html).

## Configure connections

Initializing the template adds a placeholder connection to the repository-level
`.bruin.yml`. Keep the name `gcp-default`, or rename it consistently in both
`.bruin.yml` and `pipeline.yml`.

```yaml
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          project_id: your-gcp-project-id
          service_account_file: /path/to/service-account.json
```

Do not commit credentials. If the GA4 or Search Console export lives in a
different project from the one you write to, qualify the dataset variables with
that project and grant the service account read access there.
