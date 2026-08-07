# Google Analytics and Search Console Reporting on BigQuery

`google-web-analytics` builds an organic search reporting layer on top of
GA4 and Search Console data you already have in BigQuery. It does not ingest
anything: both products export to BigQuery natively, so this template starts where
those exports land, conforms them in `web_analytics_staging`, and publishes nine reports in
`web_analytics_reports`.

Every report answers a question GA4 and Search Console cannot answer themselves —
not because the data is missing, but because it is split across two products that
never join, capped at a thousand exported rows, or aggregated behind an "(other)"
bucket. The template contains 19 assets and is meant to be edited.

It is set up for a **B2B SaaS** motion out of the box — value carried by key events
rather than ecommerce transactions, pages split by the job they do, and queries
split by commercial intent — while remaining usable for any site that runs both
exports. See [B2B SaaS setup](#b2b-saas-setup).

## Requirements

You need both exports already running and landing in BigQuery:

- The [GA4 BigQuery export](https://support.google.com/analytics/answer/9823238)
  with **streaming** export enabled, which creates a dataset named
  `analytics_<property_id>` containing `events_intraday_YYYYMMDD` tables. The
  template reads those and only those; see [Scope](#scope-streaming-export-only).
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
google-web-analytics/
├── pipeline.yml
├── README.md
├── macros/
│   ├── search.sql
│   └── url.sql
└── assets/
    ├── web_analytics_raw/
    │   ├── ga4_events_intraday.asset.yml
    │   ├── gsc_searchdata_url_impression.asset.yml
    │   ├── gsc_searchdata_site_impression.asset.yml
    │   └── gsc_export_log.asset.yml
    ├── web_analytics_staging/
    │   ├── ga4_sessions.sql
    │   ├── ga4_page_daily.sql
    │   ├── gsc_url_query_daily.sql
    │   ├── gsc_site_query_daily.sql
    │   ├── gsc_position_click_curve.sql
    │   └── gsc_export_log.sql
    └── web_analytics_reports/
        ├── gsc_brand_split_weekly.sql
        ├── gsc_query_opportunities.sql
        ├── gsc_query_cannibalization.sql
        ├── gsc_page_trend.sql
        ├── gsc_new_and_lost_queries.sql
        ├── gsc_competitor_visibility.sql
        ├── ga4_gsc_landing_page_performance.sql
        ├── ga4_gsc_query_value.sql
        └── ga4_gsc_intent_pipeline.sql
```

Every asset follows one rule: the folder under `assets/` is exactly the dataset
name, and the file name is exactly the table name. Each table is prefixed with the
platform it comes from — `ga4_`, `gsc_`, or `ga4_gsc_` where the two are joined —
so another platform can be added later without renaming anything.

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
| `key_event_names` | `["sign_up", "trial_start", "demo_request", "generate_lead", "purchase"]` | GA4 events counted as conversions. Match these to the key events configured on your property. |
| `key_event_values` | `{demo_request: 400, trial_start: 150, sign_up: 50, generate_lead: 120}` | USD value of the pipeline each key event creates. **Set this first** — without it every value metric reads zero on a site with no ecommerce revenue. |
| `demo_event_names` | `["demo_request", "request_demo", "book_demo", "contact_sales"]` | Events meaning "the prospect asked to talk to sales". |
| `signup_event_names` | `["sign_up", "trial_start", "start_trial"]` | Self-serve product entry, kept separate from demo requests. |
| `competitor_names` | `["competitor-one", "competitor-two"]` | Competitors named in comparison queries. Each labels the query, so visibility is tracked per rival. |
| `commercial_query_pattern` | pricing, alternatives, vs, best, review, … | Modifiers marking a query as commercially motivated rather than informational. |
| `support_path_pattern` | `^/(docs\|help\|support\|…)` | Paths whose organic traffic is existing customers, not prospects. |
| `content_path_pattern` | `^/(blog\|resources\|guides\|…)` | Paths holding top-of-funnel content marketing. |
| `source_lookback_days` | `4` | Extra days re-read before the run window, so late and revised data heals. |
| `reporting_window_days` | `180` | Trailing days of history the reports aggregate. Bounds the bytes each report scans. |
| `trend_window_days` | `28` | Length of the current and prior comparison windows. |
| `min_query_impressions` | `100` | Minimum window impressions for a query to appear in a query-grain report. |
| `min_page_impressions` | `100` | Minimum window impressions for a page to appear in a page-grain report. |

Override any of them per run without editing the file:

```bash
bruin run --var reporting_window_days=365 google-web-analytics
```

`brand_query_pattern`, `competitor_names`, `commercial_query_pattern`,
`support_path_pattern`, and `content_path_pattern` are all evaluated **inside the
staging models**, so their results are materialized. Changing any of them means
rebuilding `web_analytics_staging`, not just the reports:

```bash
bruin run --full-refresh my-pipeline/assets/web_analytics_staging
```

Overriding one with `--var` on a report alone changes nothing, because the report
reads the classification that staging already stored.

The brand pattern is the one setting worth getting right before you read anything,
because it splits the traffic that measures SEO work from the traffic that measures
brand awareness. It is an RE2 pattern matched case-insensitively, and apostrophes
are safe to use — `(levi's|levis)` and `(o'reilly|oreilly)` both work, because the
`query_brand_type` macro rewrites each apostrophe into the `[']` character class
before it reaches SQL.

## What it creates

### `web_analytics_raw` — the Google exports

Four `bq.source` assets stand in for the tables Google owns. They never execute;
they exist so the exports appear as the upstream of the staging models rather than
the graph starting mid-pipeline, and so the columns the pipeline relies on are
documented in one place.

Their names are stable logical identifiers — `web_analytics_raw.ga4_events_intraday` and so
on — because Bruin validates asset names before Jinja renders, so a name cannot
contain the dataset variable or the `*` wildcard. The real table each one stands
for is named in its description.

### `web_analytics_staging` — conformed models

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

`page_hostname` plus `page_path` is the join key across both systems, produced by
the shared macros in `macros/url.sql`. Search Console reports a canonical absolute
URL and GA4 reports whatever the tag collected, so both sides are lowercased and
stripped of scheme, query string, fragment, and trailing slashes. If your site
needs different rules — locale prefixes, pagination parameters that matter —
change the macro once and every model follows.

The hostname stays in the grain of every page-level report on purpose. A property
that spans hosts routinely serves the same path on more than one of them, and a
docs or blog subdomain is a different page from the marketing site even when both
answer `/guide`. Joining on the path alone would sum their impressions and revenue
together and dilute the value of whichever host actually earns it. The cost is
that a property whose two systems disagree about the canonical host — GA4
recording a visit before a `www` redirect, say — reports that page twice, once as
`search_only` and once as `ga4_only`, instead of hiding the disagreement inside a
merged row. If you would rather see one row per path, aggregate over
`page_hostname` in your own model on top of these reports.

### `web_analytics_reports` — the seven reports

| Report | The question | Why neither product answers it |
| --- | --- | --- |
| `gsc_brand_split_weekly` | How much demand is branded versus non-branded? | Search Console has no concept of a brand, so its headline click trend mixes discovery with people who already knew you. |
| `gsc_query_opportunities` | Which queries are leaving clicks on the table, and how many? | Search Console shows CTR and position side by side but never says whether a CTR is good *for that position*. |
| `gsc_query_cannibalization` | Which queries do several of my own pages compete for? | The interface can filter by query or group by page, never pivot one against the other. |
| `gsc_page_trend` | Which pages are quietly decaying? | Period comparison works one view at a time and within a thousand exported rows, so the long tail never surfaces. |
| `gsc_new_and_lost_queries` | What did I start and stop ranking for? | Comparing periods shows how shared queries moved, not which ones appeared or vanished. |
| `ga4_gsc_landing_page_performance` | What happened after the click? | Search Console stops at the click; GA4 never receives the query or the impression. |
| `ga4_gsc_query_value` | Which queries make money? | Google deliberately never passes the query to analytics. |
| `gsc_competitor_visibility` | Where do I stand on each rival's comparison queries? | Search Console scatters them through a row-capped export with no idea they belong together. |
| `ga4_gsc_intent_pipeline` | Does content marketing actually produce pipeline? | Needs query intent, page role, and post-click outcomes at once. None of the three exists in either product. |

## Reading the reports

### Start with `ga4_gsc_landing_page_performance`

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
no visibility into Bing or any other engine. `web_analytics_staging.ga4_sessions` exposes both
`is_organic_search_session` and the narrower `is_google_organic_session` so you can
choose deliberately.

### `ga4_gsc_query_value` is modelled, not measured

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
here fall short of those in `ga4_gsc_landing_page_performance`.

### Opportunity sizing

`gsc_query_opportunities` gives two numbers, and they answer different
questions:

- `clicks_at_expected_ctr` — what fixing the title and description alone would
  return, holding position constant.
- `clicks_at_top_three_ctr` — what ranking in the top three would return, holding
  impressions constant.

Both are estimates from observed behaviour rather than forecasts; impressions move
when position moves. `is_expected_ctr_reliable` tells you whether the click-curve
bucket behind the comparison has enough impressions to trust.

## B2B SaaS setup

Three things about a B2B SaaS site break a conventional SEO reporting stack, and
the template is built around them.

### Revenue is not in GA4, so value comes from key events

Revenue is recognized in a CRM or billing system weeks after the visit. The GA4
export carries no purchase amount, so any report that ranks pages by
`ecommerce.purchase_revenue` ranks every page at zero.

`key_event_values` fixes that by pricing each key event. A reasonable starting
point for each weight is your average deal size multiplied by the rate at which
that event becomes a closed deal:

```yaml
key_event_values:
  type: object
  default:
    demo_request: 400     # 20k ACV x 2% demo-to-close
    trial_start: 150
    sign_up: 50
    generate_lead: 120
```

Every value column then works: `outcome_value_per_search_click_usd` on
`ga4_gsc_landing_page_performance` and `modelled_outcome_value_per_click_usd` on
`ga4_gsc_query_value` are the two columns to rank a content roadmap by. `purchase`
is deliberately absent from the weights because real ecommerce revenue is added
from the GA4 ecommerce fields instead; giving it a weight would double count it.

These figures are **modelled**. They exist to rank pages and queries against each
other, not to be reported as recognized revenue.

### Documentation traffic is not acquisition traffic

Docs and help centres routinely out-rank the marketing site. That traffic is
overwhelmingly existing customers looking something up, and leaving it in the same
pool as pricing pages makes acquisition conversion rates look far worse than they
are for reasons that have nothing to do with acquisition.

Every page therefore carries a `page_role` of `product`, `content`, or `support`,
driven by `support_path_pattern` and `content_path_pattern`. Filter to `product`
when judging acquisition. In a typical shape, support pages are a large share of
organic clicks and none of the pipeline.

### Clicks and pipeline are not the same demand

`ga4_gsc_intent_pipeline` crosses query intent with page role, which settles the
recurring argument about whether content marketing works. A representative result:

| intent | page role | clicks | click share | value share | value / click |
| --- | --- | ---: | ---: | ---: | ---: |
| informational | support | 7,840 | 41% | 0% | $0.00 |
| informational | content | 7,448 | 39% | 13% | $3.38 |
| commercial | product | 1,008 | 5% | 27% | $51.11 |
| branded | product | 672 | 4% | 23% | $65.00 |
| competitor | product | 616 | 3% | 25% | $77.27 |

Informational demand is 80% of the clicks and 13% of the value; commercial,
branded, and competitor demand is 12% of the clicks and 75% of the value. That is
not automatically an argument against content marketing — informational content is
how a site earns the authority to rank commercially later — but it is the number
that should decide what gets built next.

`gsc_competitor_visibility` breaks the highest-intent slice out per rival.
Populate `competitor_names` with real competitors, then look at `position_band`:
ranking eleventh for "rival alternatives" means the demand exists and you are
nowhere a searcher will look, which is usually the cheapest pipeline available to
a startup. `has_dedicated_comparison_page` flags where a blog post is accidentally
ranking for a comparison query instead of a real comparison page.

### If you are not B2B SaaS

Nothing here is mandatory. Set `key_event_values` to your own events, point
`support_path_pattern` and `content_path_pattern` at your own structure, and leave
`competitor_names` empty to switch competitor classification off — the intent
column then falls back to branded, commercial, and informational. A site with real
ecommerce revenue can leave the weights empty and the value columns still work from
`purchase_revenue_in_usd` alone.

## Check your export shape before the first run

Two properties of the GA4 export decide whether this template returns anything at
all, and both fail quietly rather than loudly. Verified against a real
streaming-only property, where the daily-table filter matched 0 rows for a window
that held 9,089 rows in intraday tables.

### Scope: streaming export only

This template reads `events_intraday_*` and nothing else. It does not read the
daily `events_YYYYMMDD` tables, and it does not read `pseudonymous_users_*` or
`users_*`. Confirm your property produces intraday tables:

```sql
SELECT COUNTIF(REGEXP_CONTAINS(table_id, r'^events_intraday_')) AS intraday,
       COUNTIF(REGEXP_CONTAINS(table_id, r'^events_[0-9]{8}$'))  AS daily
FROM `your_project.your_ga4_dataset.__TABLES__`
```

A property with streaming export enabled reports `intraday > 0` and the template
works as shipped. A property that produces only daily tables reports
`intraday = 0`, and every GA4 model returns nothing — point `ga4_dataset` at a
streaming-enabled property, or change the two GA4 models to read `events_*`.

Intraday tables are a complete record for past dates: on the property this was
built against, 981 of them span 2023-12-01 onward with events covering full days.
The one partial day is today, which the default run window excludes and which
whole-day replacement corrects on the next run.

### Whether session attribution is populated

```sql
SELECT COUNT(*) AS events,
       COUNTIF(session_traffic_source_last_click.cross_channel_campaign
                 .default_channel_group IS NOT NULL) AS session_scoped,
       COUNTIF(traffic_source.medium IS NOT NULL)     AS user_first_touch
FROM `your_project.your_ga4_dataset.events_intraday_*`
WHERE _TABLE_SUFFIX >= '20260101'
```

Streaming-only exports return `session_scoped = 0`: Google does not populate
session-scoped traffic source in intraday tables. `web_analytics_staging.ga4_sessions`
therefore falls back through `collected_traffic_source` and then the user-scoped
`traffic_source`, and records which one it used in `traffic_source_basis`.

That fallback is what keeps the organic reports from being empty, but it is weaker
than GA4's own session attribution and **will not tie out to the GA4 interface**.
On the property tested, all 6,433 sessions resolved through `user_first_touch` or
`event_collected`, none through `session_last_click`. Check the
`traffic_source_basis` distribution before quoting any channel number:

```sql
SELECT traffic_source_basis, COUNT(*) FROM web_analytics_staging.ga4_sessions GROUP BY 1
```

### Your event names are probably not the defaults

`key_event_names` and `key_event_values` ship with placeholder B2B SaaS events.
The property tested had none of them — its form conversion fires `form_start`. List
what you actually send before setting the weights:

```sql
SELECT event_name, COUNT(*) FROM `your_project.your_ga4_dataset.events_intraday_*`
WHERE _TABLE_SUFFIX >= '20260101' GROUP BY 1 ORDER BY 2 DESC
```

## Timing, freshness, and incremental behaviour

Search Console publishes with a two-to-three day delay and revises history in
place. GA4 finalizes a daily table only after dropping its intraday table. So the
staging models widen their read window by `source_lookback_days` and replace whole
days with `delete+insert`, which makes late and restated data self-healing on the
next run. `web_analytics_staging.gsc_export_log` is where you confirm what Google actually
published: a bumped `epoch_version` means Google restated a date rather than the
site changing.

The GA4 models read the `events_intraday_*` wildcard, so the only partial day is
today's; see [Scope](#scope-streaming-export-only).

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
bruin init google-web-analytics my-search-pipeline
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

The `web_analytics_staging` models are the interface; build your own reports on those rather
than against the raw exports. Common first changes:

- Set `key_event_names` and `key_event_values` to the key events your property
  actually reports, and price each one.
- Put your real competitors in `competitor_names`.
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
