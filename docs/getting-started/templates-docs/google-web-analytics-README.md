# Google Analytics and Search Console Reporting on BigQuery

## At a glance

- Reads your existing GA4 and Search Console exports in BigQuery
- Ingests nothing — it starts from data you already have
- Joins two Google products that never talk to each other
- Cleans both exports into one tidy staging layer
- Publishes nine reports on your organic search performance
- Answers questions neither Google tool can answer alone
- Ships as a starter template you edit for your business
- **Builds the foundational search models and context an AI agent can analyze and act on**

`google-web-analytics` turns the GA4 and Google Search Console exports you already
have in BigQuery into an organic search reporting layer. It ingests nothing: both
products export to BigQuery natively, so the template starts where those exports
land, conforms them into a staging layer, and publishes nine reports.

Each report answers a question neither product answers on its own — not because the
data is missing, but because it is split across two systems that never join,
capped at a thousand rows in the interface, or hidden behind an "(other)" bucket.

This is a **starter template**, not a finished product. It ships 19 assets, a
documented column schema on every one of them, and 166 quality checks, so you can
run it on day one and then edit it. The defaults describe a lead-generation site;
[Adapt it to your business](#adapt-it-to-your-business) covers changing them for
ecommerce, publishing, or anything else.

## What you need

Two Google exports, already running and landing in BigQuery:

| Export | Creates | The template reads |
| --- | --- | --- |
| [GA4 BigQuery export](https://support.google.com/analytics/answer/9823238) with **streaming** enabled | dataset `analytics_<property_id>` | `events_intraday_*` |
| [Search Console bulk data export](https://support.google.com/webmasters/answer/12918484) | a dataset you name | `searchdata_site_impression`, `searchdata_url_impression`, `ExportLog` |

Neither export backfills — each starts collecting the day you enable it, so the
earliest date you can report on is the day you turned it on. A BigQuery connection
whose credentials can read both export datasets and write the three datasets this
pipeline creates.

> Streaming export is required. See
> [Scope: streaming export only](#scope-streaming-export-only) before your first run.

## How it works

Three datasets, in dependency order:

```text
web_analytics_raw          web_analytics_staging          web_analytics_reports
(Google's tables,     →    (conformed, incremental)  →    (rebuilt each run)
 documented, no-op)
```

- **`web_analytics_raw`** — four `bq.source` assets that never execute. They exist
  so Google's tables appear as the upstream of the graph rather than the lineage
  starting mid-pipeline, and so the columns the pipeline depends on are documented
  in one place.
- **`web_analytics_staging`** — six models that flatten GA4's nested event export
  into sessions and pages, conform both Search Console tables, and classify every
  query and URL once. These are the interface you should build on.
- **`web_analytics_reports`** — nine reports, each rebuilt in full over a trailing
  window on every run.

Staging models replace whole days with `delete+insert` and re-read a few extra days
before the run window, which makes late and restated source data self-healing.
Reports use create+replace, so any run is safe to repeat.

The join between the two systems is `page_hostname` + `page_path`, produced by the
shared macros in `macros/url.sql`. Search Console reports a canonical absolute URL
and GA4 reports whatever the tag collected, so both sides are lowercased and
stripped of scheme, query string, fragment, and trailing slash. Change that macro
once and every model follows.

### Project structure

```text
google-web-analytics/
├── pipeline.yml
├── macros/
│   ├── search.sql                 # query classification, list/pattern helpers
│   └── url.sql                    # URL normalization, page role
├── dashboards/
│   ├── 01-overview.yml            # headline KPIs and trends
│   ├── 02-ga4-insights.yml        # session and page deep dive
│   └── 03-gsc-insights.yml        # search demand deep dive
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

One naming rule throughout: the folder under `assets/` is exactly the dataset name
and the file name is exactly the table name. Each table is prefixed with the
platform it comes from — `ga4_`, `gsc_`, or `ga4_gsc_` where the two are joined — so
another platform can be added later without renaming anything.

The raw assets carry stable logical names such as `web_analytics_raw.ga4_events_intraday`
because Bruin validates asset names before Jinja renders, so a name cannot contain
a variable or the `*` wildcard. Each one names the real table it stands for in its
description.

## What it builds

### `web_analytics_staging`

| Asset | Grain | What it gives you |
| --- | --- | --- |
| `ga4_sessions` | session | One row per session rebuilt from raw events — no sampling, no "(other)" bucket. Carries landing page, channel, device, geography, and per-session outcome counts. |
| `ga4_page_daily` | day × host × page × channel × source × device | Page views from any session, not only sessions that landed on the page. |
| `gsc_site_query_daily` | day × query × country × device × search type | Property-level impressions, counted once per property. Use this for totals that reconcile with the Search Console interface. |
| `gsc_url_query_daily` | day × URL × query × country × device × search type | The only grain carrying both page and query. Summing impressions across pages overstates property totals by design. |
| `gsc_position_click_curve` | property × search type × position | The CTR your property actually earns at each position, measured from its own history. |
| `gsc_export_log` | table × reporting date | When Google published each date, and whether it later restated it. |

Both Search Console models keep the rows where Google withheld the query: the
impressions and clicks are real, so dropping them would understate daily totals.
They carry a NULL query and a `query_brand_type` of `anonymized`, and the
query-grain reports exclude them explicitly.

Every page-level report keeps `page_hostname` in its grain on purpose. A property
that spans hosts routinely serves the same path on more than one, and a docs or
blog subdomain is a different page from the marketing site even when both answer
`/guide`. Joining on path alone would sum their impressions and value together.
The cost is that when the two systems disagree about the canonical host, the page
appears twice — once as `search_only`, once as `ga4_only` — instead of hiding the
disagreement in a merged row. Aggregate over `page_hostname` in your own model if
you would rather see one row per path.

### `web_analytics_reports`

| Report | The question | Why neither product answers it |
| --- | --- | --- |
| `gsc_brand_split_weekly` | How much demand is branded versus non-branded? | Search Console has no concept of a brand, so its headline click trend mixes discovery with people who already knew you. |
| `gsc_query_opportunities` | Which queries leave clicks on the table, and how many? | The interface shows CTR and position side by side but never says whether a CTR is good *for that position*. |
| `gsc_query_cannibalization` | Which queries do several of my own pages compete for? | The interface can filter by query or group by page, never pivot one against the other. |
| `gsc_page_trend` | Which pages are quietly decaying? | Period comparison works one view at a time within a thousand exported rows, so the long tail never surfaces. |
| `gsc_new_and_lost_queries` | What did I start and stop ranking for? | Comparing periods shows how shared queries moved, not which ones appeared or vanished. |
| `gsc_competitor_visibility` | Where do I stand on each rival's comparison queries? | Search Console scatters them through a row-capped export with no idea they belong together. |
| `ga4_gsc_landing_page_performance` | What happened after the click? | Search Console stops at the click; GA4 never receives the query or the impression. |
| `ga4_gsc_query_value` | Which queries produce outcomes? | Google deliberately never passes the query to analytics. |
| `ga4_gsc_intent_pipeline` | Does content marketing actually produce anything? | Needs query intent, page role, and post-click outcomes at once. None of the three exists in either product. |

## Reading the reports

### Start with `ga4_gsc_landing_page_performance`

This is the join the other reports lean on. Each page carries its Search Console
visibility next to what GA4 recorded afterwards, so the per-click and
per-thousand-impression value columns rank pages by outcome rather than by traffic.

It is a full outer join on purpose, and `coverage_status` explains every mismatch:

| `coverage_status` | Meaning | Usual cause |
| --- | --- | --- |
| `matched` | Both systems agree the page gets Google search traffic. | |
| `search_only` | Search Console reports clicks GA4 never recorded. | Missing or blocked tracking, a redirect chain, or bot filtering. |
| `ga4_only` | GA4 recorded organic sessions Search Console has no data for. | Traffic from a non-Google engine, or a page excluded from the property. |
| `session_shortfall` | GA4 saw fewer than half the reported clicks. | Consent-mode loss, slow tag load, or a redirect on entry. |

The GA4 side is restricted to Google organic sessions, because Search Console has
no visibility into other engines. `ga4_sessions` exposes both
`is_organic_search_session` and the narrower `is_google_organic_session` so you can
choose deliberately.

### Opportunity sizing gives two different numbers

`gsc_query_opportunities` answers two separate questions:

- `clicks_at_expected_ctr` — what fixing the title and description alone would
  return, holding position constant.
- `clicks_at_top_three_ctr` — what ranking in the top three would return, holding
  impressions constant.

Both are estimates from your own observed behaviour, not forecasts; impressions
move when position moves. `is_expected_ctr_reliable` tells you whether the
click-curve bucket behind the comparison has enough impressions to trust.

### `ga4_gsc_query_value` is modelled

Google never passes the search query to analytics, so query-level outcomes cannot
be measured — only allocated. Each page's GA4 outcomes are split across the queries
that sent clicks to it, in proportion to those clicks. Within a page the weights
sum to one, so page totals are preserved exactly and only the split between queries
is estimated.

The assumption is that every query sending traffic to a page converts at that
page's average rate. That is wrong in a predictable direction: a high-intent query
really does convert better than the page average, so its value is understated and a
broad informational query's is overstated. Rank queries against each other with it;
do not report the figure as measured.

Clicks Google withheld are left out of the denominator, so their share of a page's
outcome spreads across that page's disclosed queries. A page whose clicks were all
withheld contributes nothing, which is why totals here fall short of those in
`ga4_gsc_landing_page_performance`.

## Scope and limitations

Read this section before you quote a number from these tables to anyone.

### Scope: streaming export only

The template reads `events_intraday_*` and nothing else. It does not read the daily
`events_YYYYMMDD` tables, `pseudonymous_users_*`, or `users_*`. Confirm your
property produces intraday tables:

```sql
SELECT COUNTIF(REGEXP_CONTAINS(table_id, r'^events_intraday_')) AS intraday,
       COUNTIF(REGEXP_CONTAINS(table_id, r'^events_[0-9]{8}$'))  AS daily
FROM `your_project.your_ga4_dataset.__TABLES__`
```

`intraday > 0` and the template works as shipped. `intraday = 0` means every GA4
model returns nothing — enable streaming export, or change the two GA4 models to
read `events_*` instead. The failure is silent: empty tables, not an error.

On a streaming-only property, intraday tables are a complete record of past days
and are not cleaned up, so the wildcard covers your full history. The only partial
day is today, which the default run window excludes and which whole-day replacement
corrects on the next run.

### GA4 session attribution is weaker in intraday tables

Google does not populate session-scoped traffic source in intraday tables. Check
your own property:

```sql
SELECT COUNT(*) AS events,
       COUNTIF(session_traffic_source_last_click.cross_channel_campaign
                 .default_channel_group IS NOT NULL) AS session_scoped,
       COUNTIF(collected_traffic_source.manual_source IS NOT NULL) AS event_collected,
       COUNTIF(traffic_source.source IS NOT NULL) AS user_first_touch
FROM `your_project.your_ga4_dataset.events_intraday_*`
WHERE _TABLE_SUFFIX >= '20260101'
```

Expect `session_scoped = 0`. `ga4_sessions` therefore falls back through
`collected_traffic_source`, then the user-scoped `traffic_source`, and records
which one it used in `traffic_source_basis`.

Sessions where none of the three is populated get
`traffic_source_basis = 'unavailable'` and a channel of `Unassigned`, and are never
counted as organic. Expect that bucket to be large — two thirds of sessions on the
property this was tested against.

That sounds worse than it is. Checked against the GA4 interface, those
`unavailable` sessions are almost exactly the ones GA4 itself labels **Direct**, so
leaving them out of organic is mostly right rather than lost traffic. The real cost
is narrower: organic sessions came in roughly 16% below what GA4 reported. Treat
organic as a floor, not a total, but a close floor. Measure your own split before
quoting any channel figure:

```sql
SELECT traffic_source_basis, COUNT(*) AS sessions
FROM `your_project.web_analytics_staging.ga4_sessions`
GROUP BY 1 ORDER BY sessions DESC
```

These numbers will not tie out to the GA4 interface, which applies its own
attribution model on top of the same events.

### Value columns are modelled, not recognized revenue

Unless your site sends GA4 ecommerce revenue, all value in these reports comes from
the weights you set in `key_event_values`. They exist to rank pages and queries
against each other. They are not recognized revenue, bookings, or cash, and should
not be reported as such.

### The two systems will never tie out exactly

Session counts and click counts come from different measurement systems.
`session_per_click_ratio` sits near one on healthy pages; treat a low value as a
tracking lead, not a bug in the report. Search Console dates are Pacific Time while
GA4 `event_date` uses the property's reporting timezone, so where those differ a
day's numbers are offset by a few hours of activity between the two systems.

### Freshness, restatement, and window edges

Search Console publishes with a two-to-three day delay and revises history in
place. The staging models widen their read window by `source_lookback_days` and
replace whole days, so late and restated data heals on the next run.
`gsc_export_log` is where you confirm what Google actually published — a bumped
`epoch_version` means Google restated a date rather than your site changing.

Sessions that cross midnight span two daily export tables; a session split by the
window edge is completed by the following run, because the lookback re-reads and
replaces that day.

The reports are rebuilt over a trailing `reporting_window_days` window, which
bounds what each one scans. The trend reports need at least
`2 × trend_window_days` of staged history before they say anything useful.

### An event you do not send is a silently empty column

The shipped `key_event_names` are placeholders. If your property does not fire
them, the matching report columns are zero rather than an error. List what you
actually send before configuring anything:

```sql
SELECT event_name, COUNT(*) AS events
FROM `your_project.your_ga4_dataset.events_intraday_*`
WHERE _TABLE_SUFFIX >= '20260101'
GROUP BY 1 ORDER BY events DESC
```

### How closely this reconciles with Google's own interfaces

Validated against both Google interfaces over a calendar month on a live property.

Every Search Console figure matched exactly: clicks, impressions, CTR, and average
position, and each of those again broken down by day, device, and country. The GA4
side matched page views and users to within a fraction of a percent. Sessions came
in about 2% below the interface, because the export itself holds slightly fewer
sessions than GA4 reports — the models reproduce the export, and that residual gap
is not something a transformation can close. Organic sessions are the one real gap,
for the attribution reason above.

## Configure it

All configuration lives in the `variables` block of `pipeline.yml`. At minimum, set
the two dataset names and your brand pattern:

```yaml
variables:
  ga4_dataset:
    type: string
    default: analytics_123456789        # your GA4 export dataset
  search_console_dataset:
    type: string
    default: searchconsole              # your Search Console export dataset
  brand_query_pattern:
    type: string
    default: "(acme|acmecorp|acme co)"  # your brand and its misspellings
```

| Variable | Default | What it controls |
| --- | --- | --- |
| `ga4_dataset` | `analytics_000000000` | GA4 export dataset. Qualify it with a project when the export lives outside the connection's project. |
| `search_console_dataset` | `searchconsole` | Search Console export dataset. |
| `brand_query_pattern` | `(example\|exampl\|examp1e)` | Case-insensitive RE2 pattern marking a query as branded. |
| `key_event_names` | `["sign_up", "trial_start", "demo_request", "generate_lead", "purchase"]` | GA4 events counted as conversions. Match to the key events on your property. |
| `key_event_values` | `{demo_request: 400, trial_start: 150, sign_up: 50, generate_lead: 120}` | Value of the outcome each key event creates. **Set this** — without it every value metric reads zero on a site with no ecommerce revenue. |
| `demo_event_names` | `["demo_request", "request_demo", "book_demo", "contact_sales"]` | Events meaning "asked to talk to a person". |
| `signup_event_names` | `["sign_up", "trial_start", "start_trial"]` | Self-serve entry, kept separate from the above. |
| `competitor_names` | `["competitor-one", "competitor-two"]` | Names appearing in comparison queries. Each labels the query, so visibility is tracked per rival. Leave empty to switch this off. |
| `commercial_query_pattern` | pricing, alternatives, vs, best, review, … | Modifiers marking a query as commercially motivated rather than informational. |
| `support_path_pattern` | `^/(docs\|help\|support\|…)` | Paths whose traffic is existing customers, not prospects. |
| `content_path_pattern` | `^/(blog\|resources\|guides\|…)` | Paths holding top-of-funnel content. |
| `source_lookback_days` | `4` | Extra days re-read before the run window, so late and revised data heals. |
| `reporting_window_days` | `180` | Trailing days of history the reports aggregate. Bounds the bytes each report scans. |
| `trend_window_days` | `28` | Length of the current and prior comparison windows. |
| `min_query_impressions` | `100` | Minimum window impressions for a query to appear in a query-grain report. |
| `min_page_impressions` | `100` | Minimum window impressions for a page to appear in a page-grain report. |

Override any of them for a single run without editing the file:

```bash
bruin run --var reporting_window_days=365 my-search-pipeline
```

`brand_query_pattern`, `competitor_names`, `commercial_query_pattern`,
`support_path_pattern`, and `content_path_pattern` are evaluated **inside the
staging models**, so their results are materialized. Changing one means rebuilding
staging, not just the reports:

```bash
bruin run --full-refresh my-search-pipeline/assets/web_analytics_staging
```

Overriding one with `--var` on a report alone changes nothing, because the report
reads the classification staging already stored.

Get `brand_query_pattern` right before reading anything else: it splits the traffic
that measures SEO work from the traffic that measures brand awareness. It is an RE2
pattern matched case-insensitively, and apostrophes are safe — `(levi's|levis)` and
`(o'reilly|oreilly)` both work, because the macro rewrites each apostrophe into the
`[']` character class before it reaches SQL.

## Adapt it to your business

The defaults describe a lead-generation site: value carried by key events rather
than transactions, pages split by the job they do, queries split by commercial
intent. Every part of that is a variable or a macro, and nothing about the model is
specific to one industry.

### 1. Decide where value comes from

**If your site has GA4 ecommerce revenue**, leave `key_event_values` empty. The
value columns work from `purchase_revenue_in_usd` alone, and `purchase` is
deliberately absent from the default weights so real revenue is never double
counted.

**If your revenue lands somewhere else** — a CRM weeks after the visit, a
subscription biller, an offline sale — GA4 carries no amount, so any report ranking
pages by revenue ranks every page at zero. Price each key event instead. A
reasonable starting point is the average value of a converted customer multiplied by
the rate at which that event becomes one:

```yaml
key_event_values:
  type: object
  default:
    demo_request: 400     # 20k average deal x 2% demo-to-close
    trial_start: 150
    sign_up: 50
    generate_lead: 120
```

**If you are a publisher or have no conversions at all**, leave the weights empty
and rank on the click, impression, and engagement columns, which never depend on
value.

### 2. Classify your URLs

Every page gets a `page_role` of `product`, `content`, or `support` from
`support_path_pattern` and `content_path_pattern`. The point is to stop mixing
audiences: documentation and help centres routinely out-rank the marketing site,
and that traffic is overwhelmingly existing customers, so leaving it in the same
pool as pricing pages makes acquisition conversion rates look far worse than they
are. Filter to `product` when judging acquisition.

Point the two patterns at your own structure — `/support` and `/kb` for a support
centre, `/news` and `/features` for a publisher, `/collections` and `/products` for
a store. Or redefine `page_role` in `macros/url.sql` entirely if three buckets do
not fit; every model downstream follows.

### 3. Set brand, competitor, and intent patterns

`brand_query_pattern` needs your brand and its common misspellings.
`competitor_names` takes real competitor names and labels queries per rival, which
`gsc_competitor_visibility` then breaks out — ranking eleventh for "rival
alternatives" means the demand exists and you are nowhere a searcher will look.
Leave the list empty to switch competitor classification off; the intent column then
falls back to branded, commercial, and informational.

`commercial_query_pattern` decides which queries count as commercially motivated.
Its default modifiers are broad by design and will over-match on some sites — a
term like `api` or `integration` is commercial for a software vendor and purely
informational for a publisher. Review it against your own top queries.

### 4. Read intent against page role

`ga4_gsc_intent_pipeline` crosses query intent with page role, which is what
settles arguments about whether content marketing earns its keep. The shape it
produces, illustrative only:

| intent | page role | click share | value share | value / click |
| --- | --- | ---: | ---: | ---: |
| informational | support | 41% | 0% | $0.00 |
| informational | content | 39% | 13% | $3.38 |
| commercial | product | 5% | 27% | $51.11 |
| branded | product | 4% | 23% | $65.00 |
| competitor | product | 3% | 25% | $77.27 |

A split like this says informational demand is most of the clicks and little of the
value. That is not automatically an argument against content marketing —
informational content is often how a site earns the authority to rank commercially
later — but it is the number that should decide what gets built next.

## Run it

```bash
bruin init google-web-analytics my-search-pipeline
```

Set the dataset names and brand pattern in `pipeline.yml`, then validate:

```bash
bruin validate --fast my-search-pipeline
```

The staging models are incremental, so the destination tables have to exist before
an ordinary run can replace days in them. Build them once with `--full-refresh`,
scoped to the earliest date your exports cover:

```bash
bruin run --full-refresh --no-validation \
  --start-date 2026-01-01 --end-date 2026-08-12 my-search-pipeline
```

After that, run `bruin validate my-search-pipeline` and schedule the pipeline
daily. Reload a specific range without touching the rest of the history:

```bash
bruin run --start-date 2026-06-01 --end-date 2026-06-30 my-search-pipeline
```

Local runs take their window from `--start-date` and `--end-date`, which both
default to yesterday. `start_date` in `pipeline.yml` is the anchor Bruin Cloud uses
for scheduled backfills — set it to the day your exports began.

## Extend it

Treat `web_analytics_staging` as the interface and build on it rather than against
the raw exports. Common first additions:

- Group queries into topics with a classification model beside
  `gsc_url_query_daily`, then aggregate the query-grain reports over it.
- Replace `brand_query_pattern` with a lookup table if one regex cannot describe
  your brand.
- Adjust the `page_path` macro if your URLs carry meaningful query parameters or
  locale prefixes.
- Join a CRM or billing export onto `ga4_gsc_landing_page_performance` to replace
  modelled value with recognized revenue.
- Add another platform — Bing Webmaster Tools, a paid channel — as a new
  `web_analytics_staging` model with its own prefix.

Every asset declares its full column schema with types, descriptions, and
primary-key marks, so `bruin docs my-search-pipeline` generates a browsable
reference from the same files.

## View the dashboards

`dashboards/` ships three DAC dashboards over the tables this pipeline builds:

| Dashboard | What it covers |
| --- | --- |
| **Web Analytics Overview** | Headline impressions, clicks, CTR, position, organic sessions, and outcome value; weekly demand trends; the brand split; where GA4 and Search Console disagree. |
| **GA4 Insights** | Session and page behaviour — channels, devices, countries, engagement, landing pages, and how much of the traffic the intraday export could attribute at all. Filter by date, channel, device, page role, and country. |
| **GSC Insights** | Search demand — the CTR curve your property earns at each position, click opportunities, page decay, cannibalization, and queries won and lost. Filter by date, search type, device, brand type, intent, and page role. |

They read the same `gcp-default` connection as the pipeline. Install a verified
DAC release with the
[DAC installation guide](https://getbruin.com/docs/dac/getting-started/installation.html),
then validate, execute every query, and serve them:

```bash
dac --config .bruin.yml validate --dir dashboards
dac --config .bruin.yml check --dir dashboards
dac --config .bruin.yml serve --dir dashboards --port 8321
```

Two things to expect on a fresh property. The competitor table stays empty until
`competitor_names` holds real competitors, and every value column reads zero
until `key_event_values` prices events your property actually sends — see
[Adapt it to your business](#adapt-it-to-your-business). Both are configuration,
not errors.

Impressions and clicks are charted separately rather than on one axis, because
impressions outnumber clicks by orders of magnitude on most properties and a
shared axis flattens the click series to zero.

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

To use [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
instead, replace `service_account_file` with
`use_application_default_credentials: true` and authenticate once with
`gcloud auth application-default login`.

Do not commit credentials. If either export lives in a different project from the
one you write to, qualify the dataset variables with that project and grant read
access there.
