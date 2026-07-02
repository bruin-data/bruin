# Google Search Console

[Google Search Console](https://search.google.com/search-console/about) is a free Google service that reports how your site performs in Google Search, exposing search analytics, indexed sites, and submitted sitemaps.

Bruin supports Google Search Console as a source for [Ingestr assets](/assets/ingestr).

## Configuration

### Step 1: Add a connection to .bruin.yml

```yaml
connections:
    gsc:
        - name: 'my-gsc'
          service_account_file: '/path/to/service_account.json'
          site_url: 'sc-domain:example.com'
```

- `service_account_file` (optional): path to the service account JSON key. If omitted, Application Default Credentials are used.
- `service_account_json` (optional): the service account JSON contents (alternative to `service_account_file`).
- `site_url`: the property to read from — `sc-domain:example.com` for a Domain property or `https://example.com/` for a URL-prefix property. Multiple properties can be read at once by passing a comma-separated list (e.g. `sc-domain:example.com,https://blog.example.com/`).

The service account must be added as a user on the property in Search Console, and the Search Console API must be enabled on its Google Cloud project.

### Step 2: Create an asset file

```yaml
name: public.gsc
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'daily:query,country'
  destination: postgres
```

- `source_connection`: the Google Search Console connection defined in `.bruin.yml`.
- `source_table`: the table to ingest (see below).

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `<granularity>:<dimensions>` | site_url, date + dimensions | date | merge | Search Analytics report at the chosen time granularity (`hourly` or `daily`), grouped by the given dimensions. `dimensions` is an optional comma-separated list of `country`, `device`, `page`, `query` (e.g. `daily:query,country`). |
| `searchAppearance` | site_url, searchAppearance | - | replace | Search Analytics grouped by search appearance type (must be requested on its own — cannot be combined with other dimensions). |
| `sites` | site_url | - | replace | The list of sites the service account can access, with permission level. |
| `sitemaps` | site_url, path | - | replace | The sitemaps submitted for each configured property. |

**Granularity:** `hourly` (last ~10 days) or `daily`.

**Dimensions:** `country`, `device`, `page`, `query` (optional; cannot be combined with `searchAppearance`).

**Metrics:** each Search Analytics row includes `clicks`, `impressions`, `ctr`, and `position`.

Data in Search Console typically lags 2–3 days and retains roughly 16 months of history.

## Examples

The examples below show different values for `source_table`. Only the relevant fields are shown — set `destination` and `name` to whatever fits your pipeline.

Search Analytics grouped by query and country (daily):

```yaml
name: public.daily_query_country
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'daily:query,country'
  destination: duckdb
```

Search Analytics grouped by page (daily):

```yaml
name: public.daily_page
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'daily:page'
  destination: duckdb
```

Search Analytics grouped by device (daily):

```yaml
name: public.daily_device
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'daily:device'
  destination: duckdb
```

All dimensions at once (daily):

```yaml
name: public.daily_all
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'daily:query,page,country,device'
  destination: duckdb
```

Hourly granularity grouped by query:

```yaml
name: public.hourly_query
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'hourly:query'
  destination: duckdb
```

Search appearance report (must be on its own):

```yaml
name: public.search_appearance
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'searchAppearance'
  destination: duckdb
```

Accessible sites:

```yaml
name: public.sites
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'sites'
  destination: duckdb
```

Submitted sitemaps:

```yaml
name: public.sitemaps
type: ingestr

parameters:
  source_connection: my-gsc
  source_table: 'sitemaps'
  destination: duckdb
```
