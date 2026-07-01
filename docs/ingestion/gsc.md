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
- `site_url`: the property to read from — `sc-domain:example.com` for a Domain property or `https://example.com/` for a URL-prefix property.

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
| `<granularity>:<dimensions>` | site_url, date + dimensions | date | merge | Search Analytics report. `granularity` is `hourly` or `daily`; `dimensions` is an optional comma-separated list of `country`, `device`, `page`, `query` (e.g. `daily:query,country`). |
| `searchAppearance` | site_url, searchAppearance | - | replace | Search Analytics grouped by search appearance type (cannot be combined with other dimensions). |
| `sites` | site_url | - | replace | The sites the service account can access, with permission level. |
| `sitemaps` | site_url, path | - | replace | The sitemaps submitted for the property. |
