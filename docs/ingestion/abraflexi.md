# ABRA Flexi

[ABRA Flexi](https://www.flexibee.eu/) (formerly Flexibee) is a Czech cloud accounting and ERP system. Bruin reads it through the REST API at `https://<account>.flexibee.eu/c/<company>/<evidence>.json`.

Bruin supports ABRA Flexi as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from ABRA Flexi into your data platform.

Unlike most SaaS connectors, this one is **schema-driven rather than table-driven**. Flexi publishes a machine-readable schema for every register at `/<evidence>/properties.json`, so the connector derives each table's columns and types from the API at read time. There is no fixed table list: any evidence in the company is a valid `source_table`.

To set up an ABRA Flexi connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need a Flexi API user and password, plus the company database code.

Follow the steps below to set up ABRA Flexi correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    abraflexi:
        - name: "abraflexi"
          host: "example.flexibee.eu"
          username: "your_api_user"
          password: "your_password"
          company: "acme_s_r_o_"
```

- `host` (required): The Flexi account host, e.g. `example.flexibee.eu`.
- `path` (optional): A URL path prefix in front of the REST API, only needed for self-hosted installs mounted under a sub-path (e.g. `/flexi`). Leave it empty for cloud Flexi.
- `username` (required): The Flexi API user.
- `password` (required): That user's password.
- `company` (required): The company database code as it appears in the REST path, e.g. `acme_s_r_o_`. It selects **which** set of books to read and is never defaulted — one credential typically reaches every company in the account, and pointing the wrong one at a destination loads the wrong set of books with no error. The company codes available to a credential can be listed with `GET /c.json`.
- `scheme` (optional): The transport used to reach the account, `https` or `http`. Defaults to `https` (`http` is only accepted for loopback hosts).
- `page_size` (optional): Rows per request. Defaults to `1000`.
- `rate_limit` (optional): Requests per second. Defaults to `4`.
- `include_expensive` (optional): Whether to include properties Flexi flags as expensive to compute. Defaults to `true`.

### Step 2: Create an asset file for data ingestion

To ingest data from ABRA Flexi, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., abraflexi_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.abraflexi
type: ingestr

parameters:
  source_connection: abraflexi
  source_table: 'faktura-vydana'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For ABRA Flexi, it will always be `ingestr`.
- `source_connection`: The name of the ABRA Flexi connection defined in `.bruin.yml`.
- `source_table`: The **evidence path** in ABRA Flexi to ingest — the same identifier that appears in the REST URL. See the common evidences below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/abraflexi_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given ABRA Flexi evidence into your Postgres database.

## Available Source Tables

The table name is the **evidence path** — the same identifier that appears in the REST URL. Common ones:

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `faktura-vydana` / `faktura-vydana-polozka` | `id` | `lastUpdate` | merge | Issued invoices and their lines |
| `faktura-prijata` / `faktura-prijata-polozka` | `id` | `lastUpdate` | merge | Received invoices and their lines |
| `banka` / `banka-polozka` | `id` | `lastUpdate` | merge | Bank documents and lines |
| `pokladni-pohyb` | `id` | `lastUpdate` | merge | Cash movements |
| `interni-doklad` / `interni-doklad-polozka` | `id` | `lastUpdate` | merge | Internal documents and lines |
| `adresar` | `id` | `lastUpdate` | merge | Address book (counterparties) |
| `pohledavka` / `zavazek` | `id` | `lastUpdate` | merge | Receivables and liabilities |
| `ucetni-osnova` | `id` | `lastUpdate` | merge | Chart of accounts |
| `stredisko` | `id` | `lastUpdate` | merge | Cost centres |

`GET /c/<company>/evidence-list.json` lists every evidence in a company.

## Incremental behavior and limitations

Tables use `merge` on the primary key `id`, with `lastUpdate` as the incremental key pushed to Flexi as a server-side filter. Only the start bound is applied — re-running a wider window costs requests, never correctness, because `merge` deduplicates on `id`. Evidences that have an `id` but no usable `lastUpdate` are re-read in full on every run and still merge correctly.

- **Not every evidence is a table.** Some are derived views — the accounting journal `ucetni-denik`, the account-movement view `pohyb-na-uctech`, the VAT ledger `podklady-dph` and around a dozen report endpoints return rows whose `id` is `-1`. They cannot be deduplicated or windowed, so the connector refuses them at plan time with an explanatory error.
- **Some report endpoints require parameters** and return HTTP 400 when read as a plain table (`stav-skladu-k-datu`, `kontrolni-hlaseni-dph`, `souhrnne-hlaseni-dph`, and similar). These are not supported.
- **Relation and select fields expand into three columns** — for example `mena`, `mena_ref`, `mena_showAs`. Characters outside `[A-Za-z0-9_]` are replaced with `_`.
