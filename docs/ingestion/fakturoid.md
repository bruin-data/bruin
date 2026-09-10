# Fakturoid

[Fakturoid](https://www.fakturoid.cz/) is a Czech invoicing and accounting service for freelancers and small businesses. Bruin reads it through the [Fakturoid API v3](https://www.fakturoid.cz/api/v3).

Bruin supports Fakturoid as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest invoices and subjects into your data platform.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    fakturoid:
        - name: "fakturoid"
          client_id: "your_client_id"
          client_secret: "your_client_secret"
          slug: "your_account_slug"
          user_agent: "MyCompany (billing@mycompany.com)"
```

- `client_id` (required): OAuth client id from the Fakturoid account settings.
- `client_secret` (required): The matching client secret.
- `slug` (required): The account slug as it appears in the Fakturoid URL. One set of credentials can reach several accounts, so it is never defaulted — guessing would silently load another account's books.
- `user_agent` (required): Must carry a contact address, e.g. `MyCompany (billing@mycompany.com)`. Fakturoid rejects requests with a missing or generic `User-Agent` with a `403` on **every** endpoint (including the token endpoint), so it has no default. A bad `User-Agent` reads like an auth error but is not.
- `rate_limit` (optional): Requests per second. Defaults to `1.5` (~90/min).

Authentication is OAuth2 `client_credentials`: the client id and secret are sent as HTTP Basic credentials to obtain a bearer token valid for about two hours, refreshed lazily.

### Step 2: Create an asset file for data ingestion

Create a YAML file (e.g. fakturoid_ingestion.yml) inside the assets folder:

```yaml
name: public.invoices
type: ingestr

parameters:
  source_connection: fakturoid
  source_table: 'invoices'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Set to `ingestr`.
- `source_connection`: The name of the Fakturoid connection defined in `.bruin.yml`.
- `source_table`: One of the tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/fakturoid_ingestion.yml
```

## Available Source Tables

| Table | Primary Key | Incremental Strategy | Details |
| ----- | ----------- | -------------------- | ------- |
| `invoices` | `id` | merge | Invoices |
| `invoices_lines` | `invoice_id`, `id` | merge | Invoice line items, exploded from each invoice |
| `invoices_vat_rates` | `invoice_id`, `vat_rate` | merge | Per-invoice VAT-rate summaries |
| `subjects` | `id` | merge | Customers and suppliers |

`invoices_lines` and `invoices_vat_rates` are derived from the same `/invoices.json` payload as `invoices`, so requesting them costs a full re-page of the invoice list.

## Incremental behavior and limitations

`invoices` and `subjects` use `updated_at` as the incremental key, applied server-side via `updated_since`; the destination still performs the merge. Every field Fakturoid returns is passed through and typed by schema inference; nested objects and arrays land as JSON columns.

- **Pagination is fixed at 40 rows and there is no total count** — the only end-of-data signal is a short page.
- **`merge` cannot observe deletions.** A removed line, invoice, or subject lingers in the destination — use a periodic full reload if deletions matter.
