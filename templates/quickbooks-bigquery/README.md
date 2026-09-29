# QuickBooks to BigQuery

## At a glance

- Pulls your QuickBooks Online customers, invoices, payments, vendors, and chart of accounts into BigQuery
- Picks up edits on every run: records are loaded by their last-updated time and merged on `id`
- Pins every column's type, so optional QuickBooks fields don't disappear from the destination table
- Documents every column and checks keys, amounts, and account types after each load
- Safe to re-run and easy to schedule daily
- **Lays the raw accounting foundation for finance reporting and AI agents**

`quickbooks-bigquery` is a focused QuickBooks Online ingestion pipeline for BigQuery. It loads five QuickBooks Accounting API objects into the `quickbooks_raw` dataset with [ingestr](https://getbruin.com/docs/ingestr/supported-sources/quickbooks.html), and gives you a documented, checked starting point for your own finance models.

## Project structure

```text
quickbooks-bigquery/
├── pipeline.yml
├── README.md
└── assets/
    └── quickbooks_raw/
        ├── customers.asset.yml
        ├── invoices.asset.yml
        ├── payments.asset.yml
        ├── vendors.asset.yml
        └── accounts.asset.yml
```

## What it creates

| Schema | Asset | QuickBooks object | Materialization | Purpose |
| --- | --- | --- | --- | --- |
| `quickbooks_raw` | `customers` | `Customer` | `merge` on `id` | Customers and sub-customers with contact, terms, and open balance |
| `quickbooks_raw` | `invoices` | `Invoice` | `merge` on `id` | Sales invoices with line items, due dates, and open balance |
| `quickbooks_raw` | `payments` | `Payment` | `merge` on `id` | Customer payments received and the invoices they were applied to |
| `quickbooks_raw` | `vendors` | `Vendor` | `merge` on `id` | Suppliers, 1099 flags, payment terms, and open A/P balance |
| `quickbooks_raw` | `accounts` | `Account` | `merge` on `id` | The chart of accounts with type, sub-type, and current balance |

QuickBooks nests references and line items, for example `customer_ref`, `line`, and `bill_addr`, and the pipeline keeps them as BigQuery `JSON` columns. A `*_ref` column holds `{"value": "<QuickBooks id>", "name": "<display name>"}`, so `JSON_VALUE(customer_ref.value)` joins an invoice to `quickbooks_raw.customers.id`.

Every asset declares its full column schema with types, descriptions, and checks. `enforce_schema: true` passes those types to ingestr, and `metadata_push` in `pipeline.yml` publishes the descriptions to BigQuery. `bruin docs my-quickbooks-pipeline` builds a browsable documentation site from the same definitions.

## Configure connections

Initializing the template adds placeholder connections to the repository-level `.bruin.yml`. Keep the connection names `gcp-default` and `quickbooks-default`, or rename them consistently in both `.bruin.yml` and `pipeline.yml`.

```yaml
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          project_id: your-gcp-project-id
          location: your-gcp-region
          use_application_default_credentials: true
      quickbooks:
        - name: quickbooks-default
          company_id: ${QUICKBOOKS_COMPANY_ID}
          client_id: ${QUICKBOOKS_CLIENT_ID}
          client_secret: ${QUICKBOOKS_CLIENT_SECRET}
          refresh_token: ${QUICKBOOKS_REFRESH_TOKEN}
          environment: production
```

### BigQuery

Replace both placeholders:

- `project_id` — the Google Cloud project that owns the datasets this pipeline writes to, for example `my-company-finance`.
- `location` — the region or multi-region of those datasets, for example `US` or `EU`. See [BigQuery locations](https://cloud.google.com/bigquery/docs/locations).

The connection uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials), so authenticate once with `gcloud auth application-default login`. To use a service account, replace `use_application_default_credentials: true` with `service_account_file: /path/to/service-account.json`. The credentials need permission to create datasets and tables and to run queries.

### QuickBooks Online

The QuickBooks connection authenticates with an OAuth 2.0 app from the [Intuit Developer portal](https://developer.intuit.com/):

1. Create an app with the `com.intuit.quickbooks.accounting` scope and copy its **Client ID** and **Client Secret**. Use the development keys for a sandbox company and the production keys for your real books.
2. Open the [OAuth 2.0 Playground](https://developer.intuit.com/app/developer/playground), select your app and the accounting scope, and connect it to your company.
3. Copy the **Realm ID** (your `company_id`) and the **Refresh Token** from the playground.

Then export the values before running the pipeline:

```bash
export QUICKBOOKS_COMPANY_ID='9341450000000000'
export QUICKBOOKS_CLIENT_ID='AB...'
export QUICKBOOKS_CLIENT_SECRET='...'
export QUICKBOOKS_REFRESH_TOKEN='AB11...'
```

Set `environment: sandbox` when connecting to an Intuit sandbox company. Do not commit credentials.

Intuit rotates refresh tokens roughly once a day, and ingestr does not write the new token back to `.bruin.yml`. Once the token has rotated, runs fail with `invalid_grant` until you generate a new refresh token in the playground and update `QUICKBOOKS_REFRESH_TOKEN`.

## Run the pipeline

Initialize a project from the template:

```bash
bruin init quickbooks-bigquery my-quickbooks-pipeline
bruin validate --fast my-quickbooks-pipeline
```

For the first load, run with `--full-refresh` over a window that covers every record edit in your company, from before the QuickBooks Online company was created up to now. The window filters on each record's QuickBooks `LastUpdatedTime`, not its transaction date, so an early start date loads records that haven't changed in years. The API does the filtering, so the early start date costs nothing extra:

```bash
bruin run --full-refresh --start-date 2000-01-01 --end-date "$(date -u +%Y-%m-%dT%H:%M:%S)" \
  my-quickbooks-pipeline
```

The end time is the current UTC time, so today's edits are included. A date-only `--end-date` stops at midnight at the start of that day.

After the initial load, schedule a daily run without `--full-refresh`. By default a run covers the previous UTC day, fetches the records updated inside that window, and merges them on `id`, so re-running a window never duplicates rows:

```bash
bruin run my-quickbooks-pipeline
```

## Loading behavior and caveats

- **Edits are captured.** An invoice paid, edited, or voided after it was created is reloaded by the run whose window covers the change, because QuickBooks bumps its `LastUpdatedTime`.
- **Deletes are not.** A transaction deleted in QuickBooks stays in `quickbooks_raw` at its last loaded version. Run with `--full-refresh` periodically if your team deletes transactions instead of voiding them.
- **Inactive records.** The QuickBooks query API only returns active customers, vendors, and accounts. A record made inactive after it was loaded keeps its last loaded version, with `active = true`.
- **Money is in the transaction currency.** Amounts are stored as decimal numbers in the transaction currency, which `currency_ref` records. Multi-currency companies should not sum across currencies without an exchange-rate policy.
- **Expenses are not included yet.** Purchases (expenses, checks, card charges) and bills are not yet supported by ingestr's QuickBooks source, so spend and A/P transactions are out of scope for now.

## Customize it

Build your own staging and reporting models on top of the `quickbooks_raw` tables, for example MRR from invoice lines, AR aging from open invoice balances, or collections from payments linked to invoices. Keep the raw layer as the stable interface and review the column checks before relying on the data for financial reporting.
