# FastSpring

[FastSpring](https://fastspring.com/) is a merchant of record and e-commerce platform that handles payments, subscriptions, taxes, and invoicing for software and SaaS businesses.

Bruin supports FastSpring as a source for [Ingestr assets](/assets/ingestr). You can ingest data from FastSpring into your data platform.

To set up a FastSpring connection, add a configuration item in the `.bruin.yml` file and in your asset file. You authenticate with your API `username` and `password`, created under **Developer Tools > APIs > API Credentials** in the FastSpring app.

Follow these steps to set up FastSpring and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  fastspring:
    - name: "fastspring"
      username: "your-api-username"
      password: "your-api-password"
```

- `username`: (Required) FastSpring API username.
- `password`: (Required) FastSpring API password.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `fastspring_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.fastspring
type: ingestr

parameters:
  source_connection: fastspring
  source_table: 'orders'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for FastSpring.
- `source_connection`: The FastSpring connection name defined in `.bruin.yml`.
- `source_table`: Name of the FastSpring table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| orders | id | changed | merge | Orders and their line items, payments, taxes, and returns. Supports date-range filtering. |
| subscriptions | id | changed | merge | Recurring subscriptions, including status, billing period, and pricing. Supports date-range filtering. |
| accounts | id | | replace | Customer accounts, including contact details and address. |
| products | id | | replace | Products in your catalog, including pricing and fulfillment settings. |
| coupons | id | | replace | Coupons and their discount configuration. |
| subscription_report | subscription_id, transaction_date | sync_date | merge | Subscription metrics (MRR, ARR, subscribers, churn) grouped by the fields you choose. |
| revenue_report | Order_ID, Transaction_Date | syncDate | merge | Revenue metrics grouped by the fields you choose. |

`orders` and `subscriptions` support incremental date-range loads via `--interval-start` / `--interval-end`. Reports load incrementally on their sync-date column; customize a report's columns and grouping with the colon form `<report>:<columns>:<group_by>`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/fastspring_ingestion.yml
```

Running this command ingests data from FastSpring into your Postgres database.
