# 2Checkout (Verifone)

[2Checkout](https://www.2checkout.com/) (now part of Verifone) is a payment and subscription platform that handles online sales, recurring billing, and global payments for software and digital goods businesses.

Bruin supports 2Checkout as a source for [Ingestr assets](/assets/ingestr). You can ingest data from 2Checkout into your data platform.

To set up a 2Checkout connection, add a configuration item in the `.bruin.yml` file and in your asset file. You authenticate with your `merchant_code` and API `secret_key`, both found under **Integrations > Webhooks & API** in the 2Checkout Control Panel.

Follow these steps to set up 2Checkout and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  twocheckout:
    - name: "twocheckout"
      merchant_code: "your-merchant-code"
      secret_key: "your-secret-key"
```

- `merchant_code`: (Required) Your 2Checkout merchant code.
- `secret_key`: (Required) Your 2Checkout API secret key.
- `base_url`: (Optional) Override the default REST API host (`https://api.2checkout.com`).

::: tip
Authentication signs each request with an HMAC-SHA256 signature that embeds a UTC timestamp, so the host clock must be accurate — clock skew of more than a few minutes results in `401` errors.
:::

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `twocheckout_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.twocheckout
type: ingestr

parameters:
  source_connection: twocheckout
  source_table: 'orders'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for 2Checkout.
- `source_connection`: The 2Checkout connection name defined in `.bruin.yml`.
- `source_table`: Name of the 2Checkout table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Strategy | Details |
|-------|----|--------------|---------|
| orders | ref_no, status | merge | Orders with line items, payments, and refunds. Filtered server-side by the run's date interval. The key is `(ref_no, status)` so status transitions (e.g. `COMPLETE` → `REFUND`) are preserved as distinct rows rather than overwriting each other. |
| subscriptions | subscription_reference | merge | Recurring subscriptions, including status, billing period, and pricing. Filtered on `ModifiedAfter`, so it captures subscriptions that changed within the interval. |
| products | product_code | replace | Products in your catalog, including pricing and settings. Full snapshot each run. |
| promotions | code | replace | Promotions and their discount configuration. Full snapshot each run. |

`orders` and `subscriptions` support incremental date-range loads: the run's date interval — set with `--start-date` / `--end-date` (see [run](/commands/run)) — filters records to that window. `products` and `promotions` are full snapshots on every run.

::: warning
2Checkout only returns the first 200 pages of results per query (a hard 20,000-record ceiling). To load more than that from `orders` or `subscriptions`, narrow the run's date interval so each query stays under the ceiling. There is no dedicated customers table — customer data arrives embedded in `orders` and `subscriptions`.
:::

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/twocheckout_ingestion.yml
```

Running this command ingests data from 2Checkout into your Postgres database.
