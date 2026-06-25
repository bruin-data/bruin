# Chargebee

[Chargebee](https://www.chargebee.com/) is a subscription billing and revenue management platform for recurring billing, invoicing, and payments.

Bruin supports Chargebee as a source for [Ingestr assets](/assets/ingestr).

## Configuration

### Step 1: Add a connection to .bruin.yml

```yaml
connections:
    chargebee:
        - name: 'my-chargebee'
          site: 'your-site'
          api_key: 'live_xxx'
```

- `site`: your Chargebee site name (the subdomain in `https://<site>.chargebee.com`).
- `api_key`: your Chargebee API key.

### Step 2: Create an asset file

```yaml
name: public.chargebee
type: ingestr

parameters:
  source_connection: my-chargebee
  source_table: 'subscriptions'
  destination: postgres
```

- `source_connection`: the Chargebee connection defined in `.bruin.yml`.
- `source_table`: the Chargebee table to ingest (see below).

## Available Source Tables

Chargebee source allows ingesting the following resources:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `customers` | id | updated_at | merge | Customer records, including billing details, contact info, and custom fields. |
| `subscriptions` | id | updated_at | merge | Recurring subscriptions, including status, billing cycle, and subscription items. |
| `invoices` | id | updated_at | merge | Invoices generated for subscriptions and one-off charges, including line items and amounts. |
| `transactions` | id | updated_at | merge | Payment, refund, and credit transactions linked to invoices. |
| `orders` | id | updated_at | merge | Orders generated for invoices, including fulfillment and shipping details. Requires Order Management enabled on the site. |
| `events` | id | occurred_at | merge | Activity events such as subscription and customer changes, useful for change-style ingestion. |
