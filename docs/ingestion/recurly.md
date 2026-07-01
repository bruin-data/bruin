# Recurly

[Recurly](https://recurly.com/) is a subscription management and recurring billing platform for subscriptions, invoicing, payments, and revenue recognition.

Bruin supports Recurly as a source for [Ingestr assets](/assets/ingestr).

## Configuration

### Step 1: Add a connection to .bruin.yml

```yaml
connections:
    recurly:
        - name: 'my-recurly'
          api_key: 'your-private-api-key'
          region: 'us'
```

- `api_key`: your Recurly private API key.
- `region` (optional): the data center region, either `us` (default) or `eu`.

### Step 2: Create an asset file

```yaml
name: public.recurly
type: ingestr

parameters:
  source_connection: my-recurly
  source_table: 'subscriptions'
  destination: postgres
```

- `source_connection`: the Recurly connection defined in `.bruin.yml`.
- `source_table`: the Recurly table to ingest (see below).

## Available Source Tables

Recurly source allows ingesting the following resources:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `accounts` | id | updated_at | merge | Customer accounts, including billing details, contact info, and custom fields. |
| `subscriptions` | id | updated_at | merge | Recurring subscriptions, including status, billing cycle, and subscription add-ons. |
| `invoices` | id | updated_at | merge | Invoices generated for subscriptions and one-off charges, including line items, taxes, and discounts. |
| `transactions` | id | updated_at | merge | Payment, refund, and verification transactions linked to invoices. |
| `plans` | id | updated_at | merge | Subscription plans, including pricing, billing intervals, and add-ons. |
