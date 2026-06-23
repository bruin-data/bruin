# Paddle

[Paddle](https://www.paddle.com/) is a merchant-of-record billing platform for payments, subscriptions, and invoicing.

Bruin supports Paddle as a source for [Ingestr assets](/assets/ingestr).

## Configuration

### Step 1: Add a connection to .bruin.yml

```yaml
connections:
    paddle:
        - name: 'my-paddle'
          api_key: 'pdl_live_xxx'
```

- `api_key`: your Paddle API key. 

### Step 2: Create an asset file

```yaml
name: public.paddle
type: ingestr

parameters:
  source_connection: my-paddle
  source_table: 'transactions'
  destination: postgres
```

- `source_connection`: the Paddle connection defined in `.bruin.yml`.
- `source_table`: the Paddle table to ingest (see below).

## Available Source Tables

Paddle source allows ingesting the following resources:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `customers` | id | updated_at | merge | Customer records, including name, email, and locale. |
| `products` | id | updated_at | merge | Products you sell, including name, description, and tax category. |
| `prices` | id | updated_at | merge | Prices attached to products, including billing cycle and currency. |
| `discounts` | id | updated_at | merge | Discounts and coupon codes that can be applied to transactions and subscriptions. |
| `transactions` | id | updated_at | merge | Transactions, the core billing record. Each transaction carries the `invoice_number` Paddle generates, so this table is where invoice data lives. |
| `subscriptions` | id | updated_at | merge | Recurring subscriptions, including status, billing cycle, and scheduled changes. |
| `adjustments` | id | updated_at | merge | Adjustments such as refunds, credits, and chargebacks against transactions. |