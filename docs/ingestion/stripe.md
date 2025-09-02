# Stripe
[Stripe](https://stripe.com/) is a technology company that builds economic infrastructure for the internet, providing payment processing software and APIs for e-commerce websites and mobile applications.

Bruin supports Stripe as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Stripe into your data warehouse.

In order to set up Stripe connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/stripe#grab-credentials).

Follow the steps below to correctly set up Stripe as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Stripe, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    stripe:
        - name: 'my-stripe'
          api_key: 'test_123'
```
- `api_key`: the API key used for authentication with the Stripe API

### Step 2: Create an asset file for data ingestion

To ingest data from Stripe, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.stripe
type: ingestr
connection: postgres

parameters:
  source_connection: my-stripe
  source_table: 'event'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Stripe connection defined in .bruin.yml.
- `source_table`: The name of the data table in Stripe you want to ingest. For example, "events" would ingest data related to events. You can find the available source tables in 


   - `account`: Contains information about a Stripe account, including balances, payouts, and account settings.
    - `apple_pay_domain`: Represents Apple Pay domains registered with Stripe for processing Apple Pay payments.
    - `application_fee`: Records fees collected by platforms on payments processed through connected accounts.
    - `checkout_session`: Contains data about Checkout sessions created for payment processing workflows.
    - `coupon`: Stores data about discount codes or coupons that can be applied to invoices, subscriptions, or other charges.
    - `customer`: Holds information about customers, such as billing details, payment methods, and associated transactions.
    - `dispute`: Records payment disputes and chargebacks filed by customers or banks.
    - `payment_intent`: Represents payment intents tracking the lifecycle of payments from creation to completion.
    - `payment_link`: Contains information about payment links created for collecting payments.
    - `payment_method`: Stores payment method information such as cards, bank accounts, and other payment instruments.
    - `payment_method_domain`: Represents domains verified for payment method collection.
    - `payout`: Records payouts made from Stripe accounts to bank accounts or debit cards.
    - `plan`: Contains subscription plan information including pricing and billing intervals.
    - `price`: Contains pricing information for products, including currency, amount, and billing intervals.
    - `product`: Represents products that can be sold or subscribed to, including metadata and pricing information.
    - `promotion_code`: Stores data about promotion codes that customers can use to apply coupons.
    - `quote`: Contains quote information for customers, including line items and pricing.
    - `refund`: Records refunds issued for charges, including partial and full refunds.
    - `review`: Contains payment review information for payments flagged by Stripe Radar.
    - `setup_attempt`: Records attempts to set up payment methods for future payments.
    - `setup_intent`: Represents setup intents for collecting payment method information.
    - `shipping_rate`: Contains shipping rate information for orders and invoices.
    - `subscription`: Represents a customer's subscription to a recurring service, detailing billing cycles, plans, and status.
    - `subscription_item`: Contains individual items within a subscription, including quantities and pricing.
    - `subscription_schedule`: Represents scheduled changes to subscriptions over time.
    - `tax_code`: Contains tax code information for products and services.
    - `tax_id`: Stores tax ID information for customers and accounts.
    - `tax_rate`: Contains tax rate information applied to invoices and subscriptions.
    - `top_up`: Records top-ups made to Stripe accounts.
    - `transfer`: Records transfers between Stripe accounts.
    - `webhook_endpoint`: Contains webhook endpoint configurations for receiving event notifications.
    - `application_fee`: Records fees collected by platforms.
    - `balance_transaction`: Records transactions that affect the Stripe account balance, such as charges, refunds, and payouts.
    - `charge`: Returns a list of charges.
    - `credit_note`: Contains credit note information for refunds and adjustments.
    - `event`: Logs all events in the Stripe account, including customer actions, account updates, and system-generated events.
    - `invoice`: Represents invoices sent to customers, detailing line items, amounts, and payment status.
    - `invoice_item`: Contains individual line items that can be added to invoices.
    - `invoice_line_item`: Represents line items within invoices.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/stripe_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Stripe table into your Postgres database.



<img width="1088" alt="stripe" src="https://github.com/user-attachments/assets/7133763d-91cb-4882-bb82-02617024b5dc">
