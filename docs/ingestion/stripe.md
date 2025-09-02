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
- `source_table`: The name of the data table in Stripe you want to ingest. Available tables:

Table    PK    Inc Key    Inc Strategy    Details
account    id    created    merge    Contains information about a Stripe account, including balances, payouts, and account settings.
apple_pay_domain    id    created    merge    Represents Apple Pay domains registered with Stripe for processing Apple Pay payments.
application_fee    id    created    merge    Records fees collected by platforms on payments processed through connected accounts.
balance_transaction    id    created    merge    Records transactions that affect the Stripe account balance, such as charges, refunds, and payouts.
charge    id    created    merge    Returns a list of charges.
checkout_session    id    created    merge    Contains data about Checkout sessions created for payment processing workflows.
coupon    id    created    merge    Stores data about discount codes or coupons that can be applied to invoices, subscriptions, or other charges.
credit_note    id    created    merge    Contains credit note information for refunds and adjustments.
customer    id    created    merge    Holds information about customers, such as billing details, payment methods, and associated transactions.
dispute    id    created    merge    Records payment disputes and chargebacks filed by customers or banks.
event    id    created    merge    Logs all events in the Stripe account, including customer actions, account updates, and system-generated events.
invoice    id    created    merge    Represents invoices sent to customers, detailing line items, amounts, and payment status.
invoice_item    id    created    merge    Contains individual line items that can be added to invoices.
invoice_line_item    id    created    merge    Represents line items within invoices.
payment_intent    id    created    merge    Represents payment intents tracking the lifecycle of payments from creation to completion.
payment_link    id    created    merge    Contains information about payment links created for collecting payments.
payment_method    id    created    merge    Stores payment method information such as cards, bank accounts, and other payment instruments.
payment_method_domain    id    created    merge    Represents domains verified for payment method collection.
payout    id    created    merge    Records payouts made from Stripe accounts to bank accounts or debit cards.
plan    id    created    merge    Contains subscription plan information including pricing and billing intervals.
price    id    created    merge    Contains pricing information for products, including currency, amount, and billing intervals.
product    id    created    merge    Represents products that can be sold or subscribed to, including metadata and pricing information.
promotion_code    id    created    merge    Stores data about promotion codes that customers can use to apply coupons.
quote    id    created    merge    Contains quote information for customers, including line items and pricing.
refund    id    created    merge    Records refunds issued for charges, including partial and full refunds.
review    id    created    merge    Contains payment review information for payments flagged by Stripe Radar.
setup_attempt    id    created    merge    Records attempts to set up payment methods for future payments.
setup_intent    id    created    merge    Represents setup intents for collecting payment method information.
shipping_rate    id    created    merge    Contains shipping rate information for orders and invoices.
subscription    id    created    merge    Represents a customer's subscription to a recurring service, detailing billing cycles, plans, and status.
subscription_item    id    created    merge    Contains individual items within a subscription, including quantities and pricing.
subscription_schedule    id    created    merge    Represents scheduled changes to subscriptions over time.
tax_code    id    created    merge    Contains tax code information for products and services.
tax_id    id    created    merge    Stores tax ID information for customers and accounts.
tax_rate    id    created    merge    Contains tax rate information applied to invoices and subscriptions.
top_up    id    created    merge    Records top-ups made to Stripe accounts.
transfer    id    created    merge    Records transfers between Stripe accounts.
webhook_endpoint    id    created    merge    Contains webhook endpoint configurations for receiving event notifications.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/stripe_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Stripe table into your Postgres database.



<img width="1088" alt="stripe" src="https://github.com/user-attachments/assets/7133763d-91cb-4882-bb82-02617024b5dc">
