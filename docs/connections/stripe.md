# Stripe
[Stripe](https://stripe.com/en-de) is a technology company that builds economic infrastructure for the internet, providing payment processing software and APIs for e-commerce websites and mobile applications.
ingestr supports Stripe as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Stripe into your data warehouse.

In order to have set up Stripe connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials, read [here](https://bruin-data.github.io/ingestr/supported-sources/stripe.html)

Follow the steps below to correctly set up Stripe as a data source and run ingestion.

**Step 1: Add a Connection to .bruin.yml**

To connect to Stripe, you need to add a configuration item to the connections section of the [.bruin.yml file](https://bruin-data.github.io/bruin/connections/overview.html). This configuration must comply with the following schema:

```yaml
connections:
  connections:
  stripe:
    - name: my-stripe
      api_key: rk_yui&8v8F4_SPgjeh7hsdf
```

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from Stripe, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) and add the following content:

```yaml
name: public.stripe
type: ingestr
connection: postgres

parameters:
  source_connection: my-stripe
  source_table: 'event'

  destination: postgres
```

**name**: The name of the asset.

**type**: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.

**connection:** This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.

**parameters:**
**source_connection:** The name of the Stripe connection defined in .bruin.yml.
**source_table**: The name of the data table in Stripe you want to ingest. For example, "events" would ingest data related to ads. [Available source tables in Stripe](https://bruin-data.github.io/ingestr/supported-sources/stripe.html#available-tables)


**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run --file stripe_ingestion.yml
```
It will ingest Stripe data to postgres. 