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
- `source_table`: The name of the data table in Stripe you want to ingest. For example, "events" would ingest data related to events. You can find the available source tables in Stripe [here](https://bruin-data.github.io/ingestr/supported-sources/stripe.html#tables)


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/stripe_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Stripe table into your Postgres database.



<img width="1088" alt="stripe" src="https://github.com/user-attachments/assets/7133763d-91cb-4882-bb82-02617024b5dc">
