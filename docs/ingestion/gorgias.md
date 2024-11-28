# Gorgias

[Gorgias](https://gorgias.com) is a helpdesk for e-commerce merchants, providing customer service via email, social media, SMS, and live chat.

Bruin supports Gorgias as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Gorgias into your data warehouse.

In order to set up Gorgias connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Gorgias as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Gorgias, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      gorgias:
        - name: "my-gorgias"
          domain: "my-shop"
          email: "myemail@domain.com"
          api_key: "abc123"
```
- `domain`: the domain of the Gorgias account without the full gorgias.com, e.g. mycompany
- `api_key`: the integration token used for authentication with the Gorgias API
- `email`: the email address of the user to connect to the Gorgias API

### Step 2: Create an asset file for data ingestion

To ingest data from Gorgias, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gorgias_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gorgias
type: ingestr
connection: postgres

parameters:
  source_connection: my-gorgias
  source_table: 'customers'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Gorgias connection defined in .bruin.yml.
- `source_table`: The name of the data table in Gorgias that you want to ingest. For example, `customers` is the table of Gorgias that you want to ingest. You can find the available source tables in Gorgias [here](https://bruin-data.github.io/ingestr/supported-sources/gorgias.html#supported-entities)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gorgias_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Gorgias table into your Postgres database.