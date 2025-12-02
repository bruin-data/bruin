# Primer
[Primer](https://primer.io/) is a unified payments infrastructure that enables businesses to accept, optimize, and manage payments globally.

Bruin supports Primer as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Primer into your data platform.

To set up a Primer connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need an `api_key`. For details on how to obtain these credentials, please refer to the [Primer API documentation](https://primer.io/docs/api).

Follow the steps below to set up Primer correctly as a data source and run ingestion.

### Step 1: Add a connection to the .bruin.yml file
In order to set up Primer connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      primer:
        - name: "my-primer"
          api_key: "your-api-key"
```
- `api_key`: API key used for authentication with the Primer API

### Step 2: Create an asset file for data ingestion
To ingest data from Primer, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., primer_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.primer_payments
type: ingestr

parameters:
  source_connection: my-primer
  source_table: 'payments'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For Primer, it will be always `ingestr`
- `source_connection`: The name of the Primer connection defined in `.bruin.yml`
- `source_table`: The name of the table in Primer to ingest. Available tables:

| Table    | PK  | Inc Key | Inc Strategy | Details |
|----------|-----|---------|--------------|---------|
| payments | id  | -       | merge        | Retrieves payment details from Primer |

- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/primer_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Primer table into your destination database.
