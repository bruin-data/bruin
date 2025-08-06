# Wise
[Wise](https://www.Wise.com/) is a global financial technology company that provides international money transfers, multi-currency accounts, and business payment solutions.

Bruin supports Wise as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Wise into your data platform.

To set up a Wise connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/Wise#grab-api-token).

Follow the steps below to set up Wise correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up Wise connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      Wise:
        - name: "wise"
          api_key: "key-123"   
```
- `api_key`: key used for authentication with the Wise API

### Step 2: Create an asset file for data ingestion
To ingest data from Wise, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., wise_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.2ise
type: ingestr

parameters:
  source_connection: wise
  source_table: 'balances'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Wise, it will be always `ingestr`
- `source_connection`: The name of the Wise connection defined in `.bruin.yml`
- `source_table`: The name of the table in Wise to ingest. Available tables can be found [here](https://bruin-data.github.io/ingestr/supported-sources/wise.html#tables)
- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/wise_asset.yml
```
As a result of this command, Bruin will ingest data from the given wise table into your Postgres database.

********
<img alt="wise" src="./media/wise_ingestion.png">

