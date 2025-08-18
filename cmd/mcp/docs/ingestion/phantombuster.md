# PhantomBuster
[PhantomBuster](https://phantombuster.com/) is a cloud-based data automation and web scraping platform that allows users to extract data from websites, automate actions.

Bruin supports PhantomBuster as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from PhantomBuster into your data warehouse.

In order to set up PhantomBuster connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up PhantomBuster as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to PhantomBuster, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  PhantomBuster:
    - name: "phantombuster"
      api_key: "key_123"
```
- `api_key`: the API key used for authentication with the PhantomBuster API

### Step 2: Create an asset file for data ingestion
To ingest data from PhantomBuster, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., PhantomBuster_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.phantombuster
type: ingestr
connection: postgres

parameters:
  source_connection: phantombuster
  source_table: 'completed_phantoms:<agent_id>'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the phantombuster connection defined in .bruin.yml.
- `source_table`: The name of the data table in PhantomBuster that you want to ingest.
  For now, we only support `completed_phantoms` tables followed by an agent ID. For example:
  `completed_phantoms:<agentid>`. An Agent ID is a unique identifier for a specific Phantom.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/phantombuster_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given PhantomBuster table into your Postgres database.

