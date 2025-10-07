# Freshdesk
[Freshdesk](https://www.freshdesk.com/) is a cloud-based customer service platform that helps businesses manage customer support via multiple channels including email, phone, websites, and social media.

Bruin supports Freshdesk as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Freshdesk into your data warehouse.

In order to set up Freshdesk connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Freshdesk as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Freshdesk, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  freshdesk:
    - name: "my_freshdesk"
      domain: "my_company"
      api_key: "your_api_key"
```

- `domain`: The domain of your Freshdesk account, found in your account URL. For example, if your account URL is `https://my_company.freshdesk.com/`, then `my_company` is your domain.
- `api_key`: The API token used for authentication with Freshdesk.

**Setting up a Freshdesk integration**: Freshdesk requires a few steps to set up an integration, please follow the [dltHub setup guide](https://dlthub.com/docs/dlt-ecosystem/verified-sources/freshdesk).

### Step 2: Create an asset file for data ingestion

To ingest data from Freshdesk, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., freshdesk_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.freshdesk
type: ingestr
connection: postgres

parameters:
  source_connection: my_freshdesk
  source_table: 'contacts'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Freshdesk connection defined in `.bruin.yml`.
- `source_table`: The name of the data table in Freshdesk that you want to ingest. Available tables:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| agents | id | updated_at | merge | Retrieves users responsible for managing and resolving customer inquiries and support tickets |
| companies | id | updated_at | merge | Retrieves customer organizations or groups that agents support |
| contacts | id | updated_at | merge | Retrieves individuals or customers who reach out for support |
| groups | id | updated_at | merge | Retrieves agents organized based on specific criteria |
| roles | id | updated_at | merge | Retrieves predefined sets of permissions that determine what actions an agent can perform |
| tickets | id | updated_at | merge | Retrieves customer inquiries or issues submitted via various channels like email, chat, phone, etc. |
| tickets:\<query\> | id | updated_at | merge | Executes the Freshdesk ticket filter query while preserving incremental sync |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/freshdesk_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Freshdesk table into your Postgres database.
