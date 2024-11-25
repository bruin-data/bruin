# Zendesk
[Zendesk](https://www.zendesk.com/) is a cloud-based customer service and support platform. It offers a range of features including ticket management, self-service options, knowledgebase management, live chat, customer analytics, and conversations.

Bruin supports Zendesk as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Zendesk into your data warehouse.

In order to set up Zendesk connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in `asset` file. Depending on the data you are ingesting (source_table), you will need to use either `API Token authentication` or `OAuth Token authentication`. Choose the appropriate method based on your source table. For more details, please refer to the [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/zendesk.html)

Follow the steps below to correctly set up zendesk as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to Zendesk, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

API Token Authentication:
```yaml
      connections:
          zendesk:
            - name: "my_zendesk",
              api_token: "xyzKey",
              email: "example.zendesk@gmail.com",
              sub_domain: "myCompany",
```

OAuth Token Authentication:
```yaml
  connections:
        zendesk:
          - name: "my_zendesk",
            oauth_token: "abcToken",
            sub_domain: "myCompany",
```

- `sub_domain`: the unique Zendesk subdomain that can be found in the account URL. For example, if your account URL is https://my_company.zendesk.com/, then `my_company` is your subdomain
- `email`: the email address of the user
- `api_token`: the API token used for authentication with Zendesk
- `oauth_token`: the OAuth token used for authentication with Zendesk

### Step 2: Create an asset file for data ingestion
To ingest data from zendesk, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., zendesk_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.zendesk
type: ingestr
connection: postgres

parameters:
  source_connection: my_zendesk
  source_table: 'brands'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the zendesk connection defined in .bruin.yml.
- `source_table`: The name of the data table in zendesk that you want to ingest. You can find the available source tables in Zendesk [here](https://bruin-data.github.io/ingestr/supported-sources/zendesk.html#tables)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/zendesk_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given zendesk table into your Postgres database.

<img width="1082" alt="zendesk" src="https://github.com/user-attachments/assets/b4cb54eb-dc05-4b6e-a113-e07316be9bff">
