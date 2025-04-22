# Salesforce
[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform.

To set up a Salesforce connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `username`, `password` and `token`. You can obtain your security token by logging into your Salesforce account and navigating to the user settings under "Reset My Security Token."

Follow the steps below to set up Salesforce correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
      salesforce:
            - name: "salesforce"
              username: "user_123"
              password: "pass_123"
              token: "token_123"
```
- `username` is your Salesforce account username.
- `password` is your Salesforce account password.
- `token` is your Salesforce security token.

### Step 2: Create an asset file for data ingestion
To ingest data from Salesforce, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., salesforce_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.salesforce
type: ingestr
connection: postgres

parameters:
  source_connection: salesforce
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Salesforce, it will be always `ingestr`.
- `source_connection`: The name of the Salesforce connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Salesforce to ingest. You can find the available source tables [here](https://bruin-data.github.io/ingestr/supported-sources/salesforce.html#tables).
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/salesforce_asset.yml
```
As a result of this command, Bruin will ingest data from the given salesforce table into your Postgres database.
