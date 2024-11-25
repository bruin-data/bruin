# Slack
[slack](https://slack.com/) is a messaging platform for teams and organizations where they can collaborate, share ideas and information.

Bruin supports Slack as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Slack into your data warehouse.

In order to set up Slack connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/slack#setup-guide).

Follow the steps below to correctly set up slack as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Slack, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      slack:
        - name: "my-slack"
          api_key: "YOUR_SLACK_API_KEY"
          
```

### Step 2: Create an asset file for data ingestion

To ingest data from Slack, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., slack_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.slack
type: ingestr
connection: postgres

parameters:
  source_connection: my-slack
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for slack.
- connection: This is the destination connection.
- source_connection: The name of the slack connection defined in .bruin.yml.
- source_table: The name of the data table in slack you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/slack_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Slack table into your Postgres database.