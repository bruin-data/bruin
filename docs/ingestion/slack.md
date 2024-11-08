# slack
[slack](https://slack.com/) is a messaging platform for teams and organizations where they can collaborate, share ideas and information.

ingestr supports slack as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from slack into your data warehouse.

In order to have set up slack connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the slack section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

Follow the steps below to correctly set up slack as a data source and run ingestion:

**Step 1: Create an Asset File for Data Ingestion**

To ingest data from slack, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination.
(For e.g., ingestr.slack.asset.yml) and add the following content:

***File: ingestr.slack.asset.yml***
```yaml
name: public.slack
type: ingestr
connection: postgres

parameters:
  source_connection: slack
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.

- type: Specifies the type of the asset. It will be always ingestr type for slack.

- connection: This is the destination connection.

**parameters:**
- source_connection: The name of the slack connection defined in .bruin.yml.
- source_table: The name of the data table in slack you want to ingest.
  Step 2: Add a Connection to [.bruin.yml](https://bruin-data.github.io/bruin/connections/overview.html) that stores connections and secrets to be used in pipelines.
  You need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

***File: .bruin.yml***
```yaml
    connections:
      slack:
        - name: "connection_name"
          api_key: "YOUR_SLACK_API_KEY"
          
```
**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html] Asset) to Ingest Data**
```
bruin run ingestr.slack.asset.yml
```
It will ingest slack data to postgres.