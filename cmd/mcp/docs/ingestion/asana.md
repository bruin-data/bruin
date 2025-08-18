# Asana
[Asana](https://asana.com/) is a software-as-a-service platform designed for team collaboration and work management. Teams can create projects, assign tasks, set deadlines, and communicate directly within Asana. It also includes reporting tools, file attachments, calendars, and goal tracking.


Bruin supports Asana as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Asana into your data warehouse.

In order to set up Asana connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up Asana as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Asana, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      asana:
        - name: "my-asana"
          workspace: "YOUR_WORKSPACE_ID"
          access_token: "YOUR_ACCESS_TOKEN"
          
```

* `workspace`: is the `gid` of your workspace.
* `access_token`: is a personal access token.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/asana.html#uri-format).

### Step 2: Create an asset file for data ingestion

To ingest data from Asana, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., asana_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.asana
type: ingestr
connection: postgres

parameters:
  source_connection: my-asana
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Asana.
- connection: This is the destination connection.
- source_connection: The name of the Asana connection defined in .bruin.yml.
- source_table: The name of the data table in Asana you want to ingest. 

You can find a list of supported tables [here](https://bruin-data.github.io/ingestr/supported-sources/asana.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/asana_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given Asana table into your Postgres database.