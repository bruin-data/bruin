# Klaviyo
[Klaviyo](https://www.Klaviyo.com/) is a marketing automation platform that helps businesses build and manage digital relationships with their customers by connecting through personalized email and enhancing customer loyality.

ingestr supports Klaviyo as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Klaviyo into your data warehouse.

To set up a Klaviyo connection, you need to have Klaviyo API key and source table. For more information, read [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html)

Follow the steps below to correctly set up Klaviyo as a data source and run ingestion:

**Step 1: Create an Asset File for Data Ingestion**

To ingest data from Klaviyo, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination.
(For e.g., ingestr.klaviyo.asset.yml) and add the following content:

***File: ingestr.klaviyo.asset.yml***
```yaml
name: public.klaviyo
type: ingestr
connection: postgres

parameters:
  source_connection: klaviyo
  source_table: 'events'
  destination: postgres
```

- name: The name of the asset.

- type: Specifies the type of the asset. It will be always ingestr type for Klaviyo.

- connection: This is the destination connection. 

**parameters:**
- source_connection: The name of the Klaviyo connection defined in .bruin.yml.
- source_table: The name of the data table in klaviyo you want to ingest. For example, "events" would ingest data related to events.
[Available source tables in Klaviyo](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html#available-tables)

Step 2: Add a Connection to [.bruin.yml](https://bruin-data.github.io/bruin/connections/overview.html) that stores connections and secrets to be used in pipelines.
You need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

***File: .bruin.yml***
```yaml
    connections:
      klaviyo:
        - name: "connection_name"
          api_key: "YOUR_Klaviyo_API_KEY"
```
**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run ingestr.klaviyo.asset.yml
```
It will ingest klaviyo data to postgres.