# Kafka
[Kafka](https://kafka.apache.org/) is a distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

ingestr supports Kafka as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from kafka into your data warehouse.

In order to have set up Kafka connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Kafka section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

Follow the steps below to correctly set up Kafka as a data source and run ingestion:

**Step 1: Create an Asset File for Data Ingestion**

To ingest data from Kafka, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination.
(For e.g., ingestr.kafka.asset.yml) and add the following content:

***File: ingestr.kafka.asset.yml***
```yaml
name: public.kafka
type: ingestr
connection: postgres

parameters:
  source_connection: kafka
  source_table: 'kafka.my_topic'
  destination: postgres
```

- name: The name of the asset.

- type: Specifies the type of the asset. It will be always ingestr type for Kafka.

- connection: This is the destination connection.

**parameters:**
- source_connection: The name of the Kafka connection defined in .bruin.yml.
- source_table: The name of the data table in kafka you want to ingest.
Step 2: Add a Connection to [.bruin.yml](https://bruin-data.github.io/bruin/connections/overview.html) that stores connections and secrets to be used in pipelines.
You need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

***File: .bruin.yml***
```yaml
    connections:
      kafka:
        - name: "connection_name"
          bootstrap_servers: "localhost:9093"
          group_id: "test123"
```
**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html] Asset) to Ingest Data**
```
bruin run ingestr.kafka.asset.yml
```
It will ingest kafka data to postgres.