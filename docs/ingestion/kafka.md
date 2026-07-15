# Kafka

[Kafka](https://kafka.apache.org/) is a distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

Bruin supports Kafka as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Kafka into your data warehouse.

In order to set up a Kafka connection, you need to add a configuration item to connections in the .bruin.yml file, following the schema below. The required fields include `bootstrap_servers` and `group_id`. The SASL-related fields (`security_protocol`, `sasl_mechanisms`, `sasl_username`, `sasl_password`) as well as `batch_size` and `batch_timeout` are optional, depending on your Kafka setup. For more information on Kafka, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/kafka.html)

Follow the steps below to correctly set up Kafka as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Kafka, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      kafka:
        - name: "my_kafka"
          bootstrap_servers: "localhost:9093"
          group_id: "test123"
```

- `bootstrap_servers`: The Kafka server or servers to connect to, typically in the form of a host and port.
- `group_id` : The consumer group ID used for identifying the client when consuming messages.

### Step 2: Create an asset file for data ingestion

To ingest data from Kafka, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., kafka_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.kafka
type: ingestr
connection: postgres

parameters:
  source_connection: my_kafka
  source_table: 'kafka.my_topic'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Kafka.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Kafka connection defined in .bruin.yml.
- `source_table`: The name of the data table in kafka you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/kafka_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Kafka table into your Postgres database. By default the asset consumes the currently available messages and exits, which fits a scheduled run.

## Continuous (streaming) ingestion

Set `stream: true` to consume the topic **continuously** instead of exiting once the current backlog is drained:

```yaml
name: public.kafka
type: ingestr
connection: postgres

parameters:
  source_connection: my_kafka
  source_table: 'kafka.my_topic'
  destination: postgres
  stream: true
  flush_interval: 30s      # optional: how often buffered records are written
  flush_records: 10000     # optional: buffered-record count that triggers a write
```

A streaming asset never finishes on its own, so it is excluded from a normal `bruin run` (which expects every asset to complete) and is launched on its own:

```bash
bruin run --stream assets/kafka_ingestion.yml
```

The stream runs in the foreground until you stop it with `Ctrl+C`, then flushes buffered records and exits cleanly. A normal `bruin run <pipeline>` skips streaming assets and prints a notice. See [Streaming assets](/assets/ingestr#streaming-assets) for the full behaviour and restrictions, which apply to message-broker streams as well.
