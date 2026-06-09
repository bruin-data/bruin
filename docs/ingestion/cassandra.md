# Cassandra

[Apache Cassandra](https://cassandra.apache.org/) is a distributed wide-column database designed for high availability and large-scale workloads.

Bruin supports Cassandra as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Cassandra into your data warehouse.

In order to set up Cassandra as a source, you need to add a connection in the `.bruin.yml` file and reference it in an asset file.

Follow the steps below to correctly set up Cassandra as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Cassandra, add a configuration item to the `connections` section of the `.bruin.yml` file:

```yaml
connections:
  cassandra:
    - name: "my-cassandra"
      username: "cassandra"
      password: "cassandra"
      host: "localhost"
      port: 9042
      keyspace: "analytics"
      consistency: "local_quorum"
      page_size: 1000
      timeout: "10s"
      connect_timeout: "5s"
      ssl: false
      disable_initial_host_lookup: true
```

- `name`: The name to identify this Cassandra connection.
- `username`: (Optional) The Cassandra username.
- `password`: (Optional) The password for the specified username.
- `host`: The Cassandra seed host.
- `hosts`: (Optional) Additional seed hosts. Bruin passes these to ingestr as the Cassandra `hosts` query parameter.
- `port`: (Optional) The Cassandra native transport port. Defaults to `9042`.
- `keyspace`: (Optional) The default Cassandra keyspace.
- `consistency`: (Optional) Cassandra consistency level, such as `one`, `quorum`, `local_quorum`, or `all`.
- `page_size`: (Optional) Source read page size.
- `timeout`: (Optional) Request timeout as a Go duration string, such as `10s`.
- `connect_timeout`: (Optional) Connection timeout as a Go duration string, such as `5s`.
- `ssl`: (Optional) Set to `true` to enable TLS.
- `disable_initial_host_lookup`: (Optional) Set to `true` for single-node Docker or NAT setups.

For multiple Cassandra seed hosts:

```yaml
connections:
  cassandra:
    - name: "cassandra-cluster"
      username: "cassandra"
      password: "cassandra"
      host: "cass-1.example.com"
      hosts:
        - "cass-1.example.com"
        - "cass-2.example.com"
        - "cass-3.example.com"
      keyspace: "analytics"
```

### Step 2: Create an asset file for data ingestion

To ingest data from Cassandra, create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `cassandra_ingestion.yml`) inside the assets folder and add the following content:

```yaml
name: public.cassandra_events
type: ingestr
connection: postgres

parameters:
  source_connection: my-cassandra
  source_table: "events"

  destination: postgres
```

If the connection does not include a keyspace, use a fully qualified table name:

```yaml
name: public.cassandra_events
type: ingestr
connection: postgres

parameters:
  source_connection: my-cassandra
  source_table: "analytics.events"

  destination: postgres
```

Custom CQL queries are also supported:

```yaml
name: public.cassandra_events
type: ingestr
connection: postgres

parameters:
  source_connection: my-cassandra
  source_table: "query:SELECT id, event_type, created_at FROM analytics.events WHERE created_at >= :interval_start ALLOW FILTERING"

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` for Cassandra.
- `connection`: This is the destination connection, which defines where the data should be stored. For example, `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Cassandra connection defined in `.bruin.yml`.
- `source_table`: The Cassandra source table. Use a plain table name when the connection includes a keyspace, a fully qualified `keyspace.table` name, or a `query:` CQL query.
- `destination`: The destination platform for the ingestr asset.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/cassandra_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Cassandra table into your destination.
