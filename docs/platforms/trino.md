# Trino
Bruin supports Trino as a distributed SQL query engine.

## Connection
In order to set up a Trino connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

```yaml
    connections:
      trino:
        - name: "connection_name"
          username: "trino_user"
          password: "XXXXXXXXXX"  # Optional  
          host: "trino-coordinator.example.com"
          port: 8080
          catalog: "default" # Optional 
          schema: "schema_name" # Optional 
```

## Trino Assets

### `trino.sql`
Runs a materialized Trino asset or a Trino script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page. For information about materialization strategies, see the [Materialization](../assets/materialization.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: hive.events.install
type: trino.sql
materialization:
    type: table
@bruin */

SELECT user_id, event_name, ts
FROM hive.analytics.events
WHERE event_name = 'install'
```

#### Example: Run a Trino script
```bruin-sql
/* @bruin
name: hive.events.install
type: trino.sql
@bruin */

CREATE TABLE IF NOT EXISTS hive.events.install AS
SELECT user_id, event_name, ts
FROM hive.analytics.events
WHERE event_name = 'install'
```

### `trino.sensor.query`

Checks if a query returns any results in Trino, runs every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
```

**Parameters:**
- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Example: Partitioned upstream table
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: trino.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = '{{ end_date }}')
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: trino.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > '{{ end_timestamp }}')
```


## Lakehouse Support
Trino lakehouse integration is configured in Trino itself (catalog/connector settings). The Bruin Trino connection format stays the same.

### Supported Lakehouse Formats
#### Iceberg Format
| Catalog \ Storage | S3 |
|-------------------|----|
| Glue | <span class="lh-check" aria-label="supported"></span> |
| Nessie | <span class="lh-check" aria-label="supported"></span> |

### Prerequisites

- Docker and Docker Compose are installed.
- AWS credentials are available (exported) in your shell (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, `AWS_REGION`).
- You have an S3 warehouse prefix, for example `s3://example-lakehouse/warehouse`.

Local config file structure:

```text
└── trino
│    └── etc
│        ├── catalog
│        │   └── analytics_catalog.properties
│        ├── config.properties
│        ├── jvm.config
│        ├── log.properties
│        └── node.properties
└── docker-compose.yml
```

`trino/etc/node.properties`:

```properties
node.environment=dev
node.id=00000000-0000-0000-0000-000000000000
node.data-dir=/data/trino
```
- `node.environment` must be the same on all Trino nodes in a cluster.
- `node.id` must be unique per node and stable across restarts/upgrades.
- `node.data-dir` must be writable by Trino.

### Guide: Iceberg + Glue + S3 {#guide-glue-s3}

Use this when you want AWS Glue as the Iceberg catalog.
The Docker setup below is an example for local testing purposes.

`docker-compose.yml`:

```yaml
services:
  trino:
    image: trinodb/trino:latest
    ports:
      - "8080:8080"
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN}
      AWS_REGION: ${AWS_REGION}
    volumes:
      - ./trino/etc:/etc/trino
```

`trino/etc/catalog/analytics_catalog.properties`:

This file configures a Trino catalog that uses the Iceberg connector with AWS Glue as the metadata catalog and S3 as the warehouse location for table data.

```properties
connector.name=iceberg
iceberg.catalog.type=glue

hive.metastore.glue.region=us-east-1
hive.metastore.glue.default-warehouse-dir=s3://example-lakehouse/warehouse/
# Optional in some account setups:
# hive.metastore.glue.catalogid=<aws-account-id>

fs.native-s3.enabled=true
s3.region=us-east-1
```

### Guide: Iceberg + Nessie (In-Memory) + S3 {#guide-nessie-in-memory-s3}

Use this for local testing with ephemeral Nessie metadata.
The Docker setup below is an example for local testing and documentation purposes.

```yaml
services:
  nessie:
    image: ghcr.io/projectnessie/nessie:latest
    container_name: nessie
    ports:
      - "19120:19120"
    environment:
      NESSIE_VERSION_STORE_TYPE: IN_MEMORY
      QUARKUS_HTTP_PORT: 19120

  trino:
    image: trinodb/trino:latest
    container_name: trino
    ports:
      - "8080:8080"
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN}
      AWS_REGION: ${AWS_REGION}
    volumes:
      - ./trino/etc:/etc/trino
    depends_on:
      - nessie
```

`trino/etc/catalog/analytics_catalog.properties`:

This file tells Trino to use Iceberg tables with Nessie as the metadata catalog and S3 as the data warehouse. In practice: `iceberg.catalog.type=nessie` selects Nessie, `iceberg.nessie-catalog.uri` points to the Nessie API, `iceberg.nessie-catalog.ref` selects the active branch/ref, and `iceberg.nessie-catalog.default-warehouse-dir` sets where Iceberg data files are written in S3.

```properties
connector.name=iceberg

iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://nessie:19120/api/v1
iceberg.nessie-catalog.ref=main
iceberg.nessie-catalog.default-warehouse-dir=s3://example-lakehouse/warehouse

fs.native-s3.enabled=true
s3.region=us-east-1
```


`IN_MEMORY` does not persist Nessie metadata across restarts.

### Validate With Bruin

Bruin connection config stays unchanged; point to the Trino catalog and schema you configured.

`pipeline.yml`:

```yaml
name: trino-iceberg-smoke

default_connections:
  trino: trino_lakehouse
```

`.bruin.yml`:

```yaml
default_environment: default
environments:
  default:
    connections:
      trino:
        - name: trino_lakehouse
          host: localhost
          port: 8080
          username: trino_user
          catalog: analytics_catalog 
          # Trino catalog name from /etc/trino/catalog/analytics_catalog.properties (not Iceberg catalog type like glue/nessie)
          schema: analytics
```

`assets/smoke_test.sql`:

```sql
/* @bruin
name: analytics.smoke_test
type: trino.sql
@bruin */

CREATE TABLE IF NOT EXISTS sample_users (
  id BIGINT,
  name VARCHAR
);

INSERT INTO sample_users VALUES (1, 'alice'), (2, 'bob');

SELECT * FROM sample_users ORDER BY id;
```

Run:

```bash
bruin run my-pipeline
```

### Troubleshooting

- `Catalog analytics_catalog does not exist`: ensure `analytics_catalog.properties` is mounted under `/etc/trino/catalog/`.
- S3 permission errors: verify AWS credentials and region in container env.
- Glue errors: verify IAM permissions and `hive.metastore.glue.default-warehouse-dir`.
- Nessie errors: verify `http://nessie:19120/api/v1` and Nessie mode (`IN_MEMORY` vs persistent backend).
