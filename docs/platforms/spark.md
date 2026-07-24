# Apache Spark

Bruin supports Spark SQL through the [ADBC Spark driver](https://adbc-drivers.org/drivers/spark/). The driver can connect through Spark Connect, Livy, Thrift binary, or Thrift HTTP. Bruin installs the pinned driver with `dbc` the first time a Spark connection is used.

## Connection

Add a `spark` connection to an environment in `.bruin.yml`. The `uri` follows the ADBC Spark driver's `spark://` URI format:

```yaml
environments:
  default:
    connections:
      spark:
        - name: spark-default
          uri: "spark://user@localhost:15002?auth_type=none&api=connect"
          catalog: spark_catalog
```

The driver supports multiple APIs and authentication modes. For example, a token-authenticated Spark Connect endpoint can be configured in the URI (URL-encode reserved characters in the token):

```yaml
      spark:
        - name: spark-default
          uri: "spark://:your-token@example.com:443?api=connect&auth_type=token&tls=true"
```

Connection fields:

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Connection name used by pipelines and assets. |
| `uri` | yes | ADBC Spark URI, including the endpoint, API, and authentication settings. |
| `catalog` | no | Initial Spark catalog (`adbc.connection.catalog`). |
| `ingest_staging_area` | for seeds | S3 or S3-compatible URI where ADBC temporarily stages Parquet data during bulk ingestion. |
| `ingest_location` | no | Optional `LOCATION` for tables created by ADBC bulk ingestion. EMR deployments generally require an S3 path. |
| `options` | no | Additional ADBC Spark driver options as key/value pairs. `driver` and `uri` are managed by Bruin. |

The URI and additional options are treated as sensitive because they may contain credentials.

## Spark assets

Spark supports the regular SQL platform asset set: `spark.sql`, `spark.seed`, `spark.source`, `spark.sensor.query`, and `spark.sensor.table`. Asset names can be `table`, `schema.table`, or `catalog.schema.table`.

### `spark.sql`

Runs a Spark SQL script, with optional table or view materialization.

```bruin-sql
/* @bruin
name: analytics.daily_events
type: spark.sql
materialization:
    type: table
@bruin */

SELECT event_date, COUNT(*) AS event_count
FROM raw.events
GROUP BY event_date
```

Spark materialization supports views and all regular table strategies: `create+replace`, `append`, `delete+insert`, `truncate+insert`, `merge`, `time_interval`, `ddl`, `scd2_by_column`, and `scd2_by_time`.

The `merge` strategy supports primary keys, `update_on_merge`, `merge_sql`, and `incremental_predicate`. Merge and SCD2 targets must use a catalog and table provider that implements Spark's row-level `MERGE INTO`, including `WHEN NOT MATCHED BY SOURCE`. For example, Iceberg requires its Spark SQL extensions.

Spark applies `partition_by` when it creates a table, using a `PARTITIONED BY` clause. Expressions supported by the table provider can be used, such as `days(event_at)` or `bucket(16, customer_id)` with Iceberg. Bruin maps `cluster_by` to the Iceberg sort-order command `ALTER TABLE ... WRITE ORDERED BY`; the configured catalog must support that command. See Iceberg's [Spark DDL documentation](https://iceberg.apache.org/docs/latest/spark-ddl/) for the supported partition transforms and write-order syntax.

SCD2 tables default to `days(_valid_from)` partitioning and an `_is_current, <primary keys>` sort order. Explicit `partition_by` and `cluster_by` values override those defaults. Layout settings are applied during full refresh and `create+replace`, and they are also supported by the `ddl` strategy.

## Query annotations

Spark SQL asset statements, schema creation, quality checks, and query sensors support Bruin query annotations. Bruin prepends the annotation payload as a Spark SQL comment. Use `--query-annotations default` for the standard asset, pipeline, and query-type fields, or provide a JSON object with additional fields:

```bash
bruin run path/to/pipeline --query-annotations '{"environment":"prod","team":"data"}'
```

### `spark.seed`

Loads a local or HTTP(S) CSV file into Spark using ADBC bulk ingestion in replace mode.

```yaml
name: analytics.countries
type: spark.seed
parameters:
  path: countries.csv
columns:
  - name: country_id
    type: integer
  - name: country_name
    type: string
```

Bruin maps common Boolean, integer, floating-point, and date column declarations to Arrow types. Other seed columns are loaded as strings. Set `ingest_staging_area` to an S3 URI the Bruin process can write and Spark can read. Set `ingest_location` as well when the Spark backend requires an explicit table location.

### `spark.source`

Represents a Spark table managed outside Bruin, allowing column and custom checks without executing a main query.

```yaml
name: raw.external_events
type: spark.source
columns:
  - name: event_id
    type: string
    checks:
      - name: not_null
```

### `spark.sensor.query`

Waits until a Spark SQL query returns a positive result:

```yaml
name: upstream.events_ready
type: spark.sensor.query
parameters:
  query: "SELECT COUNT(*) FROM upstream.events WHERE event_date = '{{ end_date }}'"
  poke_interval: 30
  timeout: 2h
```

### `spark.sensor.table`

Waits until a Spark table exists:

```yaml
name: upstream.events_table
type: spark.sensor.table
parameters:
  table: spark_catalog.upstream.events
  poke_interval: 30
  timeout: 2h
```

## Driver notes

The ADBC Spark driver is dynamically installed and requires a CGO-enabled Bruin build. Backend capabilities differ: consult the [ADBC Spark driver documentation](https://adbc-drivers.org/drivers/spark/) for supported APIs, authentication methods, and backend-specific limitations.
