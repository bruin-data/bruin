# .bruin.yml Reference

The `.bruin.yml` file is the central configuration file for Bruin pipelines. It stores all credentials, connection details, and environment configurations needed to run your data pipelines. The file is automatically created when you run any `bruin` command the first time in a project, and it is automatically added to `.gitignore`.

`.bruin.yml` file is expected to be in the root of your Git repo. You can specify a different location using the `--config-file /path/to/.bruin.yml`

## File Structure

The file is a YAML file with the following structure:

```yaml
default_environment: <environment_name>
environments:
  <environment_name>:
    schema_prefix: <optional_prefix>
    connections:
      <connection_type>:
        - name: "<connection_name>"
          # connection-specific fields...
```

### Example

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
```

## Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `default_environment` | string | No | Environment to use when none is specified. Defaults to `default`. |
| `environments` | map | Yes | Map of environment names to their configurations. |

## Environment Configuration

Each environment contains:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connections` | object | Yes | Connection definitions grouped by type. |
| `schema_prefix` | string | No | Prefix added to schema names (useful for dev/staging environments). |

## Environment Variables

You can reference environment variables in your configuration using `${VAR_NAME}` syntax:

```yaml
environments:
  default:
    connections:
      postgres:
        - name: my_postgres
          username: ${POSTGRES_USERNAME}
          password: ${POSTGRES_PASSWORD}
          host: ${POSTGRES_HOST}
          port: ${POSTGRES_PORT}
          database: ${POSTGRES_DATABASE}
```

Environment variables are expanded at runtime, not when the file is parsed.

## Generic Credentials

Generic credentials are key-value pairs that can be used to inject secrets into your assets:

```yaml
connections:
  generic:
    - name: MY_SECRET
      value: secretvalue
```

Common use cases include API keys, passwords, and other secrets that don't fit a specific connection type.

## Connection Types

For the specific fields and configuration options for each connection type, refer to the dedicated documentation pages:

### Data Platforms

| Connection Type | Documentation |
|----------------|---------------|
| `google_cloud_platform` | [Google BigQuery](../platforms/bigquery.md) |
| `snowflake` | [Snowflake](../platforms/snowflake.md) |
| `postgres` | [PostgreSQL](../platforms/postgres.md) |
| `redshift` | [Redshift](../platforms/redshift.md) |
| `databricks` | [Databricks](../platforms/databricks.md) |
| `athena` | [AWS Athena](../platforms/athena.md) |
| `duckdb` | [DuckDB](../platforms/duckdb.md) |
| `motherduck` | [MotherDuck](../platforms/motherduck.md) |
| `clickhouse` | [ClickHouse](../platforms/clickhouse.md) |
| `mysql` | [MySQL](../platforms/mysql.md) |
| `mssql` | [Microsoft SQL Server](../platforms/mssql.md) |
| `synapse` | [Azure Synapse](../platforms/synapse.md) |
| `oracle` | [Oracle](../platforms/oracle.md) |
| `trino` | [Trino](../platforms/trino.md) |
| `elasticsearch` | [Elasticsearch](../platforms/elasticsearch.md) |
| `mongo_atlas` | [MongoDB Atlas](../platforms/mongoatlas.md) |
| `s3` | [S3](../platforms/s3.md) |
| `emr_serverless` | [AWS EMR Serverless](../platforms/emr_serverless.md) |
| `dataproc_serverless` | [GCP Dataproc Serverless](../platforms/dataproc_serverless.md) |

### Data Ingestion Sources

Connection schemas for ingestion sources are documented on their respective pages under [Data Ingestion](../ingestion/overview.md). Each source page includes the required `.bruin.yml` connection configuration.

---

## Complete Example

```yaml
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp-prod"
          project_id: "my-project"
          service_account_file: "credentials/gcp-service-account.json"

      postgres:
        - name: "postgres-main"
          username: "${POSTGRES_USER}"
          password: "${POSTGRES_PASSWORD}"
          host: "db.example.com"
          port: 5432
          database: "analytics"

      snowflake:
        - name: "snowflake-prod"
          account: "ABC12345"
          username: "bruin_user"
          private_key_path: "credentials/snowflake_key.p8"
          database: "ANALYTICS"
          warehouse: "COMPUTE_WH"

      generic:
        - name: "SLACK_WEBHOOK"
          value: "https://hooks.slack.com/..."

  staging:
    schema_prefix: "stg_"
    connections:
      google_cloud_platform:
        - name: "gcp-staging"
          project_id: "my-project-staging"
          use_application_default_credentials: true

      postgres:
        - name: "postgres-staging"
          username: "staging_user"
          password: "${STAGING_POSTGRES_PASSWORD}"
          host: "staging-db.example.com"
          port: 5432
          database: "analytics"
```
