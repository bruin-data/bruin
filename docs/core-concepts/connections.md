# Connections

Connections are sets of credentials that enable Bruin to communicate with external platforms. They are configured within your [project's](/core-concepts/project) `.bruin.yml` file.

## Overview

Bruin supports connections to:

- **Data Platforms**: Where your data is stored and transformed (BigQuery, Snowflake, PostgreSQL, etc.)
- **Ingestion Sources**: Where data is loaded from (Shopify, HubSpot, Stripe, etc.)

## Connection Structure

Connections are defined within an environment under the `connections` key, grouped by connection type:

```yaml
environments:
  default:
    connections:
      # Data platform connection
      google_cloud_platform:
        - name: "gcp-prod"
          project_id: "my-project"
          service_account_file: "credentials/gcp-service-account.json"
      
      # Database connection
      postgres:
        - name: "postgres-main"
          username: "bruin_user"
          password: "super_secret"
          host: "db.example.com"
          port: 5432
          database: "analytics"
      
      # Ingestion source connection
      shopify:
        - name: "shopify-default"
          api_key: "shpca_abc123"
          store_name: "my-store"
```

> [!NOTE]
> You can reference environment variables in connection fields using `${VAR_NAME}` placeholders, which are expanded at runtime.

## Connection Names

Each connection has a unique `name` that you reference in your pipeline and asset definitions:

```yaml
# pipeline.yml
default_connections:
  google_cloud_platform: "gcp-prod"
  postgres: "postgres-main"
```

```yaml
# asset.yml
name: raw.orders
type: ingestr
parameters:
  source_connection: shopify-default
  destination: postgres
```

## Default Connections

Pipelines can define default connections that are automatically used by assets of that type:

```yaml
# pipeline.yml
name: analytics-daily
default_connections:
  google_cloud_platform: "gcp-prod"
  snowflake: "sf-default"
  postgres: "pg-default"
```

Assets automatically inherit these connections unless they specify a different one.

## Limiting Connection Concurrency

If a connection should only be used by a limited number of assets at once, set `max_concurrent_assets` on that connection in `.bruin.yml`:

```yaml
environments:
  default:
    connections:
      snowflake:
        - name: "sf-default"
          account: "ABC12345"
          username: "bruin_user"
          private_key_path: "credentials/snowflake_key.p8"
          database: "ANALYTICS"
          warehouse: "COMPUTE_WH"
          max_concurrent_assets: 4
```

Bruin will queue additional assets that need `sf-default` until a slot is available. This is useful when a database, warehouse, or API has a lower concurrency limit than the overall run's worker count. See [Concurrency & Resource Limits](/getting-started/concurrency#connection-concurrency-limits) for details.

## Data Platform Connections

For specific connection fields and configuration options, see the dedicated documentation:

| Connection Type | Documentation |
|----------------|---------------|
| `google_cloud_platform` | [Google BigQuery](/platforms/bigquery) |
| `snowflake` | [Snowflake](/platforms/snowflake) |
| `postgres` | [PostgreSQL](/platforms/postgres) |
| `redshift` | [Redshift](/platforms/redshift) |
| `databricks` | [Databricks](/platforms/databricks) |
| `athena` | [AWS Athena](/platforms/athena) |
| `duckdb` | [DuckDB](/platforms/duckdb) |
| `motherduck` | [MotherDuck](/platforms/motherduck) |
| `clickhouse` | [ClickHouse](/platforms/clickhouse) |
| `mysql` | [MySQL](/platforms/mysql) |
| `doris` | [Apache Doris](/platforms/doris) |
| `mssql` | [Microsoft SQL Server](/platforms/mssql) |
| `synapse` | [Azure Synapse](/platforms/synapse) |
| `oracle` | [Oracle](/platforms/oracle) |
| `trino` | [Trino](/platforms/trino) |
| `dremio` | [Dremio](/platforms/dremio) |
| `sail` | [Sail](/platforms/sail) |
| `s3` | [S3](/platforms/s3) |

## Ingestion Source Connections

Connection schemas for ingestion sources are documented on their respective pages under [Data Ingestion](/ingestion/overview). Each source page includes the required `.bruin.yml` connection configuration.

## Testing Connections

Use the `connections` command to verify your connections:

```bash
# List all connections
bruin connections list

# Test a specific connection
bruin connections test --name gcp-prod
```

## Related Topics

- [Project](/core-concepts/project) - Configure your project and environments
- [Secrets](/core-concepts/secrets) - Manage custom credentials
- [.bruin.yml Reference](/secrets/bruinyml) - Complete configuration reference
