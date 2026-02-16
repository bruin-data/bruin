# Connections

Connections are sets of credentials that enable Bruin to communicate with external platforms. They are configured within [environments](/core-concepts/environments) in your `.bruin.yml` file.

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
          password: "${POSTGRES_PASSWORD}"
          host: "db.example.com"
          port: 5432
          database: "analytics"
      
      # Ingestion source connection
      shopify:
        - name: "shopify-default"
          api_key: "${SHOPIFY_API_KEY}"
          store_name: "my-store"
```

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
| `mssql` | [Microsoft SQL Server](/platforms/mssql) |
| `synapse` | [Azure Synapse](/platforms/synapse) |
| `oracle` | [Oracle](/platforms/oracle) |
| `trino` | [Trino](/platforms/trino) |
| `s3` | [S3](/platforms/s3) |

## Ingestion Source Connections

Connection schemas for ingestion sources are documented on their respective pages under [Data Ingestion](/ingestion/overview). Each source page includes the required `.bruin.yml` connection configuration.

## Testing Connections

Use the `connections` command to verify your connections:

```bash
# List all connections
bruin connections list

# Test a specific connection
bruin connections ping gcp-prod
```

## Related Topics

- [Environments](/core-concepts/environments) - Configure multiple environments
- [Secrets](/core-concepts/secrets) - Manage custom credentials
- [.bruin.yml Reference](/secrets/bruinyml) - Complete configuration reference
