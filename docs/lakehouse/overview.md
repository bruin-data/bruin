# Lakehouse <Badge type="warning" text="beta" />

> [!WARNING]
> Lakehouse support is currently in **beta**. APIs and configuration may change in future releases.

Bruin supports querying open table formats (Iceberg, Delta, DuckLake) stored in cloud object storage through various query engines. See DuckDB's [lakehouse format overview](https://duckdb.org/docs/stable/lakehouse_formats) for background.

## What is a Lakehouse?

A lakehouse combines the scalability of data lakes with the reliability of data warehouses. Data is stored in open formats on object storage (S3, GCS, Azure Blob) while metadata catalogs track schema, partitions, and table history.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Query Engine                        │
│                  (DuckDB, Trino, ...)                   │
└─────────────────────────┬───────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼                               ▼
┌─────────────────────┐       ┌─────────────────────┐
│      Catalog        │       │      Storage        │
│  (Glue, REST, ...)  │       │   (S3, GCS, ...)    │
│                     │       │                     │
│  - Table metadata   │       │  - Parquet files    │
│  - Schema info      │       │  - Manifest files   │
│  - Partition info   │       │  - Data files       │
└─────────────────────┘       └─────────────────────┘
```

## Supported Configurations

| Component | Supported | Planned |
|-----------|-----------|---------|
| **Formats** | Iceberg | Delta, DuckLake |
| **Catalogs** | AWS Glue | REST, Nessie, Hive |
| **Storage** | S3 | GCS, Local |
| **Engines** | DuckDB | Trino |

## Engine Support

Each query engine has its own configuration and capabilities:

| Engine | Status | Read | Write | Notes |
|--------|--------|------|-------|-------|
| [DuckDB](./duckdb.md) | Available | Yes | No | Best for local/embedded analytics |
| [Trino](./trino.md) | Planned | - | - | Best for distributed queries |

## Quick Start

Add a lakehouse configuration to your DuckDB connection:

```yaml
connections:
  duckdb:
    - name: "analytics"
      path: "./analytics.db"
      lakehouse:
        format: iceberg
        catalog:
          type: glue
          catalog_id: "123456789012"
          region: "us-east-1"
        storage:
          type: s3
          region: "us-east-1"
```

Then query your Iceberg tables (defaults to the `main` schema):

```sql
SELECT * FROM users;
```

See the engine-specific pages for detailed configuration options.
