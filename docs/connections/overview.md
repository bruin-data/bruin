# Connections & Platforms

Bruin uses **connections** and **platforms** to interact with your data infrastructure. Understanding the distinction between them is key to configuring your pipelines.

## Connections

A **connection** is a named configuration that tells Bruin how to authenticate and communicate with an external system. Connections are defined in your `.bruin.yml` file and include credentials, endpoints, and other settings needed to access a data platform or source.

For example, a BigQuery connection includes your GCP project ID, region, and service account credentials. You can define multiple connections of the same type, each with a different name, allowing you to work with multiple environments or accounts.

```yaml
connections:
  google_cloud_platform:
    - name: "my-bigquery"
      project_id: "my-project"
      location: "US"
      service_account_file: "path/to/file.json"
```

Connections are referenced by name in your pipeline and asset definitions, keeping credentials separate from your pipeline logic.

## Platforms

A **platform** (also referred to as a warehouse or data lake) is the actual data system where Bruin runs transformations and stores results. Each platform has its own capabilities, SQL dialect, and configuration requirements.

Bruin supports a wide range of platforms:

- **Cloud warehouses**: BigQuery, Snowflake, Databricks, Redshift, Athena, Synapse, Microsoft Fabric
- **Relational databases**: PostgreSQL, MySQL, Microsoft SQL Server, Oracle, SAP HANA, SQLite, IBM DB2, Google Cloud Spanner, Vertica
- **Analytical engines**: ClickHouse, DuckDB, MotherDuck, Trino
- **Search & document stores**: Elasticsearch, MongoDB Atlas
- **Cloud storage**: S3, GCS
- **Compute engines**: AWS EMR Serverless, GCP Dataproc Serverless

## Data Ingestion

Beyond running transformations on platforms, Bruin can also **ingest data** from 70+ external sources (Shopify, Stripe, HubSpot, Kafka, etc.) into your data platforms using [ingestr](https://github.com/bruin-data/ingestr). Each ingestion source requires its own connection configuration.

See the [Data Ingestion](/ingestion/overview) section for details on available sources and how to configure them.
