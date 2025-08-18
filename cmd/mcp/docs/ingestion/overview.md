# Data Ingestion

Bruin has built-in data ingestion capabilities thanks to [ingestr](https://github.com/bruin-data/ingestr). The basic idea is simple:
- you have data sources
- each source may have one or more tables/streams
  - e.g. for Shopify, you have customers, orders, products, each being separate tables.
- you want to load these to a destination data platform

[Ingestr](https://github.com/bruin-data/ingestr) abstracts away all of these in the concept of sources, destinations and tables.

Using Bruin, you can load data from any source into your data platforms as a regular asset.

## Definition Schema

Ingestr assets are defined in a simple YAML file:

```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: <source-connection-name>
  source_table: customers
  destination: bigquery
```

The interesting part is in the `parameters` list:
- `source_connection`: the [connection](../getting-started/concepts.md#connection) that defines the source platform
- `source_table`: the table name for that source on [ingestr](https://bruin-data.github.io/ingestr/supported-sources/shopify.html)
- `destination`: the destination you'd like to load the data on

Effectively, this asset will run `ingestr` in the background and load the data to your data warehouse.

## Examples
There are various combinations of sources and destinations, but below are a few examples for common scenarios.

### Load data from Postgres -> BigQuery
```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: my-postgres
  source_table: raw.customers
  destination: bigquery
```

### Shopify Orders -> Snowflake
```yaml
name: raw.orders
type: ingestr
parameters:
  source_connection: my-shopify
  source_table: orders
  destination: snowflake
```

### Kafka -> BigQuery
```yaml
name: raw.topic1
type: ingestr
parameters:
  source_connection: my-kafka
  source_table: topic1
  destination: bigquery
```

