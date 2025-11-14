# Elasticsearch

Elasticsearch is a distributed, RESTful search and analytics engine built on Apache Lucene. It provides powerful full-text search, real-time analytics, and scalable data storage capabilities.

Bruin supports Elasticsearch as a data platform for ingestion destinations.

> [!NOTE]
> Elasticsearch is only supported as a destination for ingestion using [Ingestr Assets](../assets/ingestr.md). It cannot be used for SQL-based transformations or other asset types.

## Connection

To set up an Elasticsearch connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
connections:
  elasticsearch:
    - name: "connection_name"
      username: "your-username"
      password: "your-password"
      host: "cluster.cloud.es.io"
      port: 443
      secure: "true"
      verify_certs: "true"
```

**Parameters**:
- `username`: Elasticsearch username (optional for local instances without authentication)
- `password`: Elasticsearch password (optional for local instances without authentication)
- `host`: Elasticsearch host (e.g., `localhost` or `cluster.cloud.es.io`)
- `port`: Elasticsearch port (e.g., `9200` for local, `443` for cloud)
- `secure`: Whether to use HTTPS (`"true"` or `"false"`, defaults to `"true"`)
- `verify_certs`: Whether to verify SSL certificates (`"true"` or `"false"`, defaults to `"true"`)

### Connection Examples

#### Cloud Elasticsearch (with authentication)

```yaml
connections:
  elasticsearch:
    - name: "elastic_cloud"
      username: "elastic"
      password: "changeme"
      host: "cluster.cloud.es.io"
      port: 443
      secure: "true"
      verify_certs: "true"
```

#### Local Elasticsearch with authentication

```yaml
connections:
  elasticsearch:
    - name: "local_elastic"
      username: "elastic"
      password: "changeme"
      host: "localhost"
      port: 9200
      secure: "false"
      verify_certs: "false"
```

#### Local Elasticsearch without authentication

```yaml
connections:
  elasticsearch:
    - name: "local_elastic"
      host: "localhost"
      port: 9200
      secure: "false"
      verify_certs: "false"
```

> [!TIP]
> Cloud Elasticsearch instances typically use HTTPS (port 443) and should have `secure: "true"`. Local instances typically use HTTP (port 9200) and should have `secure: "false"`.

## Using Elasticsearch as a Destination

Elasticsearch can be used as a destination for [Ingestr Assets](../assets/ingestr.md). This allows you to load data from various sources into your Elasticsearch cluster.

### Example: Load data from Snapchat Ads to Elasticsearch

```yaml
name: snapchat.ads
type: ingestr
description: Snapchat Ads individual ads data with merge strategy for incremental updates

tags:
  - snapchat
  - ads
  - ingestion

parameters:
  destination: elasticsearch
  source_connection: my-snapchatads
  source_table: ads
```

This configuration will:
1. Extract ads data from Snapchat Ads using the `my-snapchatads` connection
2. Load the data into the `ads` index in your Elasticsearch cluster
3. Use the default Elasticsearch connection (`elasticsearch-default`) from your pipeline configuration

> [!NOTE]
> By default, ingestr uses a "replace" strategy which deletes the existing index before loading new data. The target index will be created automatically if it doesn't exist.

## Index Naming

The `destination_table` parameter specifies the Elasticsearch index name where data will be loaded. Index names in Elasticsearch:
- Must be lowercase
- Cannot contain spaces or special characters like `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, `,`, `#`
- Cannot start with `-`, `_`, `+`
- Should follow your organization's naming conventions

## Additional Resources

- [Elasticsearch Official Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Ingestr Elasticsearch Documentation](https://bruin-data.github.io/ingestr/supported-sources/elasticsearch.html)
- [Bruin Ingestr Assets](../assets/ingestr.md)
