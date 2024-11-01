# Kafka
[Kafka](https://kafka.apache.org/) is a distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

ingestr supports Kafka as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Klaviyo into your data warehouse.

In order to have set up Kafka connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Kafka section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

```yaml
    connections:
      Kafka:
        - name: "connection_name"
          bootstrap_servers: "localhost:9093"
          group_id: "test123"
```