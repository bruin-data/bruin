# HubSpot
[HubSpot](https://www.hubspot.com/)  is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as a source, and you can use it to ingest data from Hubspot into your data warehouse.

In order to have set up Hubspot connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the HubSpot section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

```yaml
    connections:
      hubspot:
        - name: "connection_name"
          api_key: "key123"
```