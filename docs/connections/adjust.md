# Adjust
[Adjust](https://www.adjust.com/) is a mobile marketing analytics platform that provides solutions for measuring and optimizing campaigns, as well as protecting user data.

Bruin supports Adjust as a source, and you can use it to ingest data from Adjust into your data warehouse.

In order to have set up Adjust connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Adjust section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)


```yaml
    connections:
      adjust:
        - name: "connection_name"
          api_key: "abc123"
```