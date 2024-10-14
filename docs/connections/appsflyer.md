# Appsflyer
[Appsflyer](https://www.appsflyer.com/) is a mobile marketing analytics and attribution platform that helps businesses track, measure, and optimize their app marketing efforts across various channels.

Bruin supports Appsflyer as a source, and you can use it to ingest data from Appsflyer into your data warehouse.

In order to have set up Appsflyer connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Appsflyer section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)


```yaml
    connections:
      appsflyer:
        - name: "connection_name"
          api_key: "abc123"
```