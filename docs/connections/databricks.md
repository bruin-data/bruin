# Databricks

In order to have set up a databricks connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      databricks:
        - name: "connection_name"
          token: "XXXXXXXX" # This is a personal access token
          host: "xxxxxxxxxx.azuredatabricks.net"
          port: 443 # optional, defaults to 443 
          catalog: "mycatalog"
          schema: "myschema"
```
Despite having to add `catalog` and `schema` to the connection configuration, you also have to add these to the asset name for Bruin to work correctly. See more in [databricks asset documentation.](../assets/databricks.md)


