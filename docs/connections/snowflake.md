# Snowflake

In order to have set up a Postgres connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      snowflake:
        - name: "connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
```
