# Athena

Bruin also supports amazon athena as a connection. In order to have set up an athena connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      athena:
        - name: "connection_name"
          region: "us-west-2"
          database: "some_database" 
          access_key: "XXXXXXXX"
          secret_key: "YYYYYYYY"
          query_results_path: "s3://some-bucket/some-path" 
```

The field `database` is optional, if not provided, it will default to `default`.
The results of the materialization as well as any temporary tables bruin needs to create, will be stored at the location defined by `query_results_path`. This location must be writable and might be required to be empty at the beginning.

