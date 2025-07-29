# Trino Operator

This package provides a Trino operator for the Bruin ELT tool.

## Configuration

Trino connections can be configured with optional catalog and schema specification. Here's an example configuration:

```yaml
connections:
  trino:
    - name: trino-default
      host: trino.example.com
      port: 8080
      username: your_username
      catalog: hive  # Optional - specifies the data source catalog
      schema: default  # Optional - specifies the schema within the catalog
```

## Fields

- `name`: Connection name (required)
- `host`: Trino server hostname (required)
- `port`: Trino server port (required, typically 8080)
- `username`: Username for authentication (required)
- `catalog`: **Optional** - The catalog to use (e.g., "hive", "iceberg", "delta")
- `schema`: **Optional** - The schema within the catalog (e.g., "default", "public")

## Usage

Once configured, you can use Trino assets in your pipeline:

```yaml
assets:
  - name: my_trino_query
    type: trino.sql
    connection: trino-default
    executable_file:
      content: |
        SELECT * FROM my_table
        WHERE date >= '2024-01-01'
```

## Notes

- Catalog and schema are optional - if not specified, Trino will use defaults
- Queries should not include semicolons at the end (they are automatically stripped)
- The operator supports the standard Bruin query operations: `Select`, `SelectWithSchema`, `RunQueryWithoutResult`, and `Ping` 