# Seed Assets
Seeds are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your data platform. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the destination platform accurately.

You can define seed assets in a file ending with `.asset.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

The `type` key in the configuration defines what platform to run the query against. 

You can see the "Data Platforms" on the left sidebar to see supported types.

## Parameters

The `parameters` key in the configuration defines the parameters for the seed asset. The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.

##  Examples
The examples below show how to load a CSV into a DuckDB & BigQuery database.

### Simplest: Load csv into a Duckdb
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv
```

Example CSV:
```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the CSV into a table called `seed.raw` in the DuckDB database.

### Adding quality checks
You can attach quality checks to seed assets the same way you do for other assets.

```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv

columns:
  - name: name
    type: string
    checks:
      - name: not_null
      - name: unique
```

The example above ensures that the `name` column contains unique and non-null values after the CSV is loaded.
