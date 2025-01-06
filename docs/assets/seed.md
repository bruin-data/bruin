# Seed Assets
Seeds are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your data platform. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the destination platform accurately.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

The `type` key in the configuration defines what platform to run the query against. 

You can see the "Data Platforms" on the left sidebar to see supported types.

## Parameters

The `parameters` key in the configuration defines the parameters for the seed asset. The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file. If the path is not provided, the asset name will be used to find the CSV file in the same directory as the asset definition file.

##  Examples
The examples below show how load a csv into a Duckdb & bigquery database.

### Simplest: Load csv into a Duckdb
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

Example CSV:
```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the csv into a table called `seed.raw` in the Duckdb database.
