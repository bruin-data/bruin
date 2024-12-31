# SQL Seed Assets
Bruin supports running SQL assets against a variety of data platforms natively. Along with SQL assets, Bruin also supports running SQL Seed assets that will populate a table with data from a csv file. 

You can define SQL Seed assets in a file ending with `.yaml`:
```bruin-yaml
name: dashboard.hello_bq
type: duckdb.seed

parameters:
    path: seed.csv
```

The `type` key in the configuration defines what platform to run the query against. 

You can see the "Data Platforms" on the left sidebar to see supported types.

##  Examples
The examples below show how load a csv into a Duckdb & bigquery database.

### Simplest: Load csv into a Duckdb
```bruin-yaml
name: dashboard.hello_bq
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

### Simplest: Load csv into a Bigquery
```bruin-yaml
name: dashboard.hello_bq
type: bq.seed

parameters:
    path: seed.csv
```

Example CSV:
```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the csv into a table called `seed.raw` in the Bigquery database.
