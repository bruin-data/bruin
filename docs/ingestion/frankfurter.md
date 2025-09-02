# Frankfurter
[Frankfurter](https://www.frankfurter.app/) is a free API for current and historical foreign exchange rates.

Bruin supports Frankfurter as a source for ingestr assets, allowing you to ingest exchange rate data directly into your data warehouse.

To set up Frankfurter as a data source and perform data ingestion, follow the instructions provided below. 

### Step 1: Add a connection to .bruin.yml file

Add or modify the `.bruin.yml` file as follows:

```yaml
    connections:
      frankfurter:
        - name: frankfurter
```

### Step 2: Create an asset file for data ingestion

To ingest data from Frankfurter, create an `ingestr` asset with the file `assets/frankfurter.asset.yml`. This file defines the data flow from the source to the destination.

```yaml
name: dataset.frankfurter
type: ingestr
connection: duckdb-default

parameters:
  source_connection: frankfurter
  source_table: 'latest'

  destination: duckdb

```

- `name`: The name of the asset. This must be unique within the pipeline. 
- `type`: Specifies the type of the asset. As Frankfurter is an `ingestr` asset, this should be set to `ingestr`. 
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: The name of the Frankfurter connection defined in `.bruin.yml`.
- `source_table`: The name of the Frankfurter table you want to ingest.

## Available Source Tables

- `currencies`: Retrieves list of available currencies with ISO 4217 codes and names.
- `latest`: Fetches latest exchange rates for all currencies.
- `exchange_rates`: Retrieves historical exchange rates for specified date range.

### Step 3: [Run](/commands/run) asset to ingest data

Navigate to your pipeline folder and run the following command to ingest data from Frankfurter into your data warehouse:

```bash
bruin run assets/frankfurter.asset.yml
```

### Step 4: Query Your Data in DuckDB
Now that the data is in your database, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:

```bash
bruin query --c duckdb-default  --q "SELECT DATE, CURRENCY_NAME, RATE FROM dataset.frankfurter LIMIT 10;" 
```
After executing the query, you will see the following results displayed in your terminal in a clear and organized table format:
```plaintext
┌────────────┬───────────────┬─────────┐
│ DATE       │ CURRENCY_NAME │ RATE    │
├────────────┼───────────────┼─────────┤
│ 2025-04-11 │ EUR           │ 1       │
│ 2025-04-11 │ AUD           │ 1.8201  │
│ 2025-04-11 │ BGN           │ 1.9558  │
│ 2025-04-11 │ BRL           │ 6.6159  │
│ 2025-04-11 │ CAD           │ 1.5736  │
│ 2025-04-11 │ CHF           │ 0.9252  │
│ 2025-04-11 │ CNY           │ 8.2819  │
│ 2025-04-11 │ CZK           │ 25.147  │
│ 2025-04-11 │ DKK           │ 7.4675  │
│ 2025-04-11 │ GBP           │ 0.86678 │
└────────────┴───────────────┴─────────┘
```