# Frankfurter
[Frankfurter](https://www.frankfurter.app/) is a free API for current and historical foreign exchange rates.

Bruin supports Frankfurter as a source for ingestr assets, allowing you to ingest exchange rate data directly into your data warehouse.

To set up Frankfurter as a data source and perform data ingestion, follow the instructions provided below. 

As in [Quickstart](../getting-started/introduction/quickstart.md), we will start by creating a new default pipeline.

### Step 1: Create a default pipeline

Bruin includes a handy command called `init`. Run this command followed by `default` and a name for your project to create a new default Bruin project.

```bash
bruin init default my-pipeline   
```

This command will:
- Create a project named `my-pipeline`.
- Generate a folder called `my-pipeline` containing the following:
  - An `assets` folder
  - `.bruin.yml` file (where you will add connection and credentials )
  - `pipeline.yml` file to manage your pipeline.



### Step 2: Add a connection to .bruin.yml file

After initializing your project, edit `.bruin.yml` to configure your environments and connections. This file specifies the default environment settings and connections your pipeline will use.

Add or modify the `.bruin.yml` file as follows:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: database.db
      frankfurter:
        - name: frankfurter
```
Here we have defined two connections: one to a DuckDB file named "database.db" (located in the same folder as the .bruin.yml file) and the other to Frankfurter. 

### Step 3: Create an asset file for data ingestion

To ingest data from Frankfurter, create an ingestr asset with the file `assets/frankfurter.asset.yml`. This file defines the data flow from the source to the destination.

```yaml
name: dataset.frankfurter
type: ingestr
connection: duckdb-default

parameters:
  destination: duckdb
  source_connection: frankfurter
  source_table: 'exchange_rates'
  interval-start: '2025-03-26'
  interval-end: '2025-03-27'
```

- `name`: The name of the asset. This must be unique within the pipeline. 
- `type`: Specifies the type of the asset. As Frankfurter is an ingestr asset, this should be set to ingestr. Bruin supports many other types of asset as well. 
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: The name of the Frankfurter connection defined in `.bruin.yml`.
- `source_table`: The name of the Frankfurter table you want to ingest. Frankfurter provides the following tables:
    - `latest`: Contains the latest exchange rates for all supported currencies. The base currency is EUR.
    - `exchange_rates`: Includes historical exchange rate data, allowing you to query rates across a defined date range.
    - `currencies`: Provides a list of all supported currencies available in the Frankfurter API.
- `interval_start` (optional): The beginning of the date range for which historical exchange rates should be fetched. This must be specified in the format `YYYY-MM-DD`. This parameter is **only relevant when using the `exchange_rates` table** and is ignored for other tables.
- `interval_end` (optional): The end of the date range for which historical exchange rates should be fetched. This must be specified in the format `YYYY-MM-DD`. This parameter is **only relevant when using the `exchange_rates` table** and is ignored for other tables.

### Step 4: [Run](/commands/run) asset to ingest data

Run the following command to ingest data from Frankfurter into your data warehouse:

```bash
bruin run my-pipeline
```

### Step 5: Query Your Data in DuckDB
Now that the data is in DuckDB, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:

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
### Notes: 

If no interval For the table `exchange_rates` is specified, the date defaults to today's date and latest published data is retrieved. 
If `interval_start` is specified but `interval_end` is not, `interval_end` defaults to same date as `interval_start`. 
If `interval_end` is specified but `interval_start` is not, both `interval_start` and `interval_end` default to today's date and latest published data is retrieved. .