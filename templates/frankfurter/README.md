# Bruin - Frankfurter Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes the following example assets:
- `frankfurter_raw/rates.asset.yml`: An ingestr asset which copies raw exchange rate data to DuckDB.
- `frankfurter_raw/currencies.asset.yml`: An ingestr asset which copies the list of available currencies to DuckDB.
- `frankfurter/currency_names.sql`: An SQL-asset which creates a table from the list of available currencies.
- `frankfurter/daily_rates.sql`: An SQL-asset which cleans the raw currency exchange data by filling in missing rates for weekends and public holidays.
- `fx_insights/currency_performance.sql`: An SQL-asset which shows the latest exchange rates as well as each currency's performance over the past 30 days from `frankfurter/daily_rates.sql` and matches each currency code to the currencies full name from `frankfurter/currency_names.sql`.

## Setup
Add your connections and environments to the `.bruin.yml` file at your project root, not inside the pipeline folder. You can read more about connections [here](https://getbruin.com/docs/bruin/commands/connections.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

You can simply switch the environment using the `--environment` flag, e.g.:


## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
```

You can also run a single task:

```shell
bruin run assets/frankfurter_raw/rates.asset.yml
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it! Good luck!
