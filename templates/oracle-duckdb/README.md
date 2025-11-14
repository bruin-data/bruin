# Bruin - Oracle to DuckDB

`oracle-duckdb` is an end-to-end pipeline template that copies three operational tables from Oracle into DuckDB using `ingestr`, then builds a curated DuckDB table on top of the staged data. The template demonstrates how to combine Oracle ingestion jobs with a downstream DuckDB transformation inside the same Bruin pipeline.

## Assets included
- `oracle_raw.customers` – Copies customer master data from Oracle into DuckDB
- `oracle_raw.orders` – Copies the main orders fact table from Oracle into DuckDB
- `oracle_raw.order_items` – Copies order line items from Oracle into DuckDB
- `duckdb.sales_per_customer` – DuckDB table that aggregates order metrics from the three staging tables

## Setup
1. **Configure connections** – Update `.bruin.yml` after initializing the template. Here is a sample configuration to pair an Oracle XE container with a DuckDB file:
    ```yaml
    default_environment: default
    environments:
      default:
        connections:
          oracle:
            - name: "oracle-default"
              username: "bruin_tmpl"
              password: "bruin_password"
              host: "127.0.0.1"
              port: 1521
              service_name: "XEPDB1"
          duckdb:
            - name: "duckdb-default"
              path: "/absolute/path/to/oracle_duckdb.duckdb"
    ```
2. **Start Oracle (Docker)** – This template assumes Oracle is reachable at the host/port above. Locally you can use:
    ```bash
    docker run -d --name bruin-oracle -p 1521:1521 \
      -e ORACLE_PASSWORD=Password123 \
      gvenzl/oracle-xe
    ```
    Wait for the logs to show `DATABASE IS READY TO USE!`.
3. **Create schema + sample data** – Connect with `sqlplus` (or any Oracle client) and create the `bruin_tmpl` user plus the three tables referenced by the assets.

## Running the pipeline
After the connections are configured, run the template from its directory:
```bash
bruin run templates/oracle-duckdb
```
Bruin will execute every asset in dependency order:
1. Each `ingestr` asset copies its table from Oracle to DuckDB.
2. The DuckDB SQL asset builds `duckdb.sales_per_customer`, providing a downstream example that joins across all three staging tables.

Use `bruin run --asset assets/oracle_orders.asset.yml --downstream` to re-run the orders sync together with all downstream dependents whenever you update the Oracle source.
