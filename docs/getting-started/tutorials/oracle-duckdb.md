# Copy Oracle Tables Into DuckDB With Bruin

This hands-on tutorial walks you through the new `oracle-duckdb` template. By the end, you will spin up a local Oracle XE container, ingest three source tables into DuckDB using `ingestr`, and materialize the `duckdb.sales_per_customer` table that joins everything together.

## Prerequisites

- [Bruin CLI](../introduction/installation.md) installed locally
- Docker Desktop (or another Docker runtime) running
- Optional but handy: the `duckdb` CLI to inspect the generated `.duckdb` file

## 1. Initialize the template

Pick an empty folder and run:

```bash
bruin init oracle-duckdb oracle-demo
cd oracle-demo
```

The template ships with:

- `pipeline.yml` defining default `oracle` and `duckdb` connections plus the four assets.
- `assets/` containing three `ingestr` jobs (`oracle_raw.*`) and a downstream `duckdb.sql`.
- `.bruin.yml` ready for you to wire credentials.

## 2. Configure `.bruin.yml`

Open the generated `.bruin.yml` file and point it to your Oracle XE container + local DuckDB path:

```yaml
default_environment: default

environments:
  default:
    connections:
      oracle:
        - name: oracle-default
          username: BRUIN_TMPL
          password: bruin_password
          host: 127.0.0.1
          port: 1521
          service_name: XEPDB1
      duckdb:
        - name: duckdb-default
          path: ./oracle_duckdb.duckdb
```

> Keep the connection names (`oracle-default`, `duckdb-default`) unchanged so the prebuilt assets pick them up automatically.

## 3. Start Oracle in Docker

Launch Oracle XE and expose the SQL*Net port:

```bash
docker run -d --name bruin-oracle -p 1521:1521 \
  -e ORACLE_PASSWORD=Password123 \
  gvenzl/oracle-xe
```

Watch the logs until you see `DATABASE IS READY TO USE!`. This image defaults to the `XEPDB1` pluggable DB that the template references.

## 4. Create a demo schema and seed data

Connect with `sqlplus` (ships in the container) to create the `BRUIN_TMPL` user and populate three sample tables:

```bash
docker exec -it bruin-oracle sqlplus system/Password123@XEPDB1 <<'SQL'
BEGIN
  EXECUTE IMMEDIATE 'DROP USER BRUIN_TMPL CASCADE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -1918 THEN
      RAISE;
    END IF;
END;
/
CREATE USER BRUIN_TMPL IDENTIFIED BY bruin_password;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO BRUIN_TMPL;
SQL
```

Now seed three operational tables plus fake data:

```bash
docker exec -it bruin-oracle sqlplus bruin_tmpl/bruin_password@XEPDB1 <<'SQL'
CREATE TABLE customers (
  customer_id NUMBER PRIMARY KEY,
  full_name   VARCHAR2(100),
  segment     VARCHAR2(30),
  email       VARCHAR2(150),
  updated_at  TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE orders (
  order_id     NUMBER PRIMARY KEY,
  customer_id  NUMBER NOT NULL REFERENCES customers(customer_id),
  order_status VARCHAR2(20),
  order_total  NUMBER(10,2),
  ordered_at   TIMESTAMP,
  updated_at   TIMESTAMP
);

CREATE TABLE order_items (
  order_item_id NUMBER PRIMARY KEY,
  order_id      NUMBER NOT NULL REFERENCES orders(order_id),
  product_sku   VARCHAR2(40),
  quantity      NUMBER,
  unit_price    NUMBER(10,2)
);

INSERT ALL
  INTO customers VALUES (1, 'Alice Carter', 'digital', 'alice@example.com', SYSTIMESTAMP - 5)
  INTO customers VALUES (2, 'Ben Howard', 'retail', 'ben@example.com', SYSTIMESTAMP - 3)
  INTO customers VALUES (3, 'Chloe Kim', 'enterprise', 'chloe@example.com', SYSTIMESTAMP - 1)
SELECT * FROM dual;

INSERT ALL
  INTO orders VALUES (1001, 1, 'completed', 180.50, SYSTIMESTAMP - 4, SYSTIMESTAMP - 3)
  INTO orders VALUES (1002, 1, 'completed', 75.00, SYSTIMESTAMP - 2, SYSTIMESTAMP - 2)
  INTO orders VALUES (1003, 2, 'processing', 220.00, SYSTIMESTAMP - 1, SYSTIMESTAMP - 1)
  INTO orders VALUES (1004, 3, 'completed', 540.00, SYSTIMESTAMP - 1, SYSTIMESTAMP - 1/24)
SELECT * FROM dual;

INSERT ALL
  INTO order_items VALUES (1, 1001, 'SKU-RED-CHAIR', 2, 60.00)
  INTO order_items VALUES (2, 1001, 'SKU-BLUE-RUG', 1, 60.50)
  INTO order_items VALUES (3, 1002, 'SKU-TEA-CUP', 3, 25.00)
  INTO order_items VALUES (4, 1003, 'SKU-PLANTER', 4, 55.00)
  INTO order_items VALUES (5, 1004, 'SKU-DESK', 1, 540.00)
  INTO order_items VALUES (6, 1003, 'SKU-CANDLE', 2, 20.00)
SELECT * FROM dual;
COMMIT;
SQL
```

Feel free to adjust the data; the assets ingest whatever is present.

## 5. Run the pipeline

From the project root:

```bash
bruin run . --config-file ./.bruin.yml
```

Bruin validates the pipeline, spins up three parallel `ingestr` transfers, and finally runs the DuckDB transformation:

- `oracle_raw.customers` → DuckDB table `oracle_raw.customers`
- `oracle_raw.orders` → DuckDB table `oracle_raw.orders`
- `oracle_raw.order_items` → DuckDB table `oracle_raw.order_items`
- `duckdb.sales_per_customer` builds a reporting-friendly aggregation.

All four assets should finish successfully in ~10 seconds with Docker running locally.

## 6. Inspect the DuckDB output

Use DuckDB (or any SQLite-compatible tool) to confirm the files:

```bash
duckdb oracle_duckdb.duckdb "SELECT * FROM duckdb.sales_per_customer ORDER BY customer_id"
```

Example output:

```
┌─────────────┬──────────────┬───────────────┬─────────────┬─────────────────────┐
│ customer_id │  full_name   │ total_revenue │ order_count │   last_order_date   │
├─────────────┼──────────────┼───────────────┼─────────────┼─────────────────────┤
│         1.0 │ Alice Carter │         255.5 │           2 │ 2025-11-11 20:45:34 │
│         2.0 │ Ben Howard   │         260.0 │           1 │ 2025-11-13 20:45:34 │
│         3.0 │ Chloe Kim    │         540.0 │           1 │ 2025-11-14 19:45:34 │
└─────────────┴──────────────┴───────────────┴─────────────┴─────────────────────┘
```

## 7. Clean up (optional)

When you’re done experimenting:

```bash
docker rm -f bruin-oracle
rm oracle_duckdb.duckdb
```

## Next steps

- Replace the sample tables with your real Oracle schemas by editing each `source_table` parameter under `assets/`.
- Add more DuckDB models in `assets/*.sql` to transform the landed data.
- Deploy on a schedule via the `schedule` block in `pipeline.yml` once you move beyond local testing.

Happy data moving! 🚀
