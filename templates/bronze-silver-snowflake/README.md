# Bruin - Bronze to Silver Snowflake Template

This template bootstrap demonstrates a full bronze-to-silver workflow on Snowflake using the public [Frankfurter FX API](https://www.frankfurter.app/). Bruin ingests raw exchange rates with `ingestr` into a Snowflake bronze schema and then produces a curated silver snapshot that summarises recent performance metrics.

## Prerequisites

- A Snowflake account with permission to create tables in your target database/schema.
- A warehouse that can execute the silver transformation query.
- Authenticated `bruin` CLI with `ingestr` dependencies installed (via `make deps`).

### Configure `.bruin.yml`

Update the generated `.bruin.yml` with your credentials. The template ships with placeholders that you can replace:

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      snowflake:
        - name: "snowflake-default"
          account: "ABCD1234-XY00000"
          username: "BRUIN_DEV"
          password: "<replace-with-password-or-remove-for-key-auth>"
          database: "BRUIN_DEMO"
          schema: "BRONZE"
          warehouse: "COMPUTE_WH"
          role: "ANALYST"
```

If you use key-based authentication, replace the `password` with the `private_key_path` or inline `private_key` fields. See [`docs/platforms/snowflake.md`](../../docs/platforms/snowflake.md) for full configuration options.

## Assets

- **`bronze_raw_data.asset.yml`**: Uses `ingestr` to copy the Frankfurter `/rates` endpoint into Snowflake. Column-level checks confirm base currency coverage, ensure exchange rates remain positive, and track ingestion timestamps.
- **`silver_aggregated.sql`**: Snowflake SQL transformation (`sf.sql`) that surfaces the most recent rate alongside 7-day and 30-day deltas with rolling averages and bounds for each currency code. It includes column checks for critical metrics and a custom check enforcing non-null rolling averages.

## Running the Pipeline

1. Initialise the template (choose your target directory):
   ```bash
   bruin init bronze-silver-snowflake my-fx-pipeline
   ```
2. Replace the Snowflake placeholders in `.bruin.yml` with working credentials and grant your role rights to the target schema.
3. Validate the project structure and asset definitions:
   ```bash
   bruin validate my-fx-pipeline
   ```
4. Execute the bronze and silver layers:
   ```bash
   bruin run my-fx-pipeline/pipeline.yml --environment default
   ```

The run will ingest raw FX observations into `bronze.frankfurter_rates` and then populate `silver.currency_rate_snapshot` with current rate intelligence that can feed downstream gold data products.
