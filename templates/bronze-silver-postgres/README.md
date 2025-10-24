# Bruin - Bronze to Silver Postgres Template

This template demonstrates a complete bronze-to-silver ingestion workflow using PostgreSQL for storage.
It combines the Frankfurter FX API as a credential-free source with a PostgreSQL destination to present a
canonical ELT pattern: raw collection via `ingestr` followed by curated transformations in SQL.

## Included assets

- `assets/bronze_raw_data.asset.yml`  
  Uses `ingestr` to copy foreign exchange rates from the Frankfurter API into PostgreSQL. The asset tracks
  data quality via column-level checks, deduplication assurance, and freshness validation.

- `assets/silver_aggregated.sql`  
  Builds a summarized silver table in PostgreSQL with recent exchange rate insights, rolling averages,
  and observation counts ready for analytics or downstream modeling.

## Setup

The template includes a `.bruin.yml` file seeded with placeholder connection values:

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      postgres:
        - name: "postgres-default"
          host: "localhost"
          port: 5432
          database: "bruin"
          username: "postgres"
          password: "postgres"
          ssl_mode: "disable"
```

Update the PostgreSQL connection with the credentials for your target instance. The Frankfurter source does not
require authentication.

Ensure the target database allows table creation in the `public` schema or adjust the asset definitions to match
your own schema naming conventions.

## Running the pipeline

Initialize a new project from this template and execute the full bronze-to-silver flow:

```bash
bruin init bronze-silver-postgres my-fx-pipeline
cd my-fx-pipeline
bruin run
```

The run will:
1. Ingest the latest Frankfurter exchange rates into `public.bronze_exchange_rates`.
2. Build `public.silver_exchange_rate_summary`, providing recent averages and data freshness checks.

Both assets include built-in quality checks so `bruin validate` and `bruin run` report actionable status.
