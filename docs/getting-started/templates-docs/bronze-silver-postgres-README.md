# Bronze-to-Silver PostgreSQL Template

The Bronze-to-Silver PostgreSQL template showcases how to pair a credential-free source with a relational
destination by orchestrating an end-to-end Bruin pipeline. The flow ingests raw Frankfurter FX rates into a bronze
table via `ingestr`, then curates a silver summary table with Postgres SQL transformations and built-in quality
checks.

## When to use this template

Choose this template when you want to:

- Stand up a lightweight demonstration of Bruin’s ELT workflow using managed PostgreSQL.
- Combine ingestion, transformation, and validation in a single reproducible project.
- Explore how Bruin enforces data contracts with column-level checks and freshness guarantees.

## Pipeline overview

```
bronze-silver-postgres/
├── .bruin.yml
├── pipeline.yml
└── assets/
    ├── bronze_raw_data.asset.yml
    └── silver_aggregated.sql
```

- **Bronze Layer (`bronze_raw_data.asset.yml`)**
  - Uses `ingestr` to pull daily FX rates from the Frankfurter API into PostgreSQL.
  - Adds column integrity checks, deduplication verification, and freshness monitoring.
- **Silver Layer (`silver_aggregated.sql`)**
  - Materializes an analytic summary table with rolling averages, latest rate snapshots, and observation counts.
  - Ensures downstream quality with mandatory positive rate checks and recency validation.

## Required connections

Edit `.bruin.yml` to supply your PostgreSQL details (Frankfurter is credential-free):

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

Feel free to parameterize the credentials with environment variables or secrets before running in production.

## Try it out

```bash
bruin init bronze-silver-postgres fx-demo
cd fx-demo
bruin validate
bruin run
```

Running the template will populate `public.bronze_exchange_rates` and build `public.silver_exchange_rate_summary`.
Both stages surface quality results so you can immediately confirm the data meets expectations.
