# Bruin - Bronze to Silver BigQuery Template

This template delivers an end-to-end example of the bronze-to-silver pattern in
Bruin. It ingests raw exchange rates from the credential-free Frankfurter API
into BigQuery and then builds curated metrics that are ready for analytics.

The pipeline ships with:

- `bronze.frankfurter_rates`: An ingestr asset that captures daily FX rates in a
  bronze dataset.
- `silver.fx_rate_enriched`: A BigQuery SQL asset that adds rolling averages,
  day-over-day deltas, and quality checks.
- `.bruin.yml`: A starter configuration showing how to connect Frankfurter and
  Google Cloud.

## Setup

Initialize the template from the Bruin CLI:

```bash
bruin init bronze-silver-bigquery
```

Update the generated `.bruin.yml` with your Google Cloud project and service
account. Frankfurter does not require credentials.

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      google_cloud_platform:
        - name: "gcp-default"
          project_id: "your-gcp-project-id"
          service_account_file: "/path/to/service-account.json"
```

Create the `bronze` and `silver` datasets in BigQuery, or adjust the asset names
to match datasets that already exist.

## Running the pipeline

From the template directory, validate the pipeline and then materialize it:

```bash
bruin validate .
bruin run .
```

The run performs two steps:

1. Loads raw currency rates from Frankfurter into `bronze.frankfurter_rates`.
2. Generates rolling metrics in `silver.fx_rate_enriched`, complete with sensible
   quality checks.

## Quality checks

- Bronze layer: not-null and positivity checks plus a custom validation that
  ensures the base currency remains EUR.
- Silver layer: not-null and positivity checks on analytics columns and a custom
  query to guarantee rolling averages are present for historical data.

Use this template as the foundation for more advanced multi-layer pipelines or
swap the source for any other ingestr-compatible system that does not require
credentials.
