# Bruin - Bronze to Silver BigQuery Template

This template demonstrates a complete bronze-to-silver pattern built on top of
BigQuery. It combines a raw ingestion step powered by ingestr with a curated
transformation layer that adds analytics-friendly aggregates and data quality
checks.

## Pipeline Overview

- `assets/bronze_raw_data.asset.yml` creates the **bronze** layer by loading
  publicly available foreign exchange rates from the Frankfurter API into
  BigQuery.
- `assets/silver_aggregated.sql` materializes the **silver** layer by computing
  rolling 7-day averages and day-over-day deltas for each currency.
- `pipeline.yml` wires the assets together and defines default connections for
  Frankfurter and Google Cloud.
- `.bruin.yml` contains a starter configuration you can adapt with your own
  GCP project and service account.

## Initialize the Template

```bash
bruin init bronze-silver-bigquery
```

This creates a folder with the files listed above so you can run the template in
place or tailor it to your environment.

## Configure Connections

Update `.bruin.yml` with your Google Cloud project details and service account.
The Frankfurter source does not require credentials, but you can rename the
connection if desired.

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

> **BigQuery dataset**: The template writes to the `bronze` and `silver`
> datasets. Create them ahead of time or change the dataset names in the asset
> definitions to match your environment.

## Run the Pipeline

Validate and execute the pipeline from the template directory:

```bash
bruin validate .
bruin run .
```

The bronze asset loads historical exchange rates, and the silver asset enriches
that data with rolling metrics suitable for downstream analytics and reporting.

## Data Quality Highlights

- Every column in the bronze asset is monitored for nulls, and a custom check
  ensures the base currency remains `EUR`.
- The silver layer adds positive and not-null checks, plus a custom validation
  that guarantees rolling averages are present for data older than seven days.

## Next Steps

- Swap in another no-auth source such as the Chess template by updating the
  `source_connection` and transformation logic.
- Extend the silver layer with additional materializations (e.g. gold-level
  dashboards or alerts).
- Schedule the pipeline using the `schedule` and `start_date` fields in
  `pipeline.yml` or integrate it with your orchestrator of choice.
