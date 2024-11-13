# Bruin - Shopify to BigQuery

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that takes copies a table from Notion to BigQuery

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: "gcp"
                  service_account_file: "<path to service account file>"
                  project_id: "bruin-common-health-check"

            shopify:
                - name: "my-shopify-connection"
                  api_key: "********"
                  url: "******.myshopify.com"
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run assets/shopify.asset.yml
```

```shell
❯ bruin run ./templates/shopify-bigquery/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

[2024-11-11 21:07:43] Starting: raw.shopify
[2024-11-11 21:07:43] [raw.shopify] Triggering ingestr...
[2024-11-11 21:07:46] [raw.shopify] /usr/local/lib/python3.11/site-packages/dlt/common/configuration/specs/base_configuration.py:234: UserWarning: You indicated connection_url to be dlt.secrets.value but type hint is not a secret
[2024-11-11 21:07:46] [raw.shopify]   warnings.warn(
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] Initiated the pipeline with the following:
[2024-11-11 21:07:46] [raw.shopify]   Source: shopify / orders
[2024-11-11 21:07:46] [raw.shopify]   Destination: bigquery / raw.shopify
[2024-11-11 21:07:46] [raw.shopify]   Incremental Strategy: Platform-specific
[2024-11-11 21:07:46] [raw.shopify]   Incremental Key: None
[2024-11-11 21:07:46] [raw.shopify]   Primary Key: None
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] Starting the ingestion...
[2024-11-11 21:07:51] [raw.shopify] ------------------------------- Extract shopify --------------------------------
[2024-11-11 21:07:51] [raw.shopify] Resources: 1/1 (100.0%) | Time: 0.78s | Rate: 1.29/s
[2024-11-11 21:07:51] [raw.shopify] shopify: 0  | Time: 0.03s | Rate: 0.00/s
[2024-11-11 21:07:51] [raw.shopify] 
[2024-11-11 21:07:51] [raw.shopify] Successfully finished loading data from 'shopify' to 'bigquery'  
[2024-11-11 21:07:51] [raw.shopify] 
[2024-11-11 21:07:53] [raw.shopify] ingestr container completed with response code: 0
[2024-11-11 21:07:53] Finished: raw.shopify (9.656s)


Executed 1 tasks in 9.656s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
❯ bruin run ./templates/shopify-bigquery/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

[2024-11-11 21:07:43] Starting: raw.shopify
[2024-11-11 21:07:43] [raw.shopify] Triggering ingestr...
[2024-11-11 21:07:46] [raw.shopify] /usr/local/lib/python3.11/site-packages/dlt/common/configuration/specs/base_configuration.py:234: UserWarning: You indicated connection_url to be dlt.secrets.value but type hint is not a secret
[2024-11-11 21:07:46] [raw.shopify]   warnings.warn(
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] Initiated the pipeline with the following:
[2024-11-11 21:07:46] [raw.shopify]   Source: shopify / orders
[2024-11-11 21:07:46] [raw.shopify]   Destination: bigquery / raw.shopify
[2024-11-11 21:07:46] [raw.shopify]   Incremental Strategy: Platform-specific
[2024-11-11 21:07:46] [raw.shopify]   Incremental Key: None
[2024-11-11 21:07:46] [raw.shopify]   Primary Key: None
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] 
[2024-11-11 21:07:46] [raw.shopify] Starting the ingestion...
[2024-11-11 21:07:51] [raw.shopify] ------------------------------- Extract shopify --------------------------------
[2024-11-11 21:07:51] [raw.shopify] Resources: 1/1 (100.0%) | Time: 0.78s | Rate: 1.29/s
[2024-11-11 21:07:51] [raw.shopify] shopify: 0  | Time: 0.03s | Rate: 0.00/s
[2024-11-11 21:07:51] [raw.shopify] 
[2024-11-11 21:07:51] [raw.shopify] Successfully finished loading data from 'shopify' to 'bigquery'  
[2024-11-11 21:07:51] [raw.shopify] 
[2024-11-11 21:07:53] [raw.shopify] ingestr container completed with response code: 0
[2024-11-11 21:07:53] Finished: raw.shopify (9.656s)


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!