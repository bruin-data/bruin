# Bruin - Shopify to Snowflake

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to Snowflake. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that copies a table from Shopify to Snowflake

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            snowflake:
              - name: "connection_name"
                username: "sfuser"
                password: "XXXXXXXXXX"
                account: "AAAAAAA-AA00000"
                database: "dev"
                schema: "schema_name" # optional
                warehouse: "warehouse_name" # optional
                role: "data_analyst" # optional
                region: "eu-west1" # optional

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
❯ bruin run ./templates/shopify-snowflake/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
❯ bruin run ./templates/shopify-snowflake/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!