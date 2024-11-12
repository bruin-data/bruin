# Bruin - Shopify to Redshift

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to Redshift. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that takes copies a table from Shopify to Redshift

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            redshift:
              - name: "connection_name"
                username: "awsuser"
                password: "XXXXXXXXXX"
                host: "redshift-cluster-1.xxxxxxxxx.eu-north-1.redshift.amazonaws.com"
                port: 5439
                database: "dev"
                ssl_mode: "allow"

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
❯ bruin run ./templates/shopify-redshift                                                       (bruin) 
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
❯ bruin run ./templates/shopify-redshift/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!