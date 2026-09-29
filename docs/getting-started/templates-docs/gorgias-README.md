# Bruin - Gorgias to Bigquery Template

This pipeline is a simple example of a Bruin pipeline that copies data from Gorgias to BigQuery. It copies data from the following resources:

- `customers`
- `tickets`
- `ticket_messages`
- `satisfaction_surveys`

> [!CAUTION]
> Gorgias has very strict rate limits as of the time of building this pipeline, 2 req/s. This means that we cannot extract data from Gorgias in parallel, therefore all of these steps here are built to run sequentially. This is not a problem for small datasets, but it can be a bottleneck for larger datasets.

## Setup

Add your connections and environments to the `.bruin.yml` file at your project root, not inside the pipeline folder. You can read more about connections [here](https://getbruin.com/docs/bruin/ingestion/gorgias).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      gorgias:
        - name: "gorgias"
          domain: "my-shop"
          email: "my-email@myshop.com"
          api_key: "XXXXXXXX"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
# this will get all the satisfaction surveys starting from 2024-01-01
bruin run --start-date 2024-01-01 assets/satisfaction_surveys.asset.yml
```

You can also run a single task:

```shell
bruin run assets/satisfaction_surveys.asset.yml
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!
