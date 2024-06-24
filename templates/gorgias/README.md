# Bruin - Sample Pipeline

This pipeline is a simple example of a Bruin pipeline that copies data from Gorgias to BigQuery. It copies data from the following resources:
- `customers`
- `tickets`
- `ticket_messages`
- `satisfaction_surveys`

> [!CAUTION]
> Gorgias has very strict rate limits as of the time of building this pipeline, 2 req/s. This means that we cannot extract data from Gorgias in parallel, therefore all of these steps here are built to run sequentially. This is not a problem for small datasets, but it can be a bottleneck for larger datasets.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).

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

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run assets/gorgias.asset.yml
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!