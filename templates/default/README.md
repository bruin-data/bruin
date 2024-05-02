# Bruin - Sample Pipeline

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `myschema.example`: A simple SQL asset that creates a table in BigQuery.
  - Feel free to change the type from `bq.sql` to anything.
- `myschema.country_list`: A simple Python asset that installs a dependency and runs the logic.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-dev-schema"
      generic:
        - name: KEY1
          value: value1
  production:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/prod-key.json"
          project_id: "my-project-prod"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-prod-schema" 
      generic:
        - name: KEY1
          value: value1
```

You can simply switch the environment using the `--environment` flag, e.g.:

```shell
bruin validate --environment production . 
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
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