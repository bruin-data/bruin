# Bruin - Clickhouse Template

This pipeline is a simple example of a Bruin pipeline for Clickhouse, 
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints 
like `not_null`, `unique`, and `primary_key`.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).
You will need a clickhouse server. You can run one locally with docker running the following:

```bash
docker run -e CLICKHOUSE_DB=default -e CLICKHOUSE_USER=username -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=password -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

Here's a sample `.bruin.yml` file that would work with the docker container above:


```yaml
default_environment: default
environments:
  default:
    connections:
      clickhouse:
        - name: clickhouse-default
          username: username
          password: password
          host: 127.0.0.1
          port: 19000
          database: my_database
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell

bruin run ./clickhouse/pipeline.yml
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