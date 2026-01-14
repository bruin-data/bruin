# Bruin - Firebase to GCP Template

This pipeline is a simple example of a Bruin pipeline for Firebase. 

The pipeline includes several sample assets:

- `events/events.asset.yaml`: Monitors for new events data in BigQuery to trigger downstream tasks when new data is detected.
- `events/events.sql`: Defines a BigQuery view for formatted Firebase Analytics event data to support ad-hoc analysis.
- `user_model/cohorts.sql`: A SQL asset that defines cohort-based aggregations for user data.
- `user_model/users.sql`: A SQL asset that defines the users table structure.
- `user_model/users_daily.sql`: A SQL asset that manages daily updates for user data.

For a more detailed description of each asset, refer to the **description** section within each sql asset. Each file provides specific details and instructions relevant to its functionality.

## Setup
The pipeline includes a `.bruin.yml` file where you need to configure your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/commands/connections.html).

Here's a sample `.bruin.yml` configuration:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-id"
 ```

##  Important Note
1- Rename analytics_123456789 folder according to yours.
2- Keep only events_intraday or events in this folder depending on your use case. We recommend you to use events_intraday since streaming data is not bound by 1 million events per day limit.
3- Review TODOs: The SQL files events/events.sql, user_model/users_daily.sql, and events_json.sql contain TODO comments. These indicate sections where you should make adjustments based on your data and project requirements.


## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run ./firebase/pipeline.yml
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
