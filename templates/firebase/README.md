# Bruin - Firebase to GCP Template

This pipeline is a simple example of a Bruin pipeline for Firebase. 

The pipeline includes several sample assets:

- `analytics_123456789/events.asset.yaml` / `events_intraday.asset.yaml`: Sensors that watch for new Firebase export tables in BigQuery and trigger downstream tasks. Keep one depending on your export type (daily vs intraday).
- `analytics_123456789/parse_version.sql`: BigQuery UDF that normalizes app version strings (e.g. `1.20.3` → `001.020.003`) for sortable comparisons.
- `events/stg_events.sql`: View over the raw `analytics_*.events_*` wildcard table. Owns all parsing/flattening logic (event_params, user_properties, experiments, device, geo).
- `events/events_json.sql`: Materialized incremental table over `stg_events`, partitioned by `dt`, clustered by `event_name` + `user_pseudo_id`. Used for historical queries.
- `events/events.sql`: View that unions `events_json` (history, `dt <= end_date`) with `stg_events` (intraday, `dt > end_date`) for near-real-time coverage without re-scanning history. Adds typed columns (screen, session, ads, idfa/idfv).
- `user_model/stg_users_daily.sql`: Incremental table with daily user-level aggregates (sessions, ad/IAP revenue, first/last device & geo of day) from `events.events`.
- `user_model/users.sql`: User-level table with install-time attributes and cohorted retention/revenue metrics (`ret_d{1..90}`, `{metric}_d{N}`).
- `user_model/users_daily.sql`: Enriched daily rollup that joins `stg_users_daily` back with `users` to tag each daily row with install context, `days_since_install`, and `nth_active_day`.

For a more detailed description of each asset, refer to the **description** section within each sql asset. Each file provides specific details and instructions relevant to its functionality.

## Setup
The pipeline includes a `.bruin.yml` file where you need to configure your connections and environments. You can read more about connections [here](https://getbruin.com/docs/bruin/commands/connections.html).

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
1- Rename `analytics_123456789` (folder + references in `stg_events.sql`) to your Firebase analytics ID.
2- Keep only `events_intraday.asset.yaml` or `events.asset.yaml` depending on your use case. We recommend `events_intraday` since streaming data is not bound by the 1M events/day limit. `stg_events.sql` defaults to the intraday sensor — update its `depends:` block if you use daily export instead.
3- Review TODOs: `events/stg_events.sql`, `events/events.sql`, and `user_model/stg_users_daily.sql` contain TODO comments. These indicate sections where you should make adjustments based on your data and project requirements (analytics ID, user_id vs user_pseudo_id, app-specific event params and metrics).


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
