# Cross-pipeline dependencies

Bruin Cloud allows defining dependencies between pipelines. This allows you to run pipelines and refresh assets in parallel, while ensuring assets that need to wait on each other wait until the upstream dependencies are ready.

## URIs

Bruin considers assets unique; however, asset names often do not fulfill the uniqueness criteria across multiple repos, projects and pipelines. While asset names are required to be unique within the same pipeline, there can be assets with the same name across different pipelines. For instance, a mobile gaming company may have a pipeline for each game, and each pipeline may have an asset named `raw.events`. This poses a problem of uniqueness for dependencies that span pipelines and repos. 

However, even if they have assets with the same name, in reality they have separate tables in their data warehouse.

In order to uniquely identify where the individual assets contain, Bruin has a concept of a URI. A URI is a unique identifier for an asset, and it is expected to be unique across all pipelines and repos of the customer.


> [!WARNING]
> URI is a required field for cross-pipeline dependencies. If you do not provide a URI, Bruin will not be able to resolve the dependency.

### URI Format
Bruin Cloud does not enforce a structure for the URI yet; however we recommend following the standard URI format with the destination information. These URIs need not be resolvable or accessible by any means, they are meant to convey the location of the asset.

Here are some common examples for major data warehouses:
- BigQuery: `bigquery://project.dataset.table`
- Snowflake: `snowflake://database.schema.table`
- Redshift: `redshift://database.schema.table`
- S3: `s3://bucket/path/to/file`

## Defining dependencies

In order to depend on an upstream that lives elsewhere, Bruin Cloud utilizes URIs, and identifies the upstream asset by the URI.

Let's say you have an upstream asset with a URI:
```yaml
name: asset1

# the URI allows us to identify the asset across pipelines and repos.
uri: external://some_external_asset
```

In order to define a dependency on this upstream asset that might be living anywhere across your repos and pipelines, you can use the new `uri` key in the `depends` array:
```yaml
name: domain.my_asset

depends:
  - some_asset
  - another_one
  
  # dependencies now accept objects with a `uri` key as well.
  - uri: external://some_external_asset
```

The new `uri` key in the `depends` array allows you to define a dependency on an asset that lives in a different pipeline. This allows you to define cross-pipeline dependencies without having to know what pipeline or repo the asset lives in.

> [!INFO]
> Bruin required `depends` to be a string array previously, whereas now it can accept an object with the `uri` key as well.


## How it works?

When you define a cross-pipeline dependency, Bruin will wait for the upstream pipeline to finish before starting the downstream pipeline. This ensures that the downstream pipeline has the latest data from the upstream pipeline. The way it works is this:
- Bruin identifies the upstream asset from the URI, looking across all the repos and pipelines of the team.
- Once the upstream is identified, Bruin defines a "sensor" on the upstream asset. 
  - This sensor is a lightweight process that listens to the upstream asset and triggers the downstream pipeline when the upstream asset is refreshed.
  - These sensors are displayed as "prerequisites" in the asset run page for the downstream.
- Once the sensor succeeds, i.e. the upstream asset is passed successfully, the current asset is triggered and the pipeline runs as usual.

This approach has a couple of advantages:
- It allows running pipelines in parallel without any trigger-based approach, enables a more resilient and decoupled architecture.
- It is flexible for upstream failures, meaning that it will keep waiting for the upstream even if it fails, in case it succeeds later.
- It allows for a more granular control over the dependencies, as you can define dependencies on a per-asset basis.

## Limitations
- `bruin validate` CLI command validates the structure of the dependencies, but cannot validate if the URI actually exists.
- The downstream will wait for 12 hours maximum for the upstream to pass, then it will fail. This is to prevent the downstream from waiting indefinitely for the upstream to pass.

## Dependencies with different schemas

When the upstream and downstream both have the same schedule, it's easy to determine when does the downstream need to run. 
E.g if both every 5 minutes, then if downstream has a data interval from `2025-10-11 15:30:00`to `2025-10-11 15:35:00`,
then the upstream should have a successful run for the same interval.

In the case of mixed schedules it's a bit more complicated but still possible.
Imagine we have a downstream that runs every 5 minutes and we have a run with the data interval  `2025-10-11 15:30:00`to `2025-10-11 15:35:00`.
This downstream has an external dependency that runs every 2 minutes.

The in order to run the downstream we will wait until the upstream has successful runs with data intervals covering fully the downstream data interval.
For example for the aforementioned `2025-10-11 15:30:00`to `2025-10-11 15:35:00`, we would need downstream to have for example.

 * `2025-10-11 15:30:00`to `2025-10-11 15:32:00
 * `2025-10-11 15:32:00`to `2025-10-11 15:34:00
 * `2025-10-11 15:34:00`to `2025-10-11 15:36:00

These intervals fully cover the original downstream interval and thus the downstream can run.



