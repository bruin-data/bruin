# Instance Types

Bruin Cloud runs all assets in individual, ephemeral environments, called "instances". These instances are managed by the Bruin Cloud platform, and provide serverless compute for all of your assets.

You can configure the instance type for each asset inside your asset definitions:

```yaml
instance: "b1.small"
```

The following instance types are available in Bruin Cloud:

| Instance Type | CPU  | Memory |
|---------------|------|--------|
| b1.pico       | 125m | 256Mi  |
| b1.nano       | 250m | 512Mi  |
| b1.small      | 500m | 1500Mi |
| b1.medium     | 750m | 2400Mi |
| b1.large      | 1    | 4Gi    |
| b1.xlarge     | 2    | 6Gi    |
| b1.2xlarge    | 2.5  | 8Gi    |
| b1.3xlarge    | 3    | 12Gi   |
| b1.4xlarge    | 3.5  | 16Gi   |
| b1.5xlarge    | 4    | 24Gi   |
| b1.6xlarge    | 4.5  | 32Gi   |
| b1.7xlarge    | 5    | 48Gi   |
| b1.8xlarge    | 5    | 64Gi   |

If you don't set `instance`, Bruin Cloud picks one based on the asset type:

| Asset Type | Default Instance |
|------------|------------------|
| Python | `b1.nano` (`b1.small` when materialized as a table) |
| Ingestr | `b1.medium` |
| SQL | `b1.nano` |
| dbt | `b1.nano` |
| Sensors | `b1.pico` |

Notes:

- CPU is measured in cores (1000m = 1 core)
- Memory is measured in gibibytes (Gi) or mebibytes (Mi)
- Values are the maximum each instance can use; on `b1.xlarge` and larger, CPU may be lower when the cluster is busy
- An asset that goes over its max memory is stopped with an out-of-memory error; pick a larger instance if this happens

## How Instance Size Affects Concurrency

Larger instances consume more of your tenant's resource pool. See [Concurrency & Resource Limits](/getting-started/concurrency) for details on the weighted-slot model.

## Custom Instance Types

Bruin Cloud supports custom instance types. You can specify a custom instance type by setting the `instance` field to the name of the instance type you want to use. Talk to your account manager to get access to custom instance types.

## Related

- [Pipelines](/cloud/pipelines) for how scheduled and manual runs use these instances.
- [Asset definition](/assets/definition-schema) for setting the `instance` field on an asset.
