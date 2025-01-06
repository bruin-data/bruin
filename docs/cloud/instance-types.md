# Instance Types

Bruin Cloud runs all assets in individual, ephemeral environments, called "instances". These instances are managed by the Bruin Cloud platform, and provide serverless compute for all of your assets.

You can configure the instance type for each asset inside your asset definitions:

```yaml
instance: "b1.small"
```

The following instance types are available in Bruin Cloud:

| Instance Type | CPU | Memory | Description |
|--------------|-----|--------|-------------|
| b1.nano      | 250m | 256Mi | Smallest instance type, suitable for lightweight tasks |
| b1.small     | 500m | 1Gi   | Good for most data processing tasks |
| b1.medium    | 750m | 2Gi   | For medium-sized workloads |
| b1.large     | 1    | 4Gi   | For compute-intensive tasks |
| b1.xlarge    | 2    | 6Gi   | Largest instance type for heavy workloads |

By default, Bruin Cloud will use the `b1.nano` instance type.

Notes:
- CPU is measured in cores (1000m = 1 core)
- Memory is measured in gibibytes (Gi) or mebibytes (Mi)
- Values shown represent guaranteed resources allocated to your instance

## Custom Instance Types
Bruin Cloud supports custom instance types. You can specify a custom instance type by setting the `instance` field to the name of the instance type you want to use. Talk to your account manager to get access to custom instance types.


