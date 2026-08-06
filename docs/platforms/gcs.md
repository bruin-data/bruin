# Google Cloud Storage Platform

Google Cloud Storage (GCS) is supported in Bruin for data ingestion and as a sensor for monitoring object availability.

## Connection Configuration

Add a GCS connection to your `.bruin.yml` file. To use [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials), configure:

```yaml
connections:
  gcs:
    - name: gcs-default
      use_application_default_credentials: true
```

For local development, create ADC with:

```bash
gcloud auth application-default login
```

You can use a service account instead:

```yaml
connections:
  gcs:
    - name: gcs-default
      service_account_file: path/to/service-account.json
```

The `service_account_json` field is also supported. A `google_cloud_platform` connection can be referenced explicitly by name instead of a GCS connection.

## Object Sensor

Create the sensor as a standalone YAML asset named `<name>.asset.yml`:

```yaml
name: wait_for_gcs_object
type: gcs.sensor.object
connection: gcs-default
parameters:
  bucket: my-data-bucket
  object: path/to/expected/file.csv
  poke_interval: 30
  timeout: 1h
```

### Parameters

- `bucket` (required): GCS bucket name without the `gs://` prefix. `bucket_name` is accepted as an alias.
- `object` (required): Full object name within the bucket. `object_name` and `bucket_key` are accepted as aliases.
- `poke_interval` (optional): Polling interval in seconds. Defaults to `30`.
- `timeout` (optional): Maximum wait duration. It uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, or `ns`), such as `1h` or `90m`, and defaults to `24h`.

The object field supports the same wildcard syntax as the S3 key sensor:

- `*` matches any characters except `/`.
- `{a,b,c}` matches any listed alternative.

For example:

```yaml
parameters:
  bucket: my-data-bucket
  object: exports/{orders,customers}-*.parquet
```

The sensor succeeds as soon as any matching object exists.

## Prefix Sensor

Use a prefix sensor when any object below a literal prefix should satisfy the sensor:

```yaml
name: wait_for_gcs_prefix
type: gcs.sensor.prefix
connection: gcs-default
parameters:
  bucket: my-data-bucket
  prefix: incoming/2025-01-01/
```

The legacy type `gcs.sensor.object_sensor_with_prefix` remains supported. The aliases `object_prefix` and `bucket_key` are accepted for `prefix`.

## Sensor Modes

Sensor execution is controlled by the `--sensor-mode` flag:

- `once` (default): Check once and fail when no matching object exists.
- `wait`: Poll until an object exists or the timeout is reached.
- `skip`: Skip sensor execution.

Run a waiting sensor with:

```bash
bruin run path/to/wait_for_gcs_object.asset.yml --sensor-mode wait
```

## GCS for Data Ingestion

GCS connections can also be used as ingestr sources and destinations. The bucket, path, and layout fields required by an ingestion connection are separate from the sensor's asset parameters.
