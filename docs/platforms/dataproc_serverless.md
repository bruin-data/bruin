# GCP Dataproc Serverless Spark

Google Cloud Dataproc Serverless is a fully managed service that lets you run Apache Spark workloads without the need to manage clusters or servers. It automatically provisions and scales resources based on your workload requirements, making it ideal for batch processing, data transformation, and analytics tasks.

Bruin supports Dataproc Serverless as a data platform. You can use Bruin to integrate your Spark workloads into complex pipelines that use different data technologies, all without leaving your terminal.

## Connection

In order to use Bruin to run Spark jobs in Dataproc Serverless, you need to define a `dataproc_serverless` connection in your `.bruin.yml` file. Here's a sample `.bruin.yml` with the required fields defined.

```yaml
environments:
  default:
    connections:
      dataproc_serverless:

        # name of your connection
      - name: dataproc_serverless-default

        # GCP project ID
        project_id: my-gcp-project

        # Google Cloud region
        region: us-central1

        # GCS bucket path for temporary job files
        workspace: gs://your-bucket/dataproc-workspace/

        # (Optional) service account email for job execution
        execution_role: my-service-account@my-project.iam.gserviceaccount.com

        # (Optional) Subnetwork URI for VPC connectivity
        # Use this when your Dataproc batches need to connect to resources in a specific VPC
        subnetwork_uri: projects/my-host-project/regions/us-central1/subnetworks/my-subnetwork

        # (Optional) Network tags for firewall rules
        network_tags:
          - dataproc
          - spark

        # (Optional) Cloud KMS key for encryption
        kms_key: projects/my-project/locations/us-central1/keyRings/my-ring/cryptoKeys/my-key

        # (Optional) GCS bucket for staging files (bucket name only, not gs:// URI)
        staging_bucket: my-staging-bucket

        # (Optional) Dataproc Metastore service for Hive/Spark SQL tables
        metastore_service: projects/my-project/locations/us-central1/services/my-metastore

        # Authentication (one of the following):
        # Option 1: Use Application Default Credentials (ADC)
        use_application_default_credentials: true

        # Option 2: Inline service account JSON
        service_account_json: |
          {
            "type": "service_account",
            ...
          }

        # Option 3: Path to service account JSON file
        service_account_file: /path/to/service-account.json
```

### Authentication

Dataproc Serverless supports three authentication methods:

1. **Application Default Credentials** (`use_application_default_credentials`): Uses the default credentials from the environment. This is the recommended approach when running on GCP or when you have configured `gcloud auth application-default login`.
2. **Service Account JSON** (`service_account_json`): Inline JSON credentials for the service account.
3. **Service Account File** (`service_account_file`): Path to a JSON file containing the service account credentials.

At least one of these must be provided for authentication.

## Dataproc Serverless Assets

Bruin supports PySpark assets where Bruin takes care of delivering the code to the cluster and managing execution.

### `dataproc_serverless.pyspark`

A fully managed option where Bruin takes care of job setup, configuration, and execution. You only need to define the workload logic.

* Supports PySpark scripts.
* Handles artifact deployment to GCS.
* Automatic log streaming via Cloud Logging.
* Concurrent-safe by default.
* Bundles internal dependencies and configures your job to use them.

#### Example: Standalone script

```bruin-python
""" @bruin
name: pyspark_job
type: dataproc_serverless.pyspark
connection: dataproc_serverless-default
@bruin """

from pyspark.sql import SparkSession

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("jobName").getOrCreate()
  run_workload(spark_session)
  spark_session.stop()

def run_workload(session):
  """
    crunch some numbers
  """
  ...

```

This defines a PySpark asset that will be executed by the Dataproc Serverless batch defined by the connection named `dataproc_serverless-default`.

#### Example: Multi-module script

Advanced Spark users often package core logic into reusable libraries to improve consistency, reduce duplication, and streamline development across jobs. This approach ensures that shared transformations, validations, and business rules are implemented once and reused reliably.

Bruin has seamless support for PySpark modules.

For this example, let's assume this is how your Bruin pipeline is structured:
```
acme_pipeline/
├── assets
│   └── main.py
├── lib
│   └── core.py
└── pipeline.yml
```

Let's say that `acme_pipeline/lib/core.py` stores some common routines used throughout your jobs. For this example, we'll create a function called `sanitize` that takes in a Spark DataFrame and sanitizes its columns (a common operation in Data Analytics).

::: code-group
```python [acme_pipeline/lib/core.py]
from pyspark.sql import DataFrame

def sanitize(df: DataFrame):
  """
  sanitize a dataframe
  """
  ...
```
:::

You can now import this package in your PySpark assets.
::: code-group
```bruin-python [acme_pipeline/assets/main.py]
""" @bruin
name: raw.transaction
type: dataproc_serverless.pyspark
connection: dataproc_serverless-default
@bruin """

from acme_pipeline.lib.core import sanitize
from pyspark.sql import SparkSession

if __name__ == "__main__":
  session = SparkSession.builder.appName("raw.transaction_std").getOrCreate()
  src = session.sparkContext.textFile("gs://acme/data/transactions").toDF()
  sanitize(src)
  session.stop()

```
:::

Bruin internally sets the [`PYTHONPATH`](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH) to the root of your pipeline. So you'll always have to use the fully qualified package name to import any internal packages.

#### Workspace

PySpark assets require `workspace` to be configured in your `dataproc_serverless` connection. Workspace is a GCS path that is used by Bruin as working storage for jobs that run on Dataproc Serverless.

Bruin uses this GCS path for:
* Staging your entrypoint file.
* Uploading bundled dependencies (context.zip).

The workspace is automatically cleaned up after job completion.

## Variables

Both built-in variables (e.g., `BRUIN_START_DATE`, `BRUIN_RUN_ID`) and any user-defined variables (from your `pipeline.yml`) are accessible directly as environment variables within the execution environment of your PySpark jobs.

For `dataproc_serverless` assets, these environment variables can be accessed using `os.environ` in your PySpark scripts, similar to regular Python assets.

Refer to the [Python assets documentation](/assets/python.md#environment-variables) for more information.

## Asset Schema

Here's the full schema of the `dataproc_serverless.pyspark` asset along with a brief explanation:

```yaml
# required
name: spark_job_example

# required
type: dataproc_serverless.pyspark

# optional, defaults to dataproc_serverless-default
connection: connection-name-example

# job specific configuration
parameters:

  # Spark runtime version (optional, defaults to "2.2")
  runtime_version: "2.2"

  # args to pass to the entrypoint (optional)
  args: arg1 arg2

  # spark configuration (optional)
  config: --conf spark.executor.cores=2 --conf spark.executor.memory=4g

  # timeout for the job (optional)
  # Uses Go duration format: 1h, 30m, 1h30m, etc.
  timeout: 1h
```

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `runtime_version` | No | `2.2` | Dataproc Serverless Spark runtime version |
| `args` | No | - | Space-separated arguments passed to the PySpark script |
| `config` | No | - | Spark configuration in `--conf key=value` format |
| `timeout` | No | - | Job timeout using Go duration format (e.g., `1h`, `30m`) |

### Connection Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique identifier for the connection |
| `project_id` | Yes | GCP project ID where Dataproc jobs will run |
| `region` | Yes | Google Cloud region (e.g., `us-central1`, `europe-west1`) |
| `workspace` | Yes | GCS path for temporary job files (e.g., `gs://bucket/prefix/`) |
| `execution_role` | No | Service account email for job execution |
| `subnetwork_uri` | No | Subnetwork URI for VPC connectivity (e.g., `projects/host-project/regions/region/subnetworks/subnet`) |
| `network_tags` | No | List of network tags for firewall rules |
| `kms_key` | No | Cloud KMS key resource name for encryption |
| `staging_bucket` | No | GCS bucket name (not URI) for staging files |
| `metastore_service` | No | Dataproc Metastore service resource name for Hive/Spark SQL tables |
| `use_application_default_credentials` | No* | Set to `true` to use Application Default Credentials |
| `service_account_json` | No* | Inline service account JSON credentials |
| `service_account_file` | No* | Path to service account JSON file |

\* At least one of `use_application_default_credentials`, `service_account_json`, or `service_account_file` must be provided.

## IAM Permissions

The service account used for authentication requires the following IAM roles:

- **Dataproc Editor** (`roles/dataproc.editor`): To create and manage batch jobs
- **Storage Object Admin** (`roles/storage.objectAdmin`): To upload files to the workspace bucket
- **Logs Viewer** (`roles/logging.viewer`): To stream job logs

Additional roles may be required depending on optional features:

- If using `kms_key`: **Cloud KMS CryptoKey Encrypter/Decrypter** (`roles/cloudkms.cryptoKeyEncrypterDecrypter`)
- If using `metastore_service`: **Dataproc Metastore Editor** (`roles/metastore.editor`)

If using `execution_role`, that service account needs appropriate permissions to access data sources and destinations used by your Spark jobs.

For more details on Dataproc Serverless, see [Google Cloud Documentation][dataproc-serverless].

[dataproc-serverless]: https://cloud.google.com/dataproc-serverless/docs
