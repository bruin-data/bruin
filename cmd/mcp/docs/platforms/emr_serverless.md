# AWS EMR Serverless Spark
Amazon EMR (Elastic MapReduce) Serverless is a deployment option for Amazon EMR that provides a serverless runtime environment. This simplifies the operation of analytics applications that use the latest open-source frameworks, such as Apache Spark and Apache Hive. With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications with these frameworks.

Bruin supports EMR Serverless as a data platform. You can use Bruin to integrate your Spark workloads into complex pipelines that use different data technologies, all without leaving your terminal. 

## Connection

In order to use Bruin to run Spark jobs in EMR Serverless, you need to define an `emr_serverless` connection in your `.bruin.yml` file. Here's a sample `.bruin.yml` with the required fields defined.

```yaml 
environments:
  default:
    connections:
      emr_serverless:

        # name of your connection
      - name: emr_serverless-default

        # AWS credentials
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1

        # name of your EMR application
        application_id: EMR_APPLICATION_ID

        # role assumed by your job. This determines
        # what AWS resources your spark job can access.
        execution_role: IAM_ROLE_ARN

        # (Python assets only)
        # declares working area used by pyspark jobs.
        workspace: s3://your-bucket/optional-prefix/

```


## Logs
Bruin supports log streaming for spark jobs. This is only supported for spark logs stored in [S3](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/logging.html#jobs-log-storage-s3-buckets). Both `DRIVER` and `EXECUTOR` logs are streamed by default.

In order to stream logs, one of the following conditions must be met:
* Your EMR Serverless Application is pre-configured with an S3 Log Storage Location. 
* `parameters.logs` must be defined

> [!NOTE]
> Python assets stream logs out-of-the-box. You don't need to specify `parameters.logs` for them.

## EMR Serverless Assets

Bruin supports two different ways of defining a Spark asset:
- what we call a "managed" PySpark asset where Bruin takes care of delivering the code to the cluster as well
- as an external asset defined with YAML where Bruin simply orchestrates

### `emr_serverless.pyspark`
A fully managed option where Bruin takes care of job setup, configuration, and execution. You only need to define the workload logic.

* Supports only PySpark scripts.
* Handles artifact deployment.
* Automatic log configuration.
* Concurrent-safe by default.
* Bundles internal dependencies and configures your job to use them.

#### Example: Standalone script
```bruin-python
""" @bruin
name: pyspark_job
type: emr_serverless.pyspark
connection: app_staging
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

This defines a pyspark asset that will be executed by the EMR Serverless Application defined by the connection named `app_staging`.

The `run_workload` function is there for demonstration. You can structure your pyspark scripts however you like.

#### Example: Multi-module script

Advanced Spark users often package core logic into reusable libraries to improve consistency, reduce duplication, and streamline development across jobs. This approach ensures that shared transformations, validations, and business rules are implemented once and reused reliably.

Bruin has seamless support for pyspark modules.

For this example, let's assume this is how your Bruin pipeline is structured:
```
acme_pipeline/
├── assets
│   └── main.py
├── lib
│   └── core.py
└── pipeline.yml
```

Let's say that `acme_pipeline/lib/core.py` stores some common routines used throughout your jobs. For this example, we'll create a function called `sanitize` that takes in a Spark DataFrame and sanitize its columns (A common operation in Data Analytics).

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
type: emr_serverless.pyspark
connection: app_staging
@bruin """

from acme_pipeline.lib.core import sanitize
from pyspark.sql import SparkSession

if __name__ == "__main__":
  session = SparkSession.builder.appName("raw.transaction_std").getOrCreate()
  src = session.sparkContext.textFile("s3://acme/data/transactions").toDF()
  sanitize(src)
  session.stop()

```
:::

Bruin internally sets the [`PYTHONPATH`](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH) to the root of your pipeline. So you'll always have to use the fully qualified package name to import any internal packages. 

#### Workspace
Python assets require `workspace` to be configured in your `emr_serverless` connection. Workspace is a S3 path that is used by Bruin as working storage for jobs that run on `emr_serverless`.

Bruin uses this S3 path for:
* Storing Logs.
* Staging your entrypoint file.
* Uploading bundled dependencies.

![workspace diagram](media/pyspark-workspace.svg)

### `emr_serverless.spark`
A lightweight option that only supports triggering a job. 

* Supports both PySpark scripts and JARs.
* Users are responsible for:
  * deploying their artifacts
  * managing internal dependencies

Choose the format that best fits your use case—use YAML when you want to integrate with pre-existing infrastructure, or use Python for a streamlined, fully-managed experience.


#### Example

```yaml
name: spark_example_job
type: emr_serverless.spark
connection: app_staging
parameters:
  entrypoint: s3://amzn-test-bucket/src/script.py
  config: --conf spark.executor.cores=1
```

This defines an asset that runs a spark job on an EMR Serverless Application defined by the connection named `app_staging`. The script at `s3://amzn-test-bucket/src/script.py` is configured as the [entrypoint](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html#spark-params) of the job.

> [!note]
> YAML and Python assets require different `type` parameter.
> * YAML-style assets: `emr_serverless.spark` 
> * Python assets:  `emr_serverless.pyspark`.

## Quality Checks
[Quality checks](/quality/overview.md) for EMR Serverless are powered via [AWS Athena](/platforms/athena.md). 

> [!WARNING]
> Bruin currently requires a few extra steps in order to be able to run quality checks for your EMR Serverless Assets.
> Future versions of Bruin will automate this process for you. 

### Prerequisites
* Configure an [athena connection](/platforms/athena.html#connection) in your `bruin.yml`.
* Set `parameters.athena_connection` to the name of your Athena connection.
* Create an `athena` table on top of your data with the same name as your Asset's name.

### Example

To demonstrate quality checks, we're going to write a simple pyspark script that writes static data as CSV to a bucket in S3.

#### Initial Setup

We're going to start by creating a pipeline called `quality-checks-example`. We first run `bruin init` to create the skeleton structure.
```sh
bruin init default quality-checks-example
```

Now we'll add the pyspark asset.
::: code-group
```bruin-python [quality-checks-example/assets/users.py]
""" @bruin
name: users
type: emr_serverless.pyspark
@bruin """

from pyspark.sql import SparkSession

SCHEMA = ["id", "name", "age"]
USERS = [
  (1, "Alice", 29),
  (2, "Bob", 31),
  (3, "Cathy", 25),
  (4, "David", 35),
  (5, "Eva", 28),
]

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("users").getOrCreate()
  df = spark.createDataFrame(USERS, SCHEMA)
  df.write.csv("s3://acme/user/list", mode="overwrite")
  spark.stop()
```
:::

Next let's setup the `bruin.yml` file with the credentials necessary to run our job.
::: code-group
```yaml [bruin.yml]
environments:
  default:
    connections:
      emr_serverless:
      - name: emr_serverless-default
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1
        application_id: EMR_APPLICATION_ID
        execution_role: IAM_ROLE_ARN
        workspace: s3://acme/bruin-pyspark-workspace/
```
:::

We can now run the pipeline to verify that it works
```sh
bruin run ./quality-checks-example
```

::: info Output
<pre><b>Analyzed the pipeline &apos;quality-checks-example&apos; with 1 assets.</b>

<span style="color:#3465A4"><b>Pipeline: quality-checks-example</b></span>
<span style="color:#4E9A06">  No issues found</span>

<span style="color:#4E9A06"><b>✓ Successfully validated 1 assets across 1 pipeline, all good.</b></span>

<b>Starting the pipeline execution...</b>
<span style="color:#8D8F8A">[2025-05-11 18:10:00]</span> <span style="color:#C4A000">Running:  users</span>
<span style="color:#8D8F8A">[2025-05-11 18:10:00]</span> <span style="color:#C4A000">... output omitted for brevity ...</span>
<span style="color:#8D8F8A">[2025-05-11 18:10:05]</span> <span style="color:#C4A000">Finished: users</span>

<span style="color:#4E9A06"><b>Executed 1 tasks in 5s</b></span>
</pre>
:::

#### Enabling quality checks

In order to run quality checks, we need to:
1. Create an Athena table in our AWS account.
2. Configure an `athena` connection in our `bruin.yml` file.

To start, let's first create our Athena table. Go to your AWS Athena console and run the following DDL Query to create a table over the data our pyspark job created.
```sql
CREATE EXTERNAL TABLE users (id int, name string, age int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://acme/user/list' 
```

> [!TIP]
> For more information on creating Athena tables, see [Create tables in Athena](https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html) and [Use SerDe](https://docs.aws.amazon.com/athena/latest/ug/serde-reference.html) in the AWS Athena Documentation.

Next, update your `bruin.yml` file with an athena connection.

```yaml [bruin.yml]
environments:
  default:
    connections:
      emr_serverless:
      - name: emr_serverless-default
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1
        application_id: EMR_APPLICATION_ID
        execution_role: IAM_ROLE_ARN
        workspace: s3://acme/bruin-pyspark-workspace/
      athena:                                           # [!code ++]
      - name: quality-tests                             # [!code ++]
        access_key_id: AWS_ACCESS_KEY_ID                # [!code ++]
        secret_access_key: AWS_SECRET_ACCESS_KEY        # [!code ++]
        region: eu-north-1                              # [!code ++]
        query_results_path: "s3://acme/athena-output/"  # [!code ++]

```

Now we can update our assets to define some quality checks. For this example, we're going to add one column and one custom check.

::: code-group
```bruin-python [quality-checks-example/assets/users.py]
""" @bruin
name: users
type: emr_serverless.pyspark
columns:              # [!code ++]
  - name: id          # [!code ++]
    type: integer     # [!code ++]
    checks:           # [!code ++]
      - name: non_negative  # [!code ++]
custom_checks: # [!code ++] 
  - name: users are adults # [!code ++]
    query: SELECT count(*) from users where age < 18 # [!code ++]
    value: 0 # [!code ++]
parameters: # [!code ++]
  athena_connection: quality-tests # [!code ++]
@bruin """

from pyspark.sql import SparkSession

SCHEMA = ["id", "name", "age"]
USERS = [
  (1, "Alice", 29),
  (2, "Bob", 31),
  (3, "Cathy", 25),
  (4, "David", 35),
  (5, "Eva", 28),
]

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("users").getOrCreate()
  df = spark.createDataFrame(USERS, SCHEMA)
  df.write.csv("s3://acme/user/list", mode="overwrite")
  spark.stop()
```
:::

::: tip
If all your assets share the same `type` and `parameters.athena_connection`, you can set them as [defaults](/getting-started/concepts.html#defaults) in your `pipeline.yml` to avoid repeating them for each asset.


```yaml 
name: my-pipeline
default:
  type: emr_serverless.pyspark
  parameters:
    athena_connection: quality-checks
```
:::
Now when we run our Bruin pipeline again, our quality checks should run after our Asset run finishes.
```sh
bruin run ./quality-checks-example
```

::: info Output
<pre><b>Analyzed the pipeline &apos;quality-checks-example&apos; with 1 assets.</b>

<span style="color:#3465A4"><b>Pipeline: quality-checks-example</b></span>
<span style="color:#4E9A06">  No issues found</span>

<span style="color:#4E9A06"><b>✓ Successfully validated 1 assets across 1 pipeline, all good.</b></span>

<b>Starting the pipeline execution...</b>
<span style="color:#8D8F8A">[2025-05-11 18:20:36]</span> <span style="color:#3465A4">Running:  users</span>
<span style="color:#8D8F8A">[2025-05-11 18:20:36]</span> <span style="color:#3465A4">... output omitted for brevity ...</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:01]</span> <span style="color:#3465A4">Finished: users</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:02]</span> <span style="color:#75507B">Running:  users:id:non_negative</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:02]</span> <span style="color:#C4A000">Running:  users:custom-check:users_are_adults</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:06]</span> <span style="color:#75507B">Finished: users:id:non_negative</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:06]</span> <span style="color:#C4A000">Finished: users:custom-check:users_are_adults</span>

<span style="color:#4E9A06"><b>Executed 3 tasks in 30s</b></span>
</pre>
:::

## Variables

Both built-in variables (e.g., `BRUIN_START_DATE`, `BRUIN_RUN_ID`) and any user-defined variables (from your `pipeline.yml`) are accessible directly as environment variables within the execution environment of your PySpark jobs.

For `emr_serverless` assets, these environment variables can be accessed using `os.environ` in your PySpark scripts, similar to regular Python assets.

Refer to the [Python assets documentation](/assets/python.md#environment-variables) for more information.

::: tip
These variables are available in both `pyspark` and `spark` assets. So you can leverage the power of variables regardless of which asset kind you utilize.
:::
## Asset Schema

Here's the full schema of the `emr_serverless.spark` asset along with a brief explanation:

```yaml
# required
name: spark_submit_test

# required, should be one of 
#   - emr_serverless.spark    (yaml)
#   - emr_serverless.pyspark  (python)
type: emr_serverless.spark 

# optional, defaults to emr_serverless-default
connection: connection-name-example  

# job specific configuration
parameters:

  # path of the pyspark script or jar to run (required) [yaml only]
  entrypoint: s3://amzn-test-bucket/src/script.py   

  # path where logs are stored or should be stored (optional)
  logs: s3://amzn-test-bucket/logs

  # name of your athena connection (optional, defaults to "athena-default")
  # used for quality checks
  athena_connection: athena-conn

  # args to pass to the entrypoint (optional)
  args: arg1 arg2

  # spark configuration (optional)
  config: --conf spark.executor.cores=1

  # timeout for the job, defaults to 0 which means no time limit (optional)
  timeout: 10m
```

For more details on EMR Serverless applications, see [AWS Documentation][emr-app]


[emr-app]: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html