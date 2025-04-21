# AWS EMR Serverless Spark
Amazon EMR (Elastic MapReduce) Serverless is a deployment option for Amazon EMR that provides a serverless runtime environment. This simplifies the operation of analytics applications that use the latest open-source frameworks, such as Apache Spark and Apache Hive. With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications with these frameworks.

Bruin supports EMR Serverless as a data platform. You can use bruin to integrate your spark workloads into complex pipelines that use different data technologies, all without leaving your terminal. 

## Connection

In order to use bruin to run spark jobs in EMR Serverless, you need to define an `emr_serverless` connection in your `.bruin.yml` file. Here's a sample `.bruin.yml` with the required fields defined.

```yaml 
environments:
  default:
    connections:
      emr_serverless:
      - name: emr_serverless-default
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        application_id: EMR_APPLICATION_ID
        execution_role: IAM_ROLE_ARN
        region: eu-north-1

```

## Logs
Bruin supports log streaming for spark jobs. This is only supported for spark logs stored in [S3](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/logging.html#jobs-log-storage-s3-buckets). Both `DRIVER` and `EXECUTOR` logs are streamed by default.

In order to stream logs, one of the following conditions must be met:
* Your EMR Serverless Application is pre-configured with an S3 Log Storage Location. 
* `parameters.logs` or `parameters.workspace` is be specified and points to an S3 URI.

## EMR Serverless Spark Asset

After adding the `aws` connection to your `.bruin.yml` file, you need to create an asset configuration file. This file defines the configuration required for triggering your spark workloads. Here's an example:
```yaml
name: spark_example_job
type: emr_serverless.spark
parameters:
  entrypoint: s3://amzn-test-bucket/src/script.py
  config: --conf spark.executor.cores=1
  application_id: emr_app_123
  execution_role: arn:aws:iam::account_id_1:role/execution_role
  region: ap-south-1
```

This defines an asset that runs a spark job on `emr_app_123` [EMR Serverless Application](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html) that is defined by the script at `s3://amzn-test-bucket/src/script.py`. The `arn:aws:iam::account_id_1:role/execution_role` defines the AWS permissions that are available to your spark job. 



## Asset Schema

Here's the full schema of the `emr_serverless.spark` asset along with a brief explanation:
```yaml
name: spark_submit_test
type: emr_serverless.spark
connection: local-dev # optional, defaults to emr_serverless-default

parameters:

  # path of the pyspark script or jar to run (required)
  entrypoint: s3://amzn-test-bucket/src/script.py   

  # path where logs are stored or should be stored (optional)
  logs: s3://amzn-test-bucket/logs

  # args to pass to the entrypoint (optional)
  args: arg1 arg2

  # spark configuration (optional)
  config: --conf spark.executor.cores=1

  # timeout for the job, defaults to 0 which means no time limit (optional)
  timeout: 10m

```