# AWS EMR Serverless Spark
Amazon EMR (Elastic MapReduce) Serverless is a deployment option for Amazon EMR that provides a serverless runtime environment. This simplifies the operation of analytics applications that use the latest open-source frameworks, such as Apache Spark and Apache Hive. With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications with these frameworks.

Bruin supports EMR Serverless as a spark orchestration platform. You can use bruin to integrate your spark workloads into complex pipelines that different data technologies, all without leaving your terminal. 

## Connection

In order to use bruin to trigger spark jobs in EMR Serverless, you need to define an `aws` connection in your `.bruin.yml` file. The connection schema looks like the following:
```yaml
        connections:
            aws:
            - name: aws-connection
              access_key: _YOUR_AWS_ACCESS_KEY_ID_
              secret_key: _YOUR_AWS_SECRET_ACCESS_KEY_
```

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
parameters:

  # path of the pyspark script or jar to run (required)
  entrypoint: s3://amzn-test-bucket/src/script.py   

  # EMR Serverless Application ID (required)
  application_id: emr_app_123

  # Execution Role assigned to the job (required)
  execution_role: arn:aws:iam::account_id_1:role/execution_role

  # AWS Region of the application (required)
  region: ap-south-1

  # args to pass to the entrypoint (optional)
  args: arg1 arg2

  # spark configuration (optional)
  config: --conf spark.executor.cores=1

  # maximum retries (optional, defaults to 1)
  max_attempts: 2

  # timeout for the job, defaults to 0 which means no time limit (optional)
  timeout: 10m

```