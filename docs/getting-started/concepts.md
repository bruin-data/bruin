# Concepts

Bruin has a few simple concepts that enable you to make the most out of it.

## Asset
Anything that carries value that is derived from data is an asset from our perspective.

In more concrete terms, an asset can be:
- a table/view in your database
- a file in S3 / GCS
- a machine learning model
- an MS Excel/Google Sheets/Airbyte/Notion document

This abstraction enables building multi-language data pipelines that are agnostic of a database/destinations.
You will primarily be interacting with assets when using Bruin.

Here's an example SQL asset:

```bruinsql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

depends:
   - hello_python

materialization:
   type: table

columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

@bruin */

select 1 as one
union all
select 2 as one
```
### Building Blocks
An asset consists of two primary components:
- definition: the metadata that enables Bruin to learn about the asset and its metadata
- content: the actual query/logic that creates the asset

The details on the asset definition can be seen [here](../assets/definition-schema.md).

## Pipeline

A pipeline is a group of assets that are executed together in the right order. 
For instance, if you have an asset that ingests data from an API, and another one that creates another table from the ingested data, you have a pipeline.
Asset executions occur on a pipeline level.

A pipeline is defined with a `pipeline.yml` file, and all the assets need to be under a folder called `assets` next to this file:
```
- my-pipeline/
  ├─ pipeline.yml
  └─ assets/
    ├─ asset1.sql
    └─ asset2.py
```

Here's an example `pipeline.yml`:
```yaml
name: bruin-init
schedule: daily # relevant for Bruin Cloud deployments

default_connections:
  google_cloud_platform: "gcp"
  snowflake: "snowflake"
```

## Pipeline Run
When you run a pipeline, you create a "pipeline run". A pipeline run contains one or more asset instances that are executed in a given time with a specific configuration.

You can run a pipeline in the folder `my-pipeline` with the following command:
```shell
bruin run my-pipeline
```

## Asset Instance
An asset instance is a single execution of an asset at a given time. 
For instance, if you have a Python asset and you run it, Bruin creates an asset instance that executes your code.

Asset instance is an internal concept, although it is relevant to understand since actual executions are based on asset instances.

You can run an asset with the following command:
```shell
bruin run /path/to/the/asset/file.sql
```

## Connection
A connection is a set of credentials that enable Bruin to communicate with an external platform. 

Bruin currently supports the following connection types:
- Google Cloud Platform
- Snowflake
- Postgres
- AWS Redshift
- Generic

Platform specific connections have specific schemas, and "generic" connections are built as key-value pairs to inject secrets into your assets from outside. 

Connections are defined in the `.bruin.yml` file locally. A connection has a name and the credentials.

When you run a pipeline, Bruin will find this file in the repo root, parse the connections there, build client objects internally to interact with these external platforms and then run your assets.

> [!INFO]
> The first time you run `bruin validate` or `bruin run`, Bruin will create an empty `.bruin.yml` file and add it to `.gitignore` automatically.


## Default Connections
Default connections are top-level defaults that reduces repetition by stating what connections to use on types of assets.
For instance, a pipeline might have SQL queries that run on Google BigQuery or Snowflake, and based on the type of an asset Bruin picks the appropriate connection.

## Sensors
Sensors are a special type of assets that are used to wait on certain external signals. Sensors are useful to wait on external signals such as a table being created in an external database, or a file being uploaded to S3. A common usecase for sensors is when there are datasets/files/tables that are created by a separate process and you need to wait for them to be created before running your assets.