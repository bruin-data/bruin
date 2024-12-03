# Athena
[Athena](https://aws.amazon.com/athena/) is an interactive query service that allows users to analyze data directly in Amazon S3 using standard SQL.

The Athena destination stores data as Parquet files in S3 buckets and creates external tables in AWS Glue Catalog.

Bruin supports Athena as a destination, and you can use it to ingest data to Athena.

In order to set up Athena connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `query_results_path`, `access_key_id`, `secret_access_key` and `region` to access the S3 bucket. Please follow the guide to obtain [credentials](https://dlthub.com/docs/dlt-ecosystem/destinations/athena#2-setup-bucket-storage-and-athena-credentials). Once you've completed the guide, you should have all the above-mentioned credentials.

Follow the steps below to correctly set up Athena as a destination.
### Step 1: Add a connection to .bruin.yml file

To connect to Athena, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
      connections:
        athena:
          - name: athena
            access_key_id: "access_key_123"
            secret_access_key: "secret_key_123"
            query_results_path: "destbucket"
            region: "eu-central-1"
```
- `access_key_id and secret_access_key`: These are AWS credentials that will be used to authenticate with AWS services like S3 and Athena.
- `region`: The AWS region of the Athena service and S3 buckets, e.g. eu-central-1
- `query_results_path`: The query location path where the results of Athena queries will be saved, e.g. dest_path or s3://dest_path.



### Step 2: Create an asset file

To ingest data to  Athena, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., chess_athena.yml) inside the assets folder and add the following content:

```yaml
name: public.chess
type: ingestr
connection: athena

parameters:
  source_connection: my-chess
  source_table: 'profiles'

  destination: athena
```

- `connection`: This is the destination connection, which defines where the data should be stored.

### Step 3: [Run](/commands/run) asset
```     
bruin run assets/chess_athena.yml
```
As a result of this command, Bruin will copy the data from the Chess source into Athena.
