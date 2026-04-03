# DynamoDB

Amazon [DynamoDB](https://aws.amazon.com/dynamodb/) is a managed NoSQL database service provided by Amazon Web Services (AWS). It supports key-value and document data structures and is designed to handle a wide range of applications requiring scalability and performance.

Bruin supports DynamoDB both as a source and as a destination for [Ingestr assets](/assets/ingestr). You can use it to ingest data from DynamoDB into your data warehouse, or load data from other sources into DynamoDB.

In order to set up DynamoDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up DynamoDB as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to DynamoDB, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      dynamodb:
        - name: "my-dynamodb"
          access_key_id: "AWS_ACCESS_KEY_ID"
          secret_access_key: "AWS_SECRET_ACCESS_KEY"
          region: "AWS_REGION"
          
```

* `access_key_id`: Identifies an IAM account.
* `secret_access_key`: Password for the IAM account.
* `region`: AWS region in which your DynamoDB table exists.

For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/dynamodb.html#setting-up-a-dynamodb-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from DynamoDB, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., dynamodb_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.dynamodb
type: ingestr
connection: postgres

parameters:
  source_connection: my-dynamodb
  source_table: 'users'
  destination: postgres
```

* name: The name of the asset.
* type: Specifies the type of the asset. It will be always ingestr type for DynamoDB.
* connection: This is the destination connection.
* source_connection: The name of the DynamoDB connection defined in .bruin.yml.
* source_table: The name of the table in DynamoDB you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/dynamodb_integration.asset.yml
```

As a result of this command, Bruin will ingest data from the given DynamoDB table into your Postgres database.

## Using DynamoDB as a Destination

DynamoDB can also be used as a destination to load data from other sources. The supported incremental strategies are `replace`, `append`, and `merge`.

### Example: Loading data into DynamoDB

To use DynamoDB as a destination, create an asset file that specifies DynamoDB as the `destination`:

```yaml
name: dynamodb.my_table
type: ingestr
connection: my-dynamodb

parameters:
  source_connection: postgres
  source_table: 'public.users'

  destination: dynamodb
```

- `connection`: The name of the DynamoDB connection defined in `.bruin.yml`, used as the destination.
- `source_connection`: The name of the source connection (e.g., Postgres).
- `source_table`: The table from the source to ingest.
- `destination`: Set to `dynamodb` to use DynamoDB as the destination.

When you run this asset, Bruin will load data from the source into the specified DynamoDB table.

**Important Notes:**

- DynamoDB requires at least one primary key. You can specify it using the `primary_key` parameter in the asset file.
- The `merge` strategy uses DynamoDB's `PutItem` operation, which naturally handles insert-or-update semantics.
