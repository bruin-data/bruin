# DynamoDB
Amazon [DynamoDB](https://aws.amazon.com/dynamodb/) is a managed NoSQL database service provided by Amazon Web Services (AWS). It supports key-value and document data structures and is designed to handle a wide range of applications requiring scalability and performance. 


Bruin supports DynamoDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from DynamoDB into your data warehouse.

In order to set up DynamoDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up DynamoDB as a data source and run ingestion:

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

* `access_key_id`: Identifes an IAM account.
* `access_token`: Password for the IAM account.
* `region`: AWS region in which your DynamoDB table exists.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/dynamodb.html#setting-up-a-dynamodb-integration).

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

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for DynamoDB.
- connection: This is the destination connection.
- source_connection: The name of the DynamoDB connection defined in .bruin.yml.
- source_table: The name of the table in DynamoDB you want to ingest. 

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/dynamodb_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given DynamoDB table into your Postgres database.