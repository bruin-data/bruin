# Google Cloud Storage 

[Google Cloud Storage](https://cloud.google.com/storage?hl=en) (GCS) is an online file storage web service for storing and accessing data on Google Cloud Platform infrastructure. The service combines the performance and scalability of Google's cloud with advanced security and sharing capabilities. It is an Infrastructure as a Service (IaaS), comparable to Amazon S3. 


Bruin supports GCS as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from GCS into your data warehouse.

In order to set up the GCS connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You will need the `service_account_file` or `service_account_json`. For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#setting-up-a-gcs-integration).

Follow the steps below to correctly set up GCS as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to GCS, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      gcs:
          # name of your connection
        - name: "my-gcs"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself

### Step 2: Create an asset file for data ingestion

To ingest data from GCS, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gcs_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gcs
type: ingestr
connection: postgres

parameters:
  source_connection: my-gcs
  source_table: 'my-bucket/students_details.csv'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the gcs connection defined in .bruin.yml.
- `source_table`: bucket name and file path (or [file glob](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#file-pattern)) separated by a forward slash (`/`).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/gcs_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given gcs bucket into your Postgres database.
