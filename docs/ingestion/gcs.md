# Google Cloud Storage 

[Google Cloud Storage](https://cloud.google.com/storage?hl=en) (GCS) is an online file storage web service for storing and accessing data on Google Cloud Platform infrastructure. The service combines the performance and scalability of Google's cloud with advanced security and sharing capabilities. It is an Infrastructure as a Service (IaaS), comparable to Amazon S3. 


Bruin supports GCS as a source and a destination for [Ingestr assets](/assets/ingestr), and you can use it to move data to and from your data warehouse.

In order to set up the GCS connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You will need the `service_account_file` or `service_account_json`. For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#setting-up-a-gcs-integration).

## Reading data from GCS

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

### File type hints
Bruin can read 3 types of files from GCS:
* CSV
* JSONL
* Parquet

Bruin will check the file extension to determine the right decoder to use. If you file names are missing the correct extension, then you can explicitly tell Bruin to use a specific decoder using `file_type` parameter.


For example:
```yaml
name: public.fees
type: ingestr
connection: postgres

parameters:
  source_connection: my-gcs
  source_table: 'my-bucket/records/fees.log'
  file_type: csv # [!code ++]
  destination: postgres
```

This asset will load the contents `fees.log`, treating it as if it were a CSV File.

### Working with compressed files.
Bruin automatically detects and handles gzipped files in your GCS bucket. You can load data from compressed files with the `.gz` extension without any additional configuration.

## Writing to a GCS

Follow the steps below to correctly set up GCS as a destination and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to GCS, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      gcs:
        - name: "gcs"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
          bucket_name: "my-org-bucket"
          path_to_file: "records"
          layout: "{table_name}.{ext}" #optional
          
```
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself
- `bucket_name`: The name of the GCS bucket where data will be written.
- `path_to_file`: A base path or prefix within the bucket where files will be stored. Files specified in the asset will be relative to this path
- `layout`: Layout template (optional, destination only). If you would like to create a parquet file with the same name as the source table (as opposed to a folder) you can set layout to {table_name}.{ext}. List of available Layout variables is available [here](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#available-layout-placeholders)

### Step 2: Create an asset file for data ingestion

To ingest data to GCS, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_gcs.yml) inside the assets folder and add the following content:

```yaml
name: public.final
type: ingestr
connection: gcs

parameters:
  source_connection: stripe
  source_table: 'event'

  destination: gcs
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `gcs` indicates that the ingested data will be stored in a GCS database.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/stripe_gcs.yml
```
As a result of this command, Bruin will ingest data from the given Stripe source to your GCS database.