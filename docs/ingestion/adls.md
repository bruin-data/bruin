# Azure Data Lake Storage Gen2

[Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) is Azure Blob Storage with hierarchical namespace capabilities enabled for data lake workloads.

Bruin supports ADLS Gen2 as a source for [Ingestr assets](/assets/ingestr), so you can load files from Azure storage into your data warehouse.

In order to set up the ADLS connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. At minimum, you need the storage account name. If you omit explicit credentials, ingestr uses Azure's default credential chain. For details on the supported URI options, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/adls.html).

## Reading data from ADLS Gen2

Follow the steps below to correctly set up ADLS Gen2 as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to ADLS Gen2, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      adls:
        - name: "my-adls"
          account_name: "myaccount"
          # Optional service principal credentials.
          tenant_id: "tenant-id"
          client_id: "client-id"
          client_secret: "client-secret"
          # Optional alternatives for demos or compatibility.
          # account_key: "storage-account-key"
          # sas_token: "sv=..."
          # Optional destination-only layout template.
          # layout: "{table_name}/{load_id}.{file_id}.{ext}"
```

- `account_name`: The Azure storage account name.
- `tenant_id`, `client_id`, and `client_secret`: Optional Microsoft Entra service principal credentials.
- `account_key`: Optional Azure storage account key.
- `sas_token`: Optional Shared Access Signature token.
- `layout`: Optional destination-only layout template.

For production, prefer Microsoft Entra service principal authentication. Grant the service principal an Azure RBAC role on the storage account or file system, such as `Storage Blob Data Reader` for source reads.

When `tenant_id`, `client_id`, `client_secret`, `account_key`, and `sas_token` are omitted, ingestr uses `DefaultAzureCredential`. This supports environment variables, managed identity, Azure CLI login, and other credentials supported by the Azure SDK.

Bruin encodes these fields when it builds the ingestr URI, so provide the raw credential values in `.bruin.yml`.

### Step 2: Create an asset file for data ingestion

To ingest data from ADLS Gen2, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `adls_ingestion.yml`) inside the assets folder and add the following content:

```yaml
name: public.adls_users
type: ingestr
connection: postgres

parameters:
  source_connection: my-adls
  source_table: 'lakehouse/exports/users/*.csv'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the `adls` connection defined in `.bruin.yml`.
- `source_table`: The ADLS Gen2 file system and file path, or file glob, separated by a forward slash (`/`).

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/adls_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given ADLS Gen2 path into your destination database.

### File type hints

Bruin can read these file types from ADLS Gen2:

- CSV
- JSONL or NDJSON
- Parquet

Bruin will check the file extension to determine the right decoder to use. If your file names are missing the correct extension, then you can explicitly tell Bruin to use a specific decoder using the `file_type` parameter.

For example:

```yaml
name: public.fees
type: ingestr
connection: postgres

parameters:
  source_connection: my-adls
  source_table: 'lakehouse/records/fees.log'
  file_type: csv # [!code ++]
  destination: postgres
```

This asset will load the contents of `fees.log`, treating it as if it were a CSV file.

### Working with compressed files

Bruin automatically detects and handles gzipped files in ADLS Gen2. You can load data from compressed files with the `.gz` extension without any additional configuration.
