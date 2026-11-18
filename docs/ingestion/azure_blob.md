# Azure Blob Storage

[Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction) is Microsoft's object storage for unstructured data.

Bruin supports Azure Blob Storage as a source and a destination for [Ingestr assets](/assets/ingestr), so you can load files from Azure into your data warehouse and export tables to Azure as parquet files.

In order to set up the Azure Blob Storage connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. For details on the supported URI options, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/azure-blob.html).

## Connection

Add an `azure_blob` connection to the connections section of the `.bruin.yml` file:

```yaml
    connections:
      azure_blob:
        - name: "my-azure-blob"
          account_name: "myaccount"
          # Service principal credentials, recommended for production.
          tenant_id: "tenant-id"
          client_id: "client-id"
          client_secret: "client-secret"
          # Alternatives:
          # connection_string: "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net"
          # account_key: "storage-account-key"
          # sas_token: "sv=..."
          # Destination only:
          # container_name: "lake"
          # path_to_file: "exports"
          # layout: "{table_name}/{load_id}.{file_id}.{ext}"
```

- `account_name`: The Azure storage account name. Required unless `connection_string` is set.
- `tenant_id`, `client_id`, and `client_secret`: Microsoft Entra service principal credentials.
- `connection_string`: An Azure Storage connection string. It sets the account and endpoint, so it also works for sovereign clouds and the Azurite emulator.
- `account_key`: The storage account key.
- `sas_token`: A Shared Access Signature token, with or without a leading `?`.
- `container_name`: Destination only. The container to write into. It must already exist.
- `path_to_file`: Destination only. A folder inside `container_name` to write under. Requires `container_name`.
- `layout`: Destination only. The template for output file names.

Each sign-in method needs these fields:

| Sign-in method | Fields |
|---|---|
| Service principal | `account_name`, `tenant_id`, `client_id`, `client_secret` |
| Connection string | `connection_string` only |
| Account key | `account_name`, `account_key` |
| SAS token | `account_name`, `sas_token` |
| Default Azure login | `account_name` only |

When several are set, ingestr uses the first one in this order: connection string, account key, SAS token, service principal, then default Azure login. The default Azure login uses `DefaultAzureCredential`, which supports environment variables, managed identity, Azure CLI login, and other credentials supported by the Azure SDK.

Bruin encodes these fields when it builds the ingestr URI, so provide the raw credential values in `.bruin.yml`.

## Reading data from Azure Blob Storage

Grant the service principal the `Storage Blob Data Reader` role on the storage account or on the container you read from.

Create an asset file, for example `assets/azure_blob_ingestion.asset.yml`:

```yaml
name: public.users
type: ingestr
connection: postgres

parameters:
  source_connection: my-azure-blob
  source_table: 'exports/users/*.csv'

  destination: postgres
```

- `connection`: The destination connection, where the data is stored.
- `source_connection`: The name of the `azure_blob` connection defined in `.bruin.yml`.
- `source_table`: The container and a file path or glob pattern, separated by a forward slash (`/`).

Then run the asset:

```bash
bruin run assets/azure_blob_ingestion.asset.yml
```

Bruin can read CSV, JSONL or NDJSON, and parquet files, including gzip-compressed ones. It picks the decoder from the file extension. If your file names don't have the right extension, set the `file_type` parameter:

```yaml
name: cloudflare.http_requests
type: ingestr
connection: postgres

parameters:
  source_connection: my-azure-blob
  source_table: 'cloudflare-logs/http_requests/**/*.log.gz'
  file_type: jsonl # [!code ++]
  destination: postgres
```

## Writing data to Azure Blob Storage

Set `container_name`, and optionally `path_to_file`, on the connection:

```yaml
    connections:
      azure_blob:
        - name: "my-azure-blob-destination"
          account_name: "myaccount"
          tenant_id: "tenant-id"
          client_id: "client-id"
          client_secret: "client-secret"
          container_name: "lake"
          path_to_file: "exports"
```

Then create an asset that writes to it:

```yaml
name: public.users
type: ingestr
connection: my-azure-blob-destination

parameters:
  source_connection: postgres
  source_table: 'public.users'

  destination: azure_blob
```

- `connection`: The name of the `azure_blob` connection to write to. Bruin can't infer it from `destination`, so it must be set.
- `destination`: Set to `azure_blob`.

The data is written as parquet files under a folder named after the asset:

```plaintext
lake/
└── exports
    └── public.users
        └── <load_id>.<file_id>.parquet
```

To change the file names, set `layout` on the connection. For example, `layout: "{table_name}.{ext}"` writes a single `lake/exports/users.parquet` file and overwrites it on every run. See the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/azure-blob.html#file-layout) for the available placeholders.

The destination supports the `replace` and `append` strategies. `replace` removes the parquet files from the previous run that match the layout once the new files are written; other files are kept.

Writing needs the `Storage Blob Data Contributor` role on the storage account or the container. With a SAS token, `append` needs write (`w`) or create (`c`) permission, and `replace` also needs list (`l`) and delete (`d`).
