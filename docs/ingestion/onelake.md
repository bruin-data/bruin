# Microsoft OneLake

[Microsoft OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview) is the single, unified, logical data lake for Microsoft Fabric. Every Fabric tenant gets one OneLake, and data is organized into workspaces and lakehouses backed by Azure Data Lake Storage Gen2.

Bruin supports OneLake as a destination for [Ingestr assets](/assets/ingestr), so you can move data from any supported source into a Fabric lakehouse.

In order to set up the OneLake connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You will need the workspace and lakehouse names along with credentials for Microsoft Entra ID authentication. For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/onelake.html#microsoft-onelake).

## Writing to OneLake

Follow the steps below to correctly set up OneLake as a destination and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to OneLake, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      onelake:
        - name: "my-onelake"
          workspace_name: "myworkspace"
          lakehouse_name: "mylakehouse"
          # Authentication — provide ONE of the options below.
          # Option 1: Service principal (Microsoft Entra ID)
          tenant_id: "tenant-id"
          client_id: "client-id"
          client_secret: "client-secret"
          # Option 2: SAS token
          # sas_token: "sv=..."
          # Option 3: DefaultAzureCredential (env vars, managed identity, or Azure CLI login)
          # use_azure_default_credential: true
```

- `workspace_name`: The Fabric workspace name or GUID.
- `lakehouse_name`: The lakehouse name or GUID (the `.Lakehouse` suffix is added automatically).

OneLake requires Microsoft Entra ID authentication — shared account keys are not accepted. Provide **one** of the following authentication methods:

- **Service principal**: `tenant_id`, `client_id`, and `client_secret`. The service principal needs Contributor access to the workspace.
- **SAS token**: `sas_token`.
- **DefaultAzureCredential**: set `use_azure_default_credential: true` to let ingestr authenticate via environment variables, a managed identity, or the Azure CLI login.

### Step 2: Create an asset file for data ingestion

To ingest data to OneLake, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `stripe_onelake.yml`) inside the assets folder and add the following content:

```yaml
name: onelake.events
type: ingestr
connection: my-onelake

parameters:
  source_connection: stripe
  source_table: 'event'

  destination: onelake
  destination_table: Tables/events
```

- `name`: The Bruin asset name used for orchestration, lineage, and checks.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. Here `my-onelake` points at the OneLake connection defined in `.bruin.yml`.
- `source_connection`: The name of the source connection defined in `.bruin.yml`.
- `source_table`: The table to ingest from the source.
- `destination`: Set to `onelake`.
- `destination_table`: The OneLake table or file path passed to ingestr as `--dest-table` - see [Storage modes](#storage-modes) below.

### Storage modes

Bruin passes `parameters.destination_table` to ingestr as the destination table. If `destination_table` is not set, Bruin falls back to the asset `name`. The destination prefix determines how data is stored in the lakehouse:

- `Tables/<name>` → a Delta Lake table that is queryable in Fabric.
- `Tables/<schema>/<name>` → a Delta Lake table under a schema-enabled Fabric lakehouse schema.
- `Files/<path>` → raw Parquet files.
- A bare name (e.g. `events`) defaults to `Tables/`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/stripe_onelake.yml
```

As a result of this command, Bruin will ingest data from the given source into your Microsoft OneLake lakehouse.
