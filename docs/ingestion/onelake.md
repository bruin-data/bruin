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
          # Optional: the lakehouse's SQL analytics endpoint, required for quality checks
          # sql_endpoint: "xxxx.datawarehouse.fabric.microsoft.com"
```

- `workspace_name`: The Fabric workspace name or GUID.
- `lakehouse_name`: The lakehouse name or GUID (the `.Lakehouse` suffix is added automatically).
- `sql_endpoint`: Optional. The host of the lakehouse's SQL analytics endpoint, found under the lakehouse's SQL analytics endpoint settings in Fabric. Required to run [quality checks](#quality-checks) on the asset.

OneLake requires Microsoft Entra ID authentication — shared account keys are not accepted. Provide **one** of the following authentication methods:

- **Service principal**: `tenant_id`, `client_id`, and `client_secret`. The service principal needs Contributor access to the workspace.
- **SAS token**: `sas_token`.
- **DefaultAzureCredential**: set `use_azure_default_credential: true` to let ingestr authenticate via environment variables, a managed identity, or the Azure CLI login.

### Step 2: Create an asset file for data ingestion

To ingest data to OneLake, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `stripe_onelake.yml`) inside the assets folder and add the following content:

```yaml
name: Tables/events
type: ingestr
connection: my-onelake

parameters:
  source_connection: stripe
  source_table: 'event'

  destination: onelake
```

- `name`: The name of the asset. This is also used as the destination table (`--dest-table`) in the lakehouse — see [Storage modes](#storage-modes) below.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. Here `my-onelake` points at the OneLake connection defined in `.bruin.yml`.
- `source_connection`: The name of the source connection defined in `.bruin.yml`.
- `source_table`: The table to ingest from the source.
- `destination`: Set to `onelake`.

### Storage modes

The asset name is passed to ingestr as the destination table, and its prefix determines how data is stored in the lakehouse:

- `Tables/<name>` → a Delta Lake table that is queryable in Fabric.
- `Files/<path>` → raw Parquet files.
- A bare name (e.g. `events`) defaults to `Tables/`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/stripe_onelake.yml
```

As a result of this command, Bruin will ingest data from the given source into your Microsoft OneLake lakehouse.

## Quality checks

Column checks and custom checks on OneLake assets run through the lakehouse's SQL analytics endpoint, so set `sql_endpoint` on the connection. It uses the same service principal or DefaultAzureCredential authentication as the connection; SAS tokens cannot query the SQL endpoint.

Column checks query the table by the asset name, so name the asset `schema.table` (e.g. `silver.orders`) to use them. Custom checks can query any table the endpoint can see.

```yaml
name: silver.orders
type: ingestr
connection: my-onelake

parameters:
  source_connection: stripe
  source_table: 'event'
  destination: onelake

columns:
  - name: id
    checks:
      - name: not_null

custom_checks:
  - name: has rows
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM silver.orders
    value: 1
```

The SQL analytics endpoint can take a short while to reflect newly written data, so checks that run right after ingestion may briefly see the previous state of the table.
