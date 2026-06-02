# Microsoft Fabric Warehouse

Bruin supports Microsoft Fabric Warehouse through the SQL endpoint (TDS protocol) using the `go-mssqldb` driver. Fabric is case-sensitive for identifiers, so use quoted identifiers or consistent casing in asset names.

## Connection configuration

Add a Fabric Warehouse connection under `fabric`:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      fabric:
        - name: fabric-default
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          use_azure_default_credential: true
          # options: "encrypt=true&TrustServerCertificate=false" # optional
```

### Azure AD (DefaultAzureCredential)

If `use_azure_default_credential: true` is set, the connector uses Azure's DefaultAzureCredential chain. You can authenticate locally with Azure CLI (`az login`).

### Service principal (client secret)

```yaml
environments:
  default:
    connections:
      fabric:
        - name: fabric-sp
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          client_id: "<app id>"
          client_secret: "<secret>"
          tenant_id: "<tenant id>"
```

### SQL authentication

```yaml
environments:
  default:
    connections:
      fabric:
        - name: fabric-sql
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          username: "<username>"
          password: "<password>"
```

> [!NOTE]
> SQL authentication is only available for native Fabric assets (`fabric.sql`, `fabric.seed`, sensors). Using Fabric as an [ingestr](#using-fabric-with-ingestr) source or destination requires Microsoft Entra ID authentication — username/password connections are rejected.

## Asset types

- `fabric.sql`
- `fabric.seed`
- `fabric.sensor.query`
- `fabric.sensor.table`

## Example asset

```sql
/* @bruin
name: my_schema.my_table
type: fabric.sql
materialization:
  type: table
  strategy: delete+insert
columns:
  - name: id
    type: int
    primary_key: true
  - name: name
    type: varchar(100)
  - name: updated_at
    type: datetime2
@bruin */

SELECT
    id,
    name,
    CAST(GETDATE() AS DATETIME2(6)) as updated_at
FROM source_table
WHERE modified_date > '{{ start_date }}'
```

## Using Fabric with Ingestr

A Fabric connection can be used as both a **source** and a **destination** for [ingestr](/ingestion/overview) assets, letting you move data between Fabric and any other supported platform.

> [!IMPORTANT]
> Fabric Warehouse only supports Microsoft Entra ID authentication for ingestr. Use a connection configured with `use_azure_default_credential: true` or a service principal (`client_id` / `client_secret` / `tenant_id`); username/password (SQL auth) connections cannot be used as an ingestr source or destination.
>
> Fabric ingestion requires ingestr `1.0.5` or newer, which is the default. No extra configuration is needed unless you have pinned an older release with `parameters.version`.

### Fabric as a destination

Load data from any source into a Fabric Warehouse by setting `destination: fabric`:

```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: my-shopify
  source_table: customers
  destination: fabric
```

### Fabric as a source

Point `source_connection` at a Fabric connection to load Fabric tables into another platform:

```yaml
name: raw.orders
type: ingestr
parameters:
  source_connection: fabric-default
  source_table: dbo.orders
  destination: bigquery
```
