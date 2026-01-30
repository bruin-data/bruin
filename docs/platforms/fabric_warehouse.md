# Microsoft Fabric Warehouse

Bruin supports Microsoft Fabric Warehouse through the SQL endpoint (TDS protocol) using the `go-mssqldb` driver. Fabric is case-sensitive for identifiers, so use quoted identifiers or consistent casing in asset names.

## Connection configuration

Add a Fabric Warehouse connection under `fabric_warehouse`:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      fabric_warehouse:
        - name: fabric-default
          host: your-workspace.datawarehouse.fabric.microsoft.com
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
      fabric_warehouse:
        - name: fabric-sp
          host: your-workspace.datawarehouse.fabric.microsoft.com
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
      fabric_warehouse:
        - name: fabric-sql
          host: your-workspace.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          username: "<username>"
          password: "<password>"
```

## Asset types

- `fw.sql`
- `fw.seed`
- `fw.sensor.query`
- `fw.sensor.table`

## Example asset

```sql
/* @bruin
name: my_schema.my_table
type: fw.sql
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
