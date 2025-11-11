# MySQL

Bruin supports MySQL as a data platform for SQL assets and ingestion pipelines.

## Connection
Add a MySQL entry under `connections` in `.bruin.yml` using the following schema.

```yaml
connections:
  mysql:
    - name: "connection_name"
      username: "mysql_user"
      password: "XXXXXXXXXX"
      host: "mysql.somehost.com"
      port: 3306
      database: "analytics"
      driver: "pymysql"           # optional, defaults to pymysql
      ssl_ca_path: "path/to/ca.pem"       # optional
      ssl_cert_path: "path/to/cert.pem"   # optional
      ssl_key_path: "path/to/key.pem"     # optional
```

> [!TIP]
> If you plan to execute any SQL containing multiple statements (e.g. Bruin table materializations), ensure the connection allows multi-statements. When using the built-in MySQL client in Bruin this flag is automatically appended to the DSN.

## MySQL Assets

### `my.sql`
Executes a materialized MySQL SQL asset. See the [definition schema](../assets/definition-schema.md) for available parameters.


#### Example: Create and refresh a table
```bruin-sql
/* @bruin
name: warehouse.example
type: my.sql
materialization:
    type: table
@bruin */

SELECT
    id,
    country,
    name
FROM staging.customers
```
