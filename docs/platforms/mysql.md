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

#### Merge and SCD2 strategies
- `strategy: merge` uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` semantics to upsert rows by primary key. Optional `merge_sql` expressions are supported and map `target.*`/`source.*` references to the appropriate `VALUES(...)` expressions during the update.
- `strategy: scd2_by_time` is available for MySQL tables. Bruin evaluates the source query into a temporary table, expires existing rows that share the same primary key with newer timestamps, marks rows missing from the source as historical, and inserts the new/current versions with `_valid_from`, `_valid_until`, and `_is_current` columns maintained automatically.
