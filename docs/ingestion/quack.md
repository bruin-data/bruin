# Quack (DuckDB server)

[Quack](https://duckdb.org/quack/) is DuckDB's client-server protocol: a DuckDB instance started with `quack_serve` accepts connections from other DuckDB clients over HTTP.

Bruin supports a Quack server as both a **source** and a **destination** for [Ingestr assets](/assets/ingestr), so you can read tables from a remote DuckDB server or load data into it.

## Step 1: Add a connection to .bruin.yml

```yaml
connections:
  quack:
    - name: "my-quack"
      host: "duckdb.internal"
      port: 9494
      token: "my_secret_token"
```

- `name`: The name to identify this connection.
- `host`: The host the Quack server listens on.
- `port`: Optional. The port the server listens on. DuckDB uses `9494` by default.
- `token`: Optional. The authentication token set on the server with `quack_serve(..., token = '...')`.
- `disable_ssl`: Optional. Set to `true` when the server isn't behind TLS, for example a server on `localhost`.

The server is started from DuckDB, for example:

```sql
CALL quack_serve('quack:localhost', token = 'my_secret_token');
```

## Step 2: Create an asset file

To read from a Quack server, set it as the source connection and use `schema.table` as the source table:

```yaml
name: analytics.events
type: ingestr

parameters:
  source_connection: my-quack
  source_table: 'main.events'

  destination: bigquery
```

To load data into a Quack server, set it as the destination. The asset `name` is the destination table, in `schema.table` form:

```yaml
name: main.events
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.events'

  destination: quack
  destination_connection: my-quack
```

The `replace`, `append`, `merge` and `delete+insert` strategies are supported when writing to Quack. Incremental loads and custom queries work when reading from it.

## Step 3: [Run](/commands/run) the asset

```bash
bruin run assets/events.asset.yml
```

## Notes

- Quack is still in beta in DuckDB. The server and the DuckDB version ingestr uses must be on the same DuckDB release: a 1.5.x client can't talk to a 2.x server, and vice versa.
- Only the server's default database is reachable, so destination tables must be `schema.table`, not `database.schema.table`.
- CDC (change data capture) into a Quack server isn't supported.
