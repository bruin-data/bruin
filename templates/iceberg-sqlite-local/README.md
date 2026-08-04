# Bruin — Iceberg on your laptop

Loads two tables from [Frankfurter](https://frankfurter.dev), a public exchange-rate
API, into **Apache Iceberg** tables on your own disk. No cloud account, no
credentials, no services to start.

- `raw/currencies.asset.yml` — the list of currency codes and names.
- `raw/exchange_rates.asset.yml` — the latest rates.

## Run it

```bash
bruin run iceberg-sqlite-local
```

That is the whole setup. The connection in `.bruin.yml` needs nothing filled in:

```yaml
      iceberg:
        - name: "iceberg-default"
          catalog:
            type: sqlite
            path: "catalog.db"          # relative to .bruin.yml
          storage:
            type: local
            path: "warehouse"           # relative to .bruin.yml
```

Afterwards, `catalog.db` holds the table metadata and `warehouse/` the data:

```
warehouse/raw.db/currencies/data/00000-0-….parquet
warehouse/raw.db/currencies/metadata/00001-….metadata.json
```

Read them back with DuckDB:

```sql
SET unsafe_enable_version_guessing = true;
SELECT * FROM iceberg_scan('warehouse/raw.db/currencies');
```

## Notes

An Iceberg connection is a **catalog** (where table metadata lives) and
**storage** (where the data files go). This template picks the simplest of each:
a SQLite file and a local directory. Everything else — Glue, REST, Postgres,
Hive, and S3 or GCS storage — swaps in by changing those two blocks only. The
assets never change.

Not for production — the point is to see Iceberg work before committing to
infrastructure. See the [Iceberg docs](https://getbruin.com/docs/bruin/ingestion/iceberg.html)
for the real backends.
