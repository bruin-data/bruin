# Bruin — Iceberg with a REST catalog and MinIO

Loads two tables from [Frankfurter](https://frankfurter.dev), a public exchange-rate API, into **Apache Iceberg** — with a real catalog server in front of real object storage, both running locally in Docker. Still no cloud account.

- `raw/currencies.asset.yml` — the list of currency codes and names.
- `raw/exchange_rates.asset.yml` — the latest rates.

## Run it

```bash
docker compose up -d          # MinIO + the bucket + the REST catalog
bruin run iceberg-rest-minio
```

Browse what landed in the MinIO console at `localhost:9001` (`minioadmin` / `minioadmin`), under `warehouse/raw/`:

```
raw/currencies/data/00000-0-….parquet
raw/currencies/metadata/00001-….metadata.json
```

Tear it down with `docker compose down -v`.

## Notes

An Iceberg connection is a **catalog** (where table metadata lives) and **storage** (where the data files go). Here the catalog is a server you talk to over HTTP and storage is S3-compatible, which is the shape most production setups have — swap in Glue or Postgres and a real S3 bucket and nothing about the assets changes.

```yaml
      iceberg:
        - name: "iceberg-default"
          catalog:
            type: rest
            host: "localhost"
            port: 8181
          storage:
            type: s3
            path: "s3://warehouse"
            endpoint: "localhost:9000"        # MinIO, not AWS
            use_ssl: false                    # it is plain HTTP locally
            region: "us-east-1"
            auth:
              access_key: "minioadmin"
              secret_key: "minioadmin"
```

Two things about that `endpoint`. It marks the store as S3-*compatible* rather than S3, which ingestr turns into `s3.compat-mode` — MinIO does not need it, but GCS over its S3 endpoint does. And the REST catalog reaches storage **itself** to write metadata, which is why the same MinIO credentials appear in `docker-compose.yml`; the ones above configure only Bruin.

