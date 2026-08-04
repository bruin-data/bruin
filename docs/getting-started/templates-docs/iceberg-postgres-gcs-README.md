# Bruin — Iceberg with a Postgres catalog and Google Cloud Storage

Loads two tables from [Frankfurter](https://frankfurter.dev), a public
exchange-rate API, into **Apache Iceberg** tables catalogued in Postgres with the
data in Google Cloud Storage. Useful on GCP, or anywhere you would rather own the
catalog than depend on a cloud service: any Postgres will do — Cloud SQL, Neon,
RDS, or one you run.

## Setup

1. **A Postgres database** for the catalog. It needs no schema; Iceberg creates
   its own tables on first run.
2. **A GCS service account** with `roles/storage.objectAdmin` on the bucket.
   Download its JSON key and save it next to `.bruin.yml` as `sa.json`.

Then fill in the blanks in `.bruin.yml` — the connection is already there, only
the marked values are missing:

```yaml
      iceberg:
        - name: "iceberg-default"
          catalog:
            type: postgres
            host: "${PG_HOST}"              # <- your Postgres host
            port: 5432
            database: "${PG_DATABASE}"      # <-
            auth:
              username: "${PG_USERNAME}"    # <-
              password: "${PG_PASSWORD}"    # <-
          storage:
            type: gcs
            path: "gs://${GCS_BUCKET}/warehouse"   # <- your bucket
            key_file: "sa.json"
          properties:
            sslmode: "require"
```

Replace each `${...}` with the value, or export them as environment variables
and leave the file alone — Bruin expands both.

## Run it

```bash
bruin run iceberg-postgres-gcs
```

Data lands at `gs://$GCS_BUCKET/warehouse/raw.db/`.

## Notes

`sslmode: require` is in `properties` because managed Postgres — Neon, RDS,
Cloud SQL — refuses plaintext connections. Drop it for a local database that
does not speak TLS.

Storage authenticates with a service-account key, given as `key_file` (a path)
or `key_json` (the key inline). A path is fine here, where the pipeline runs on
your machine; use `key_json` anywhere the run happens elsewhere, such as Bruin
Cloud, since the file will not be on that machine. Leave both out to use
Application Default Credentials.
