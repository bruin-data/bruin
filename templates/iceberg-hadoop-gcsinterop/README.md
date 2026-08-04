# Bruin — Iceberg with no catalog service, on GCS

Loads two tables from [Frankfurter](https://frankfurter.dev), a public
exchange-rate API, into **Apache Iceberg** tables that need no catalog service at
all. The `hadoop` catalog keeps its metadata in the warehouse itself, so the
bucket is the only thing to provision.

Storage is Google Cloud Storage reached through its **S3 interoperability API**,
the alternative to a service-account key.

## Setup

An **HMAC key** for the bucket: Cloud Storage → Settings → Interoperability →
*Create a key*. It looks like an AWS key pair and is used as one.

Then fill in the blanks in `.bruin.yml` — the connection is already there, only
the marked values are missing:

```yaml
      iceberg:
        - name: "iceberg-default"
          catalog:
            type: hadoop
          storage:
            type: s3
            path: "s3://${GCS_BUCKET}/warehouse"        # <- your GCS bucket
            endpoint: "storage.googleapis.com"
            auth:
              access_key: "${GCS_HMAC_KEY}"             # <-
              secret_key: "${GCS_HMAC_SECRET}"          # <-
          properties:
            allow-unsafe-commits: "true"
```

Replace each `${...}` with the value, or export them as environment variables
and leave the file alone — Bruin expands both:

```bash
export GCS_BUCKET=my-company-lake
export GCS_HMAC_KEY=GOOG1E…
export GCS_HMAC_SECRET=…
```

## Run it

```bash
bruin run iceberg-hadoop-gcsinterop
```

## Notes

> **The hadoop catalog does not commit atomically to object storage.** It needs
> `allow-unsafe-commits: "true"`, already set here. Two writers to one table can
> lose a commit, so this is for getting started and for single-writer pipelines —
> not for concurrent production writes. Move to Glue, REST or Postgres when that
> matters.

The storage block says `type: s3` with an `s3://` warehouse even though the
bucket is Google's, because the S3 interop endpoint really is an S3 API — only
`endpoint` points at Google. For native GCS instead, use `type: gcs`, a `gs://`
warehouse and a service-account key, as in `iceberg-postgres-gcs`.

No `region` here: Google ignores it, and ingestr supplies the placeholder the
AWS SDK insists on. AWS S3 still needs a real one.
