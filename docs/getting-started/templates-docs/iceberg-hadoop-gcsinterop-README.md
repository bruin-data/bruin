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

```bash
export GCS_BUCKET=my-company-lake
export GCS_HMAC_KEY=GOOG1E…
export GCS_HMAC_SECRET=…
```

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

`region: auto` is a placeholder: Google ignores it, but the AWS SDK will not sign
a request without one.
