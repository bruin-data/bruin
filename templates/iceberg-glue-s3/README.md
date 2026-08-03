# Bruin — Iceberg with AWS Glue and S3

Loads two tables from [Frankfurter](https://frankfurter.dev), a public
exchange-rate API, into **Apache Iceberg** tables catalogued in AWS Glue with the
data in S3. This is the shape most AWS deployments use: a managed catalog, so
there is no metastore to run.

## Setup

An IAM user or role that can reach both halves:

- **Glue** — `glue:GetDatabase`, `glue:CreateDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`
- **The bucket** — `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`

Then set these before running:

```bash
export AWS_REGION=eu-north-1
export AWS_ACCESS_KEY_ID=AKIA…
export AWS_SECRET_ACCESS_KEY=…
export S3_BUCKET=my-company-lake
```

Use a long-lived access key, not SSO or STS credentials — those expire within
hours and a scheduled pipeline would start failing overnight. If you must use
them, add `session_token` to both `auth` blocks.

```bash
bruin run iceberg-glue-s3
```

The tables appear in Glue under the `raw` database, with data at
`s3://$S3_BUCKET/warehouse/raw.db/`.

## Notes

The catalog and the storage have **separate** `auth` blocks. They are the same
credentials here, but they do not have to be — Glue is an AWS API and the bucket
is storage, and Bruin sends each set to the right place.

`region` is required for both: Glue needs to know which regional endpoint to
call, and S3 uses it to route the request. A wrong one gives you
`PermanentRedirect`; an empty one gives you `Invalid region`.
