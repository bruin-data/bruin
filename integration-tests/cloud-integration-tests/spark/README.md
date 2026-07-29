# Spark integration tests

This suite exercises Bruin's Spark platform end to end against Apache Spark
4.0.3 over Spark Connect. Testcontainers builds the local Spark image and
starts both Spark and MinIO; no external Spark cluster or cloud credentials are
required.

The Spark image mirrors the versions used by the ADBC Spark driver's own
integration environment. It adds Iceberg for row-level table operations and
the Hadoop AWS dependencies needed to read seed staging files from MinIO.

The suite uses `DOCKER_HOST` when it is set and otherwise resolves the active
Docker CLI context, which also supports local alternatives such as OrbStack.
It is skipped automatically when no healthy Docker provider is available.

## Running

From the repository root:

```bash
make integration-test-spark
```

It also runs as part of the full integration suite and the cloud suite:

```bash
make integration-test
make integration-test-cloud
```

After building Bruin, the package can be run directly:

```bash
cd integration-tests/cloud-integration-tests/spark
go test -v -count=1 -timeout 30m .
```

The first run downloads the Spark and MinIO images, builds the cached Spark
test image, and installs the pinned ADBC Spark driver. Later runs reuse those
artifacts.

## Coverage

The suite covers:

- connection testing and ad hoc queries;
- create/replace, append, delete/insert, truncate/insert, merge, time-interval,
  DDL, SCD2-by-column, SCD2-by-time, and view materializations;
- Iceberg partition specs and sort-order-backed clustering for create/replace,
  DDL, and SCD2 tables;
- annotated Spark asset and quality-check queries;
- successful and failing column checks plus custom checks;
- CSV seed ingestion through ADBC's real S3 staging path;
- automatic catalog namespace creation;
- query sensors and metadata-based table sensors;
- source assets;
- database import through ADBC `GetObjects`, including columns.

Set `SPARK_TEST_IMAGE` to an already-built compatible image name if you need to
test a custom Spark image locally.
