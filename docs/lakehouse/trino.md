# Trino

> [!WARNING]
> Trino lakehouse support is not yet available. This page documents the planned configuration.

Trino provides distributed query execution for lakehouse formats, ideal for large-scale analytics workloads.

## Planned Support

| Component | Status |
|-----------|--------|
| Iceberg | Planned |
| Delta | Planned |
| Glue Catalog | Planned |
| REST Catalog | Planned |
| S3 Storage | Planned |
| GCS Storage | Planned |

## Expected Configuration

```yaml
connections:
  trino:
    - name: "lakehouse"
      host: "trino.example.com"
      port: 8080
      lakehouse:
        format: iceberg
        catalog:
          type: glue
          catalog_id: "123456789012"
          region: "us-east-1"
        storage:
          type: s3
          region: "us-east-1"
```

Check back for updates or follow the [GitHub repository](https://github.com/bruin-data/bruin) for announcements.
