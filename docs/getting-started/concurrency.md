# Concurrency & Resource Limits

Bruin runs assets in parallel based on their dependencies. Assets without dependencies on each other execute simultaneously.

## Workers (CLI)

The `--workers` flag controls how many assets run at once:

```bash
bruin run --workers 8
```

| Workers | Use Case |
|---------|----------|
| 1-4 | Memory-heavy Python assets, local development |
| 8-16 | General use (default: 16) |
| 16-32 | Lightweight SQL assets pushing work to the warehouse |

## Pipeline Concurrency (Bruin Cloud)

The `concurrency` setting in `pipeline.yml` controls how many **runs of the same pipeline** can overlap:

```yaml
concurrency: 2  # Allow 2 runs simultaneously (default: 1)
```

Keep this low (1-2) unless you're backfilling independent date ranges.

## Connection Concurrency Limits

Use `max_concurrent_assets` on a connection in `.bruin.yml` to limit how many assets can use that connection at the same time during a run:

```yaml
environments:
  default:
    connections:
      postgres:
        - name: "postgres-main"
          host: "db.example.com"
          port: 5432
          database: "analytics"
          username: "${POSTGRES_USER}"
          password: "${POSTGRES_PASSWORD}"
          max_concurrent_assets: 2
```

This is useful for rate-limited APIs, small databases, or warehouses with strict concurrency quotas. If more runnable assets need the same connection, Bruin keeps the extra assets queued until one of the active assets releases that connection slot.

Connection limits work together with `--workers`: `--workers` is the total number of assets Bruin can run at once, while `max_concurrent_assets` is the per-connection cap. The lower effective limit wins for assets that use that connection.

For `ingestr` assets, Bruin tracks both the source connection and the destination connection. For example, an ingestion asset moving Shopify data into Postgres counts against both the Shopify source limit and the Postgres destination limit while it runs.

The value must be a positive integer. Omit `max_concurrent_assets` when a connection should not have a per-connection limit.

## Instance Types & Weighted Slots (Bruin Cloud)

Larger instances consume more of your tenant's resource pool:

| Instance | Memory | Slots Used |
|----------|--------|------------|
| b1.nano | 256 MB | 1 |
| b1.small | 1 GB | ~2 |
| b1.large | 4 GB | ~4 |
| b1.xlarge | 6 GB | ~6 |

**Example:** With 32 available slots, you can run:

- 32 nano instances, OR
- 8 large instances, OR
- A mix (e.g., 4 large + 16 nano)

This means memory-intensive pipelines naturally run fewer assets in parallel.

## Quick Reference

| Setting | Controls | Default | Scope |
|---------|----------|---------|-------|
| `--workers` | Assets running simultaneously | 16 | Single run |
| `max_concurrent_assets` | Assets using one connection simultaneously | Unlimited | Single run |
| `concurrency` | Pipeline runs overlapping | 1 | Cloud only |
| `instance` | CPU/memory per asset | b1.nano | Cloud only |

## Platform Notes

- **DuckDB:** Cannot share database files across parallel processes. Use `--workers 1` for shared files.
- **Cloud warehouses (BigQuery, Snowflake):** Handle high concurrency well since computation happens on their infrastructure.
