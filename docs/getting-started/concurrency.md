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
| `concurrency` | Pipeline runs overlapping | 1 | Cloud only |
| `instance` | CPU/memory per asset | b1.nano | Cloud only |

## Platform Notes

- **DuckDB:** Cannot share database files across parallel processes. Use `--workers 1` for shared files.
- **Cloud warehouses (BigQuery, Snowflake):** Handle high concurrency well since computation happens on their infrastructure.
