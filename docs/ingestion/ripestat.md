# RIPEstat

[RIPEstat](https://stat.ripe.net/) is the RIPE NCC's open data platform. Its Data API is public and exposes Internet routing, registration, geography, DNS, and RPKI information without requiring any authentication.

Bruin supports RIPEstat as a source for ingestr assets, allowing you to ingest RIPEstat endpoint data directly into your data warehouse.

## Public source, no connection required

RIPEstat is a public API and needs no credentials, so there is no `.bruin.yml` connection to configure. Point the asset at the public source by setting `source_connection: stat.ripe.net` — Bruin fetches directly from the RIPEstat API.

### Step 1: Create an asset file for data ingestion

To ingest data from RIPEstat, create an `ingestr` asset with the file `assets/ripestat.asset.yml`. This file defines the data flow from the source to the destination.

```yaml
name: dataset.ripestat
type: ingestr
connection: duckdb-default

parameters:
  source_connection: stat.ripe.net
  source_table: 'as-overview?resource=AS3333'

  destination: duckdb
```

- `name`: The name of the asset. This must be unique within the pipeline.
- `type`: Specifies the type of the asset. As RIPEstat is an `ingestr` asset, this should be set to `ingestr`.
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: Use the public source identifier `stat.ripe.net`. No `.bruin.yml` entry is required for it.
- `source_table`: The RIPEstat endpoint name plus its request parameters (see below).

### Choosing an endpoint

RIPEstat has no fixed set of tables. The `source_table` is the RIPEstat [Data API](https://stat.ripe.net/docs/data-api/ripestat-data-api) endpoint name, followed by that endpoint's request parameters in URL query format:

```yaml
parameters:
  source_connection: stat.ripe.net
  source_table: 'prefix-overview?resource=193.0.20.0%2F24'

  destination: duckdb
```

Endpoints that take no parameters, such as `example-resources`, use only the endpoint name. Each request produces one snapshot row from the endpoint's `data` object: scalar properties become columns, while nested objects and arrays are stored as JSON.

### Time intervals

For endpoints that support a bounded time range, use Bruin's interval flags. They are sent to RIPEstat as `starttime` and `endtime` in UTC. The interval-aware endpoints are `allocation-history`, `announced-prefixes`, `asn-neighbours-history`, `atlas-probe-deployment`, `bgp-update-activity`, `bgp-updates`, `bgplay`, `country-resource-stats`, `prefix-count`, `rir`, `ris-peer-count`, and `routing-history`. Supplying interval flags for any other endpoint returns an error rather than silently ignoring them.

Loads default to the `replace` strategy. Set `incremental_strategy` and a primary key when snapshots should instead be appended or merged. RIPEstat has no common incremental key across endpoints, so `incremental_key` is not used for API filtering.

RIPEstat limits callers to eight concurrent requests per IP address. A source read makes one request at a time and does not paginate or parallelize requests.

## Available Source Tables

RIPEstat endpoints are dynamic, so the `source_table` uses the endpoint name and its query parameters rather than a fixed table name. Some representative values:

| Source table | Inc Strategy | Details |
|--------------|--------------| ------- |
| `as-overview?resource=AS3333` | replace | Overview of an autonomous system |
| `announced-prefixes?resource=AS3333` | replace | Prefixes announced by an AS (interval-aware) |
| `prefix-overview?resource=193.0.20.0%2F24` | replace | Overview of an IP prefix |
| `routing-history?resource=AS3333` | replace | Routing history for a resource (interval-aware) |
| `example-resources` | replace | Example resources; takes no parameters |

Any endpoint documented in the RIPEstat [Data API](https://stat.ripe.net/docs/data-api/ripestat-data-api) can be used as a source table.

### Step 2: [Run](/commands/run) asset to ingest data

Navigate to your pipeline folder and run the following command to ingest data from RIPEstat into your data warehouse:

```bash
bruin run assets/ripestat.asset.yml
```
