# Cloudflare Radar

[Cloudflare Radar](https://radar.cloudflare.com/) provides aggregated insights into Internet traffic, routing, connection quality, outages, bots, and Certificate Transparency data.

Bruin supports Cloudflare Radar as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Cloudflare Radar into your data warehouse.

To set up a Cloudflare Radar connection, add a configuration item to `.bruin.yml` and reference it from an ingestr asset. You need an API token that can read Cloudflare Radar data. Radar's data endpoints are global, so the Cloudflare account ID is not part of the connection.

## Set up a connection

Cloudflare Radar connections are defined using the following properties:

- `name`: The name to identify this connection.
- `api_token`: Your Cloudflare API token with read access to Radar data (required).

```yaml
connections:
  cloudflare_radar:
    - name: my-cloudflare-radar
      api_token: "your-api-token"
```

You can also use environment variables in `.bruin.yml` with the `${VAR_NAME}` syntax:

```yaml
connections:
  cloudflare_radar:
    - name: my-cloudflare-radar
      api_token: ${CLOUDFLARE_RADAR_API_TOKEN}
```

Bruin uses the `cloudflare_radar` connection key in `.bruin.yml`. The underlying ingestr source URI scheme is `cloudflare-radar://`, as required by ingestr.

## Ingesting data

Create an [asset configuration](/assets/ingestr#asset-structure) file to define the data flow:

```yaml
name: public.cloudflare_radar_annotations
type: ingestr

parameters:
  source_connection: my-cloudflare-radar
  source_table: 'annotations'

  destination: postgres
```

- `type`: Specifies the asset type. Set this to `ingestr` to use the ingestr data pipeline.
- `source_connection`: The name of the Cloudflare Radar connection defined in `.bruin.yml`.
- `source_table`: The Cloudflare Radar table to ingest. See [Available Source Tables](#available-source-tables) for options.
- `destination`: The name of the destination platform.

## Available Source Tables

Cloudflare Radar exposes two kinds of source tables: a set of named catalog/event tables, and a generic `api:` syntax that gives you access to every Radar GET endpoint.

### Named tables

These provide convenient, automatically paginated aliases for commonly loaded catalog and event datasets.

| Table | Primary Key | Inc Key | Inc Strategy | Description |
|-------|-------------|---------|--------------|-------------|
| `annotations` | `id` | `startDate` | merge | Internet events, outages, and traffic anomalies published by Radar. |
| `autonomous_systems` | `asn` | - | replace | Autonomous systems known to Radar. |
| `bgp_hijacks` | `id` | `min_hijack_ts` | merge | Detected BGP route hijack events. |
| `bgp_leaks` | `id` | `detected_ts` | merge | Detected BGP route leak events. |
| `bots` | `slug` | - | replace | Radar's catalog of known bots and their user-agent patterns. |
| `certificate_authorities` | `sha256Fingerprint` | - | replace | Certificate authorities tracked through Certificate Transparency. |
| `certificate_logs` | `slug` | - | replace | Certificate Transparency logs and their current states. |
| `datasets` | `id` | - | replace | Downloadable Radar ranking and report datasets. |
| `geolocations` | `geoId` | - | replace | Radar's geographic location catalog. |
| `locations` | `alpha2` | - | replace | Countries and regions available as Radar filters. |
| `outages` | `id` | `startDate` | merge | Internet outage annotations. |
| `origins` | `slug` | - | replace | Origins available in Radar's origin metrics. |
| `tlds` | `tld` | - | replace | Top-level domains and their managers. |
| `traffic_anomalies` | `uuid` | `startDate` | merge | Recent Internet traffic anomalies. |

```yaml
name: public.cloudflare_radar_autonomous_systems
type: ingestr

parameters:
  source_connection: my-cloudflare-radar
  source_table: 'autonomous_systems'

  destination: postgres
```

`annotations`, `outages`, `bgp_hijacks`, `bgp_leaks`, and `traffic_anomalies` accept `--interval-start` and `--interval-end`. Without an interval, `annotations` and `outages` cover the maximum API-supported trailing range of 364 days, BGP event tables return all events exposed by the API, and traffic anomalies cover the trailing seven days.

### All Radar API endpoints

Every GET endpoint in the [Cloudflare Radar API](https://developers.cloudflare.com/api/resources/radar/) is available as a dynamic table. Set `source_table` to `api:` followed by the endpoint path after `/radar/`, and append the endpoint's query parameters directly to the table name.

For example, the API route `/radar/http/timeseries` becomes:

```yaml
name: public.cloudflare_radar_http_timeseries
type: ingestr

parameters:
  source_connection: my-cloudflare-radar
  source_table: 'api:http/timeseries?dateRange=7d&location=US'

  destination: postgres
```

Parameterized route segments are filled in as part of the path:

```yaml
# /radar/http/summary/{dimension}
source_table: 'api:http/summary/http_protocol?dateRange=7d'

# /radar/entities/asns/{asn}
source_table: 'api:entities/asns/13335'

# /radar/ranking/domain/{domain}
source_table: 'api:ranking/domain/cloudflare.com'
```

This covers all Radar families, including agent readiness, AI, annotations, AS112, attacks, BGP, bots, Certificate Transparency, datasets, DNS, email, entities, geolocations, HTTP, leaked credential checks, netflows, origins, post-quantum, quality, ranking, robots.txt, search, TCP resets and timeouts, TLDs, and traffic anomalies. Because the endpoint path and parameters are passed through, newly added Radar GET endpoints work without a connector release as long as they use Radar's standard JSON response envelope or return CSV.

Repeated parameters are supported, for example `location=US&location=GB`. The `/radar/datasets/{alias}` download endpoint returns CSV, which the connector parses into one row per record.

Dynamic list endpoints are automatically paginated when Radar exposes offset or page pagination. Analytics, top, detail, search, and other non-list endpoints make a single request using the supplied parameters.

Dynamic tables default to the `replace` strategy. To merge them, provide the endpoint's stable key as the primary key and select the merge strategy. For endpoints that support date filters, `--interval-start` and `--interval-end` are forwarded as `dateStart` and `dateEnd` unless those parameters are already present in the source-table query string. Endpoints that only support `dateEnd` receive only the interval end.

### Response rows

Dynamic endpoint responses are converted to relational rows as follows:

- list and top responses produce one row per item;
- time-series responses produce one row per timestamp, with each metric as a column;
- grouped or multiple series include `_series` when needed;
- summary responses produce `dimension` and `value` columns;
- detail responses produce one row;
- nested values and Radar response metadata remain JSON columns.
