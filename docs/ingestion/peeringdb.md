# PeeringDB

[PeeringDB](https://www.peeringdb.com/) is a freely available, user-maintained database of networks, Internet Exchange Points (IXPs), data centers, and the interconnections between them.

Bruin supports PeeringDB as a source for [Ingestr assets](/assets/ingestr). You can use it without any connection, or with a connection that holds a PeeringDB API key.

## Public source, no connection required

There is no `.bruin.yml` entry to configure. Set `source_connection: peeringdb.com` and Bruin reads PeeringDB as a guest:

```yaml
name: peeringdb.networks
type: ingestr
connection: duckdb-default

parameters:
  source_connection: peeringdb.com
  source_table: networks

  destination: duckdb
```

PeeringDB limits guests to 20 requests per minute per IP address and only shows them contacts marked public, so run guest assets one at a time.

## Connection with an API key

A [PeeringDB API key](https://docs.peeringdb.com/howto/api_keys/) doubles the request allowance and returns contacts that are visible only to PeeringDB users. Add it as a connection in `.bruin.yml`:

```yaml
    connections:
      peeringdb:
        - name: "peeringdb"
          api_key: "<api_key>"
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | The name to identify this PeeringDB connection. |
| `api_key` | Yes | A PeeringDB user or organization API key. A read-only key is enough. |

Then point the asset at it:

```yaml
name: peeringdb.network_contacts
type: ingestr
connection: duckdb-default

parameters:
  source_connection: peeringdb
  source_table: network_contacts

  destination: duckdb
```

PeeringDB data is subject to its [acceptable use policy](https://www.peeringdb.com/aup).

## Available Source Tables

Every table uses `id` as its primary key and is loaded with the `merge` strategy, using `updated` as the incremental key. Records deleted in PeeringDB are kept with `status` set to `deleted`.

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `organizations` | id | updated | merge | Organizations that own networks, exchanges, facilities, carriers, and campuses |
| `campuses` | id | updated | merge | Campuses that group facilities |
| `facilities` | id | updated | merge | Facilities, such as data centers |
| `carriers` | id | updated | merge | Carriers that provide connectivity between facilities |
| `carrier_facilities` | id | updated | merge | Carrier presence at facilities |
| `exchanges` | id | updated | merge | Internet Exchange Points |
| `exchange_facilities` | id | updated | merge | Exchange presence at facilities |
| `exchange_lans` | id | updated | merge | Exchange peering LANs |
| `exchange_prefixes` | id | updated | merge | Prefixes used on exchange peering LANs |
| `networks` | id | updated | merge | Networks (autonomous systems) |
| `network_facilities` | id | updated | merge | Network presence at facilities |
| `network_exchange_lans` | id | updated | merge | Network connections to exchanges, with IP addresses and port speeds |
| `network_contacts` | id | updated | merge | Network points of contact; guests only see public ones |

## Run the asset

```bash
bruin run assets/peeringdb_networks.asset.yml
```
