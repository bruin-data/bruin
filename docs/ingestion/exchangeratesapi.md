# Exchangeratesapi.io

[exchangeratesapi.io](https://exchangeratesapi.io/) (an APILayer product) serves current and historical foreign exchange rates.

Bruin supports exchangeratesapi.io as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest exchange rates into your data platform.

> [!WARNING]
> **On the free tier, use [Frankfurter](/ingestion/frankfurter) instead.** exchangeratesapi.io's free tier serves the ECB reference rates with base `EUR` only — the same numbers Frankfurter serves with no API key. This source is only worth its key on a **paid** plan, which adds a switchable base currency, ~172 currencies, and weekend rows.

> [!CAUTION]
> **Historical rates are not reproducible, so do not use this source to rebuild history.** The API answers with what it believes today; asking again later returns a different answer for the same past date. Preserve stored rates rather than regenerating them.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    exchangeratesapi:
        - name: "exchangeratesapi"
          access_key: "your_access_key"
          base: "EUR"
```

- `access_key` (required): Your exchangeratesapi.io API access key.
- `base` (optional): The base currency for the returned rates, e.g. `USD`. Defaults to the API's own default (`EUR`). Changing the base requires a paid plan.

### Step 2: Create an asset file for data ingestion

Create a YAML file (e.g. exchangeratesapi_ingestion.yml) inside the assets folder:

```yaml
name: public.exchange_rates
type: ingestr

parameters:
  source_connection: exchangeratesapi
  source_table: 'exchange_rates'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Set to `ingestr`.
- `source_connection`: The name of the exchangeratesapi connection defined in `.bruin.yml`.
- `source_table`: One of the tables below. The base currency can also be given in the table name, which takes precedence over the connection, e.g. `exchange_rates:CZK`.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/exchangeratesapi_ingestion.yml
```

## Available Source Tables

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `exchange_rates` | `date,base,currency` | `date` | merge | The main table. Honours `--interval-start` / `--interval-end`. |
| `latest` | `date,base,currency` | - | merge | Most recent published rates. |
| `symbols` | `currency` | - | replace | Currency codes and names. |

Columns for `exchange_rates` and `latest`: `date`, `base`, `currency`, `exchange_rate`. Each day includes a base-to-base identity row with `exchange_rate = 1.0`.

## Incremental behavior and limitations

`exchange_rates` issues **one HTTP request per day** in the requested interval (there is no bulk endpoint on the plans this source targets). To stop an accidental multi-year interval from burning a monthly quota, intervals longer than **400 days** are refused. ingestr also requires `--interval-start` to be strictly earlier than `--interval-end`, so a single-day run should ask for the day and the day after.
