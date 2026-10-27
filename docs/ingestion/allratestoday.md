# AllRatesToday

[AllRatesToday](https://allratestoday.com/) serves the official exchange rate tables that central banks and tax authorities publish, such as the ECB reference rates, the RBI reference rate, the NBP table A middle rates or the Bank of England spot rates.

Bruin supports AllRatesToday as a source for ingestr assets, allowing you to ingest official exchange rates directly into your data warehouse.

## Public source, no connection required

AllRatesToday is a public API and needs no credentials, so there is no `.bruin.yml` connection to configure. Point the asset at the public source by setting `source_connection: allratestoday.com`.

### Step 1: Create an asset file for data ingestion

To ingest data from AllRatesToday, create an `ingestr` asset with the file `assets/rbi_rates.asset.yml`. This file defines the data flow from the source to the destination.

```yaml
name: dataset.rbi_rates
type: ingestr
connection: duckdb-default

parameters:
  source_connection: allratestoday.com
  source_table: 'central_bank_rates:rbi'

  destination: duckdb
```

- `name`: The name of the asset. This must be unique within the pipeline.
- `type`: Specifies the type of the asset. As AllRatesToday is an `ingestr` asset, this should be set to `ingestr`.
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: Use the public source identifier `allratestoday.com`. No `.bruin.yml` entry is required for it.
- `source_table`: The name of the AllRatesToday table you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `central_banks` | `code` | – | replace | Institutions with a published table: code, name, latest published date, cadence and a stale flag. |
| `central_bank_rates:<bank>` | `bank`, `date`, `base`, `quote`, `rate_type` | – | merge | The latest published table of one institution, e.g. `central_bank_rates:rbi`. Use the `code` column of `central_banks` for the bank code. |

Each row of `central_bank_rates` reads as "1 `base` = `rate` `quote`" on `date`, as published. `rate_type` is what the institution calls the rate (`reference`, `middle`, `buy`, `sell`, ...).

> [!NOTE]
> Only the **latest** table each institution has published is available, so this source cannot backfill history. Schedule the asset daily and the `merge` strategy keeps one set of rows per published date. The data is free to use, including commercially, with a visible link to allratestoday.com wherever the rates are shown; see the [open API terms](https://allratestoday.com/open-api/).

### Step 2: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/rbi_rates.asset.yml
```
