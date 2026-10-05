# Yahoo Finance

[Yahoo Finance](https://finance.yahoo.com/) provides market data for stocks, ETFs, mutual funds, indices, currencies and crypto. Bruin supports Yahoo Finance as a source for ingestr assets, so you can load price history, dividends, splits, live quotes, company profiles, option chains, financial statements and news into your data warehouse.

::: warning
Yahoo Finance has no official public API. ingestr reads the same endpoints the Yahoo Finance website uses, which Yahoo may change or throttle without notice. The data is intended for personal and research use; review [Yahoo's terms](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html) before using it commercially.
:::

## Public source, no connection required

Yahoo Finance needs no credentials, so there is no `.bruin.yml` connection to configure. Point the asset at the public source by setting `source_connection: finance.yahoo.com`.

### Step 1: Create an asset file for data ingestion

Create an `ingestr` asset, for example `assets/yfinance_prices.asset.yml`:

```yaml
name: raw.prices
type: ingestr
connection: duckdb-default

parameters:
  source_connection: finance.yahoo.com
  source_table: 'history:AAPL,MSFT'

  destination: duckdb
```

- `name`: The name of the asset. This must be unique within the pipeline.
- `type`: Specifies the type of the asset. Set this to `ingestr`.
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: Use the public source identifier `finance.yahoo.com`. No `.bruin.yml` entry is required for it.
- `source_table`: The table name followed by one or more Yahoo ticker symbols (see below).

### Step 2: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/yfinance_prices.asset.yml
```

## Selecting symbols

Every table needs one or more Yahoo ticker symbols, passed after a colon in the table name, e.g. `history:AAPL,MSFT`. Use the symbols exactly as they appear on Yahoo Finance, e.g. `BTC-USD` for Bitcoin, `^GSPC` for the S&P 500, `EURUSD=X` for EUR/USD, or `SAP.DE` for SAP on XETRA.

Symbols Yahoo doesn't recognize are skipped with a warning, except in `quotes`, `info` and `options`: these tables are fully replaced on every run, so an unknown symbol fails the run instead of silently dropping its previously loaded rows.

Options are added as URL-style parameters:

```yaml
parameters:
  source_connection: finance.yahoo.com
  source_table: 'history:AAPL,MSFT?interval=1h'

  destination: duckdb
```

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `history` | `symbol, interval, date` (intraday: `symbol, interval, timestamp`) | `timestamp` | merge | OHLCV price bars. |
| `dividends` | `symbol, date` | `date` | merge | Dividend payments by ex-date. |
| `splits` | `symbol, date` | `date` | merge | Stock splits with numerator, denominator and ratio. |
| `quotes` | `symbol` | – | replace | Current quote snapshot: price, volume, market cap, 52-week range, valuation ratios and more. |
| `info` | `symbol` | – | replace | Company profile, key statistics, financial data and calendar events; one JSON column per module. |
| `options` | `contractSymbol` | – | replace | Full option chain (calls and puts) across all expirations. |
| `income_statement` | `symbol, frequency, as_of_date, metric` | – | merge | Income statement line items, one row per metric per period. |
| `balance_sheet` | `symbol, frequency, as_of_date, metric` | – | merge | Balance sheet line items, one row per metric per period. |
| `cash_flow` | `symbol, frequency, as_of_date, metric` | – | merge | Cash flow line items, one row per metric per period. |
| `news` | `uuid` | `providerPublishTime` | merge | Recent news articles for each symbol. |

### `history`

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `interval` | `1d` | Bar size: `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `1wk`, `1mo`. |
| `prepost` | `false` | Include pre- and post-market bars (intraday intervals only). |

Without an interval start, daily and longer intervals load the symbol's full history. Yahoo only keeps intraday bars for a limited window: 30 days for `1m`, 60 days for `2m`–`90m`, and 730 days for `60m`/`1h`.

Prices are adjusted for splits but not for dividends; use the `dividends` table to compute dividend-adjusted returns. `date` is the trading date in the exchange's timezone. Keep daily-or-longer and intraday intervals in separate assets, since their primary keys differ.

A split makes Yahoo re-adjust all earlier prices. When a symbol has split within the 7 days before a run's interval, or during it, its full daily, weekly or monthly history is reloaded so the table stays consistent. Intraday bars are not reloaded: after a split, bars loaded earlier stay on the old price scale until you run a full refresh.

### `info`

The `modules` parameter selects Yahoo quoteSummary modules. The default is `assetProfile,summaryDetail,price,quoteType,defaultKeyStatistics,financialData,calendarEvents`; others include `earnings`, `earningsHistory`, `earningsTrend`, `recommendationTrend`, `upgradeDowngradeHistory`, `institutionOwnership`, `fundOwnership`, `majorHoldersBreakdown`, `insiderHolders`, `insiderTransactions` and `secFilings`.

```yaml
parameters:
  source_connection: finance.yahoo.com
  source_table: 'info:AAPL?modules=price,recommendationTrend,earningsHistory'

  destination: duckdb
```

### Financial statements

`income_statement`, `balance_sheet` and `cash_flow` accept a `frequency` parameter: `annual` (default), `quarterly`, or `trailing` (trailing twelve months; not available for `balance_sheet`).

```yaml
parameters:
  source_connection: finance.yahoo.com
  source_table: 'income_statement:AAPL,MSFT?frequency=quarterly'

  destination: duckdb
```

Each row is one line item (`metric`, e.g. `TotalRevenue`, `NetIncome`, `FreeCashFlow`) for one reporting period (`as_of_date`), with its `value` and `currency_code`. Financial statements always load the full series Yahoo provides, so newly published and restated periods are picked up on every run.

## Incremental loads

`history`, `dividends`, `splits` and `news` use Bruin's run interval (`--start-date` / `--end-date`) to limit what they load. For `history`, the start includes the whole bar it falls in (e.g. the full week or month). `news` returns the most recent articles Yahoo lists for each symbol, so it's best run on a schedule to build up an archive.
