# Kalshi

[Kalshi](https://kalshi.com/) is a regulated prediction market exchange. Bruin supports Kalshi as a public, read-only source for [Ingestr assets](/assets/ingestr), allowing you to ingest exchange status, series, events, markets, order books, trades, candlesticks, and historical market data into your data warehouse.

No API key is required for the supported public tables.

## Configuration

### Step 1: Add a connection to .bruin.yml file

Add a `kalshi` connection to the `connections` section of your `.bruin.yml` file:

```yaml
connections:
  kalshi:
    - name: "kalshi"
```

Optional Kalshi API filters can be added as URI query parameters on the connection. Use separate connections when different assets need different filters:

```yaml
connections:
  kalshi:
    - name: "kalshi-open"
      query_params:
        status: "open"

    - name: "kalshi-series-market"
      query_params:
        series_ticker: "KXHIGHNY"
        ticker: "<market-ticker>"
        period_interval: "60"
```

- `query_params` (optional): Kalshi URI parameters passed to ingestr, such as `status`, `series_ticker`, `event_ticker`, `ticker`, `tickers`, `market_tickers`, `period_interval`, and other table-specific filters.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (for example, `assets/kalshi_markets.asset.yml`):

```yaml
name: kalshi.markets
type: ingestr
connection: duckdb-default

parameters:
  source_connection: kalshi-open
  source_table: "markets"
  destination: duckdb
  schema_naming: direct
```

- `name`: The destination table name.
- `type`: Set to `ingestr`.
- `connection`: The destination connection.
- `source_connection`: The name of the Kalshi connection defined in `.bruin.yml`.
- `source_table`: The Kalshi table to ingest.
- `destination`: The destination type.
- `schema_naming`: `direct` is recommended for Kalshi tables because many provider fields are mixed-case or provider-specific.

For single-market tables, use a `ticker` or `event_ticker` returned from the `markets` table:

```yaml
name: kalshi.market_orderbook
type: ingestr
connection: duckdb-default

parameters:
  source_connection: kalshi-series-market
  source_table: "market_orderbook"
  destination: duckdb
  schema_naming: direct
```

### Step 3: Run asset to ingest data

```bash
bruin run assets/kalshi_markets.asset.yml
```

Candlestick tables require both a run start and end interval because Kalshi requires `start_ts` and `end_ts`:

```bash
bruin run assets/kalshi_candlesticks.asset.yml --start-date 2026-01-01 --end-date 2026-01-02
```

## Available Source Tables

Kalshi source allows ingesting the following tables:

| Table | Required URI params | Optional URI params | PK | Inc Key | Details |
|-------|---------------------|---------------------|----|---------|---------|
| `exchange_status` | - | - | - | - | Exchange active/trading active flags and estimated resume time. |
| `exchange_schedule` | - | - | - | - | Exchange schedule payload in `raw`. |
| `exchange_announcements` | - | - | `id` | `created_time` | Public exchange announcements. |
| `series` | - | `category`, `tags` | `ticker` | `updated_time` | Series metadata. |
| `series_by_ticker` | `series_ticker` | - | `ticker` | - | One series by ticker. |
| `events` | - | `series_ticker`, `status`, `with_nested_markets` | `event_ticker` | `updated_time` | Events and optional nested markets. |
| `event_by_ticker` | `event_ticker` | - | `event_ticker` | - | One event by ticker. |
| `markets` | - | `event_ticker`, `series_ticker`, `status`, `tickers`, `mve_filter`, `min_updated_ts`, `max_close_ts`, `min_close_ts`, `min_settled_ts`, `max_settled_ts` | `ticker` | `updated_time` | Market discovery with prices, volume, open interest, status, and `raw`. |
| `market_by_ticker` | `ticker` | - | `ticker` | - | One market by ticker. |
| `market_orderbook` | `ticker` | - | - | - | Current YES/NO bid ladders for one market. |
| `market_orderbooks` | `tickers` | - | - | - | Batch order books for comma-separated tickers. |
| `market_trades` | - | `ticker`, `is_block_trade` | `trade_id` | `created_time` | Public trades. Supports interval pushdown. |
| `market_candlesticks` | `series_ticker`, `ticker`, `period_interval` | `include_latest_before_start` | `end_period_ts` | `end_period_ts` | Candlesticks for one market. Requires intervals. |
| `market_candlesticks_batch` | `market_tickers`, `period_interval` | `include_latest_before_start` | - | - | Batch candlesticks for up to 100 market tickers. Requires intervals. |
| `historical_markets` | - | `tickers`, `event_ticker`, `series_ticker`, `status` | `ticker` | - | Archived historical markets. |
| `historical_trades` | - | `ticker`, `is_block_trade` | `trade_id` | `created_time` | Historical trades. Supports interval pushdown. |

## Interval Behavior

Intervals narrow the API request for market discovery, trades, and candlesticks:

- `markets`: `min_created_ts` and `max_created_ts` filter market creation time, not `updated_time`.
- `market_trades`: `min_ts` and `max_ts` filter trade creation/execution time.
- `historical_trades`: `min_ts` and `max_ts` filter trade creation/execution time.
- `market_candlesticks` and `market_candlesticks_batch`: `start_ts` and `end_ts` filter candlestick periods and require both start and end intervals.

Using incremental intervals on `markets` can miss markets created outside the interval but updated inside it. Kalshi exposes `min_updated_ts`, but not a matching supported `max_updated_ts`, and `min_updated_ts` is rejected when combined with many filters such as `status=open`.

## Notes

- Use `status=open` to find currently populated live markets.
- Use `tickers` for batch order books and `market_tickers` for batch candlesticks.
- Kalshi order books return YES and NO bids; asks are implied by binary market mechanics.
- Authenticated trading, portfolio, order, account, and RFQ endpoints are not supported.
