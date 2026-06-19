# Manifold

[Manifold](https://manifold.markets/) is a prediction market platform. Bruin supports Manifold as a public read-only source for [Ingestr assets](/assets/ingestr), allowing you to ingest market, bet, comment, group, user, portfolio, transaction, league, and boost history data into your data warehouse.

No API key is required for the supported Manifold tables.

## Configuration

### Step 1: Add a connection to .bruin.yml file

Add a `manifold` connection to the `connections` section of your `.bruin.yml` file:

```yaml
connections:
  manifold:
    - name: "manifold"
```

Manifold supports optional URI filters. Add them with `query_params` when a table needs a single value:

```yaml
connections:
  manifold:
    - name: "manifold-bitcoin-search"
      query_params:
        term: "bitcoin"
        sort: "newest"

    - name: "manifold-market"
      query_params:
        market_id: "YOUR_MARKET_ID"
```

For repeated URI parameters, such as the `ids` parameter used by `market_probabilities`, use `query_param_lists`:

```yaml
connections:
  manifold:
    - name: "manifold-probabilities"
      query_param_lists:
        ids:
          - "MARKET_ID_1"
          - "MARKET_ID_2"
```

The keys in `query_params` and `query_param_lists` are passed through to ingestr as Manifold URI query parameters. Use the exact parameter name from the table list below, including camelCase names such as `userId`, `contractSlug`, and `groupId`.

### Step 2: Create an asset file for data ingestion

To ingest data from Manifold, create an [asset configuration](/assets/ingestr#asset-structure) file such as `assets/manifold.asset.yml`:

```yaml
name: raw.manifold_markets
type: ingestr
connection: duckdb-default

parameters:
  source_connection: manifold
  source_table: "markets"

  destination: duckdb
```

- `name`: The name of the asset.
- `type`: Set this to `ingestr`.
- `connection`: The destination connection where the data will be stored.
- `source_connection`: The name of the Manifold connection defined in `.bruin.yml`.
- `source_table`: The Manifold table to ingest.
- `destination`: The destination platform.

For filtered tables, reference the connection that carries the relevant Manifold URI parameters:

```yaml
name: raw.manifold_bitcoin_search
type: ingestr
connection: duckdb-default

parameters:
  source_connection: manifold-bitcoin-search
  source_table: "search_markets"

  destination: duckdb
```

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `markets` | `id` | `createdTime` | merge | Public market list. Optional URI params: `sort`, `order`, `userId`, `groupId`. |
| `search_markets` | `id` | `createdTime` | merge | Search/filter markets. Supports interval end as `beforeTime`. Optional URI params: `term`, `sort`, `filter`, `creatorId`, `contractType`, `topicSlug`, `minLiquidity`, `maxLiquidity`. |
| `market_by_id` | `id` | - | replace | Full market by id. Required URI param: `market_id`. |
| `market_by_slug` | `id` | - | replace | Full market by slug. Required URI param: `contract_slug`. |
| `market_probability` | - | - | replace | Current probability for one market. Multiple choice markets return answer probabilities in `raw`. Required URI param: `market_id`. |
| `market_probabilities` | - | - | replace | Current probabilities for up to 100 market ids. Repeat `ids` in the URI for multiple markets. Required URI param: `ids`. |
| `market_positions` | - | - | replace | Position information for one market. Required URI param: `market_id`. Optional URI params: `order`, `top`, `bottom`, `userId`, `answerId`. |
| `bets` | `id` | `createdTime` | merge | Public bets. Supports interval pushdown. Optional URI params: `userId`, `username`, `contractId`, `contractSlug`, `kinds`, `order`. |
| `comments` | `id` | `createdTime` | merge | Public comments. Optional URI params: `contractId`, `contractSlug`, `userId`, `order`. |
| `groups` | `id` | `createdTime` | merge | Public groups/topics. Supports interval end as `beforeTime`. Optional URI param: `availableToUserId`. |
| `group_by_slug` | - | - | replace | One group by slug. Required URI param: `group_slug`. |
| `group_by_id` | - | - | replace | One group by id. Required URI param: `group_id`. |
| `users` | `id` | - | replace | Public users. |
| `user_by_username` | `id` | - | replace | Public user by username. Required URI param: `username`. |
| `user_by_id` | `id` | - | replace | Public user by id. Required URI param: `user_id`. |
| `user_portfolio` | - | - | replace | Current public portfolio metrics for a user. Required URI param: `userId`. |
| `user_portfolio_history` | `timestamp` | `timestamp` | merge | Historical portfolio metrics. `period` is `daily`, `weekly`, `monthly`, or `allTime`. Required URI params: `userId`, `period`. |
| `user_contract_metrics` | - | - | replace | User contract metrics with market contracts. Required URI param: `userId`. Optional URI params: `order`, `perAnswer`. |
| `transactions` | `id` | `createdTime` | merge | Public transactions. Supports interval pushdown. Optional URI params: `token`, `toId`, `fromId`, `category`. |
| `leagues` | - | - | replace | Public league standings. Optional URI params: `userId`, `season`, `cohort`. |
| `boost_history` | `id` | `createdTime` | merge | Contract and post boost history. Optional URI params: `contractId`, `postId`, `userId`, `includePending`. |

## Interval Behavior

Manifold intervals are creation-time filters for the tables that support them. `bets` and `transactions` pass both interval start and interval end to the Manifold API. `search_markets` and `groups` pass only interval end as `beforeTime`. Other tables do not have a documented API-side interval filter.

Using incremental intervals for market discovery can miss markets that were created outside the interval but updated inside it. The Manifold API exposes update timestamps, but the documented ingestr source does not provide updated-time start and end filters.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/manifold.asset.yml
```

As a result of this command, Bruin will ingest data from the selected Manifold table into your destination database.
