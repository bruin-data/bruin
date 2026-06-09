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

| Table | Required URI params | Optional URI params | PK | Inc Key | Inc Strategy | Details |
|-------|---------------------|---------------------|----|---------|--------------|---------|
| `markets` | - | `sort`, `order`, `userId`, `groupId` | `id` | `createdTime` | merge | Public market list. |
| `search_markets` | - | `term`, `sort`, `filter`, `creatorId`, `contractType`, `topicSlug`, `minLiquidity`, `maxLiquidity` | `id` | `createdTime` | merge | Search and filter markets. Supports interval end as `beforeTime`. |
| `market_by_id` | `market_id` | - | `id` | - | replace | Full market by id. |
| `market_by_slug` | `contract_slug` | - | `id` | - | replace | Full market by slug. |
| `market_probability` | `market_id` | - | - | - | replace | Current probability for one market. Multiple choice market details are returned in `raw`. |
| `market_probabilities` | `ids` | - | - | - | replace | Current probabilities for up to 100 market ids. Repeat `ids` with `query_param_lists`. |
| `market_positions` | `market_id` | `order`, `top`, `bottom`, `userId`, `answerId` | - | - | replace | Position information for one market. |
| `bets` | - | `userId`, `username`, `contractId`, `contractSlug`, `kinds`, `order` | `id` | `createdTime` | merge | Public bets. Supports interval pushdown. |
| `comments` | - | `contractId`, `contractSlug`, `userId`, `order` | `id` | `createdTime` | merge | Public comments. |
| `groups` | - | `availableToUserId` | `id` | `createdTime` | merge | Public groups/topics. Supports interval end as `beforeTime`. |
| `group_by_slug` | `group_slug` | - | - | - | replace | One group by slug. |
| `group_by_id` | `group_id` | - | - | - | replace | One group by id. |
| `users` | - | - | `id` | - | replace | Public users. |
| `user_by_username` | `username` | - | `id` | - | replace | Public user by username. |
| `user_by_id` | `user_id` | - | `id` | - | replace | Public user by id. |
| `user_portfolio` | `userId` | - | - | - | replace | Current public portfolio metrics for a user. |
| `user_portfolio_history` | `userId`, `period` | - | `timestamp` | `timestamp` | merge | Historical portfolio metrics. `period` is `daily`, `weekly`, `monthly`, or `allTime`. |
| `user_contract_metrics` | `userId` | `order`, `perAnswer` | - | - | replace | User contract metrics with market contracts. |
| `transactions` | - | `token`, `toId`, `fromId`, `category` | `id` | `createdTime` | merge | Public transactions. Supports interval pushdown. |
| `leagues` | - | `userId`, `season`, `cohort` | - | - | replace | Public league standings. |
| `boost_history` | - | `contractId`, `postId`, `userId`, `includePending` | `id` | `createdTime` | merge | Contract and post boost history. |

## Interval Behavior

Manifold intervals are creation-time filters for the tables that support them. `bets` and `transactions` pass both interval start and interval end to the Manifold API. `search_markets` and `groups` pass only interval end as `beforeTime`. Other tables do not have a documented API-side interval filter.

Using incremental intervals for market discovery can miss markets that were created outside the interval but updated inside it. The Manifold API exposes update timestamps, but the documented ingestr source does not provide updated-time start and end filters.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/manifold.asset.yml
```

As a result of this command, Bruin will ingest data from the selected Manifold table into your destination database.
