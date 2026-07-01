# Polymarket

[Polymarket](https://polymarket.com/) is a prediction market platform. Bruin supports Polymarket as a public read-only source for [Ingestr assets](/assets/ingestr), including markets, events, prices, order books, trades, and public wallet activity.

No API key is required for the supported Polymarket tables.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

For unfiltered tables, add a lowercase `polymarket` connection:

```yaml
connections:
  polymarket:
    - name: polymarket
```

Polymarket filters are URI parameters in ingestr. Add the relevant fields to a connection when a source table requires filters or identifiers:

```yaml
connections:
  polymarket:
    - name: polymarket_open_markets
      closed: "false"

    - name: polymarket_orderbook
      token_id: "<clob-token-id>"

    - name: polymarket_event_comments
      parent_entity_id: "<event-id>"
      parent_entity_type: "Event"
```

Use separate Polymarket connections for assets that need different table-specific URI parameters. Quote values such as booleans and numeric IDs to pass them to ingestr exactly as URI query values.

Bruin connection fields use snake_case. They are mapped to the URI parameters expected by ingestr, including `taker_only` to `takerOnly`, `filter_type` to `filterType`, `filter_amount` to `filterAmount`, and `event_id` to `eventId`.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file such as `assets/polymarket_markets.yml`:

```yaml
name: public.polymarket_markets
type: ingestr

parameters:
  source_connection: polymarket_open_markets
  source_table: 'markets'

  destination: duckdb
  schema_naming: direct
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for Polymarket.
- `source_connection`: The Polymarket connection name defined in `.bruin.yml`.
- `source_table`: The Polymarket table to ingest.
- `destination`: The destination connection name.
- `schema_naming`: `direct` is recommended for Polymarket tables because provider columns and keys can use camelCase.

### Step 3: Run the asset

```bash
bruin run assets/polymarket_markets.yml
```

## Supported Connection Fields

The Polymarket connection can include these optional URI fields. Some source tables require one or more of them, as shown in the source table list.

| Bruin field | Ingestr URI parameter |
| ----------- | --------------------- |
| `order` | `order` |
| `ascending` | `ascending` |
| `slug` | `slug` |
| `closed` | `closed` |
| `live` | `live` |
| `active` | `active` |
| `archived` | `archived` |
| `featured` | `featured` |
| `tag_id` | `tag_id` |
| `tag_slug` | `tag_slug` |
| `series_id` | `series_id` |
| `include_chat` | `include_chat` |
| `include_template` | `include_template` |
| `include_markets` | `include_markets` |
| `clob_token_ids` | `clob_token_ids` |
| `condition_ids` | `condition_ids` |
| `question_ids` | `question_ids` |
| `related_tags` | `related_tags` |
| `include_tag` | `include_tag` |
| `rfq_enabled` | `rfq_enabled` |
| `limit` | `limit` |
| `offset` | `offset` |
| `parent_entity_id` | `parent_entity_id` |
| `parent_entity_type` | `parent_entity_type` |
| `market` | `market` |
| `user` | `user` |
| `q` | `q` |
| `events_status` | `events_status` |
| `markets_status` | `markets_status` |
| `token_id` | `token_id` |
| `side` | `side` |
| `interval` | `interval` |
| `fidelity` | `fidelity` |
| `taker_only` | `takerOnly` |
| `filter_type` | `filterType` |
| `filter_amount` | `filterAmount` |
| `event_id` | `eventId` |
| `type` | `type` |

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `events` | `id` | `updatedAt` | merge | Polymarket events from Gamma keyset pagination. Optional connection fields: `order`, `ascending`, `slug`, `closed`, `live`, `active`, `archived`, `featured`, `tag_id`, `tag_slug`, `series_id`, `include_chat`, `include_template`, `include_markets`. |
| `markets` | `id` | `updatedAt` | merge | Polymarket markets from Gamma keyset pagination. Optional connection fields: `order`, `ascending`, `slug`, `closed`, `active`, `archived`, `clob_token_ids`, `condition_ids`, `question_ids`, `tag_id`, `related_tags`, `include_tag`, `rfq_enabled`. |
| `tags` | `id` | `updatedAt` | merge | Tags and categories. Optional connection fields: `limit`, `offset`, `order`, `ascending`, `include_template`. |
| `series` | `id` | `updatedAt` | merge | Event series metadata. Optional connection fields: `limit`, `offset`, `order`, `ascending`, `closed`, `active`, `archived`. |
| `comments` | `id` | `createdAt` | merge | Public comments. Event comments use `parent_entity_type: "Event"`. Required connection fields: `parent_entity_id`, `parent_entity_type`. Optional connection fields: `market`, `user`. |
| `search` | - | - | replace | Public search results. Optional connection fields: `q`, `events_status`, `markets_status`. |
| `orderbook` | `asset_id` | - | merge | CLOB order book for one token. Required connection field: `token_id`. |
| `price` | - | - | replace | Best price for one token side, `BUY` or `SELL`. Required connection fields: `token_id`, `side`. |
| `midpoint` | - | - | replace | Current midpoint price. Required connection field: `token_id`. |
| `spread` | - | - | replace | Current bid/ask spread. Required connection field: `token_id`. |
| `last_trade_price` | - | - | replace | Last trade price and side. Required connection field: `token_id`. |
| `price_history` | `t` | `t` | merge | Historical price points for a CLOB asset id. Required connection field: `market`. Optional connection fields: `interval`, `fidelity`. |
| `trades` | `transactionHash` | `timestamp` | merge | Public trade history from the Data API. Optional connection fields: `taker_only`, `filter_type`, `filter_amount`, `market`, `event_id`, `user`, `side`. |
| `positions` | - | - | replace | Current positions for a public wallet. Required connection field: `user`. Optional connection field: `market`. |
| `closed_positions` | - | - | replace | Closed positions for a public wallet. Required connection field: `user`. Optional connection field: `market`. |
| `activity` | `transactionHash` | `timestamp` | merge | Public wallet activity. Required connection field: `user`. Optional connection field: `type`. |

## Notes

- Polymarket has separate public Gamma, CLOB, and Data APIs. The ingestr connector maps each `source_table` to one read-only endpoint.
- CLOB pricing and order book tables use CLOB token IDs, not Gamma market IDs. Ingest `markets` first and read token IDs from the market payload.
- When using interval modifiers, only `events`, `markets`, and `price_history` push interval filters to the Polymarket API. For `events` and `markets`, intervals filter scheduled date ranges, not update times.
- The official ingestr source returns selected stable columns plus a `raw` JSON column containing the full source payload.
