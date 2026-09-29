# Lumify

[Lumify](https://lumify.ai) is an agent-ready sports intelligence API covering schedules, live scores, odds, betting splits, and explainable bet confidence across 8+ sports.

Bruin supports Lumify as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest sports reference data and event windows into your data platform.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    lumify:
        - name: "lumify"
          api_key: "lmfy-your-key"
          sport: "nba"
```

- `api_key` (required): Lumify API key (`lmfy-...`). Get a free instant key (no signup) at [lumify.ai/docs/ai](https://lumify.ai/docs/ai), or create a persistent key at [lumify.ai/api-keys](https://lumify.ai/api-keys).
- `sport` (optional): Sport slug filter applied to `seasons`, `teams`, `players`, `events`, and `leagues` (for example `nba`, `nfl`, `mlb`, `nhl`, `soccer`, `tennis`, `golf`, `mma`).
- `league` (optional): League slug filter applied to `teams` and `events`.
- `base_url` (optional): Overrides the API base URL. Defaults to `https://lumify.ai`.

### Step 2: Create an asset file for data ingestion

Create a YAML file (e.g. lumify_ingestion.yml) inside the assets folder:

```yaml
name: public.teams
type: ingestr

parameters:
  source_connection: lumify
  source_table: 'teams'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Set to `ingestr`.
- `source_connection`: The name of the Lumify connection defined in `.bruin.yml`.
- `source_table`: One of the tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/lumify_ingestion.yml
```

## Available Source Tables

| Table | Primary Key | Incremental Strategy | Details |
| ----- | ----------- | -------------------- | ------- |
| `sports` | `id` | replace | Sports from `/v1/sports`. Nested `leagues` are kept as JSON. |
| `leagues` | `id` | replace | Flattened leagues with `sport_id`, `sport_slug`, and `sport_name`. |
| `seasons` | `id` | replace | Seasons from `/v1/seasons`. Nested `sport` and `league` kept as JSON. |
| `teams` | `id` | replace | Paginated teams from `/v1/teams`. |
| `players` | `id` | replace | Paginated players from `/v1/players`. |
| `events` | `id` | merge | Events from `/v1/events` for an interval window, with scores/participants included. |

## Incremental behavior and limitations

- The API key is sent as `Authorization: Bearer <api_key>`.
- `teams`, `players`, and `events` handle Lumify `after_id` cursor pagination automatically.
- `events` uses `--interval-start` / `--interval-end` when provided. If omitted, the source defaults to the last 7 days through now. Intervals longer than 90 days are chunked to match the Lumify API limit.
- Each successful request consumes Lumify API credits. Prefer scoped `sport` / `league` filters and tight event intervals in production syncs.
