# BallDontLie FIFA

[BallDontLie](https://www.balldontlie.io/) provides FIFA World Cup data, including teams, stadiums, matches, players, rosters, lineups, events, and match analytics.

Bruin supports BallDontLie FIFA as a source for [Ingestr assets](/assets/ingestr), so you can ingest World Cup data into your data warehouse.

For the underlying connector reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/balldontlie.html).

Follow the steps below to set up BallDontLie FIFA as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

The connection targets a single World Cup edition via `season`. The default configuration targets World Cup 2026.

```yaml
    connections:
      balldontlie:
        - name: "balldontlie_worldcup"
          api_key: "<your-api-key>"
          season: "2026"
```

- `name`: Name of the connection.
- `api_key`: BallDontLie API key (sent in the `Authorization` header). Required.
- `season` (optional): World Cup edition. One of `2018`, `2022`, `2026`. Defaults to `2026`.
- `base_url` (optional): Overrides the API base URL. Defaults to `https://api.balldontlie.io`.

### Step 2: Create an asset file for data ingestion

```yaml
name: public.balldontlie_matches
type: ingestr

parameters:
  source_connection: balldontlie_worldcup
  source_table: 'matches'
  destination: postgres
```

- `name`: Name of the asset.
- `type`: Always `ingestr` for BallDontLie FIFA.
- `source_connection`: Name of the BallDontLie connection defined in `.bruin.yml`.
- `source_table`: One of the tables listed below.
- `destination`: The destination platform/type, for example `postgres`.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `teams` | `id` | - | replace | World Cup teams. Free tier. |
| `stadiums` | `id` | - | replace | Stadium metadata. Free tier. |
| `group_standings` | `season_year, team_id` | - | replace | Group standings; nested season/team/group objects kept as JSON. Requires ALL-STAR. |
| `matches` | `id` | - | replace | Matches; nested objects kept as JSON. Requires GOAT. |
| `players` | `id` | - | replace | Player profiles. Requires GOAT. |
| `rosters` | `season_year, team_id, player_id` | - | replace | Season rosters; nested player object kept as JSON. Requires GOAT. |
| `match_lineups` | `match_id, team_id, player_id` | - | replace | Match lineups; nested player object kept as JSON. Requires GOAT. |
| `match_events` | `id` | - | replace | Match incidents (goals, cards, substitutions, shootouts). Requires GOAT. |
| `player_match_stats` | `match_id, player_id` | - | replace | Player match statistics. Requires GOAT. |
| `team_match_stats` | `match_id, team_id` | - | replace | Team match statistics. Requires GOAT. |
| `match_shots` | `id` | - | replace | Shot-level data. Requires GOAT. |
| `match_momentum` | `match_id, minute` | - | replace | Match momentum data. Requires GOAT. |
| `match_best_players` | `match_id, player_id` | - | replace | Best-player summaries by match. Requires GOAT. |
| `match_avg_positions` | `match_id, player_id` | - | replace | Average player positions by match. Requires GOAT. |
| `match_team_form` | `match_id, team_id` | - | replace | Team form data by match. Requires GOAT. |

Nested BallDontLie objects are preserved as JSON columns in the destination; schema inference derives types from the actual payload. The API has no time-interval filter, so each run is a full season fetch.

BallDontLie's free tier only includes `teams` and `stadiums`; `group_standings` requires the ALL-STAR plan, and the remaining match/player/event tables require the GOAT plan.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.balldontlie.asset.yml
```

As a result of this command, Bruin will ingest data from the configured BallDontLie endpoint into your destination database.
