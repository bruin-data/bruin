# API-Football

[API-Football](https://www.api-football.com/) provides soccer data from API-SPORTS, including World Cup teams, fixtures, standings, players, venues, and match events.

Bruin supports API-Football as a source for [Ingestr assets](/assets/ingestr), so you can ingest soccer data into your data warehouse.

For the underlying connector reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/api-football.html).

Follow the steps below to set up API-Football as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

The connection routes to a specific league and season. The default configuration targets World Cup 2026 with `league=1` and `season=2026`.

```yaml
    connections:
      apifootball:
        - name: "apifootball_worldcup"
          api_key: "<your-api-key>"
          league: "1"
          season: "2026"
```

- `name`: Name of the connection.
- `api_key`: API-Football API key (sent in the `x-apisports-key` header). Required.
- `league` (optional): API-Football league ID. Defaults to `1` (FIFA World Cup).
- `season` (optional): Season year. Defaults to `2026`.
- `timezone` (optional): Timezone passed to fixture requests.
- `base_url` (optional): Overrides the API base URL. Defaults to `https://v3.football.api-sports.io`.

### Step 2: Create an asset file for data ingestion

```yaml
name: public.api_football_matches
type: ingestr
connection: postgres

parameters:
  source_connection: apifootball_worldcup
  source_table: 'matches'
  destination: postgres
```

- `name`: Name of the asset.
- `type`: Always `ingestr` for API-Football.
- `connection`: Destination connection name.
- `source_connection`: Name of the API-Football connection defined in `.bruin.yml`.
- `source_table`: One of the tables listed below.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `teams` | `id` | - | replace | Teams for the configured league/season from `/teams`. |
| `stadiums` | `id` | - | merge | Venues derived from fixtures, hydrated through `/venues`. |
| `group_standings` | `league_id, season, group_name, team_id` | - | merge | Group standings from `/standings`. |
| `matches` | `id` | - | merge | Fixtures from `/fixtures`. Honors `--interval-start`/`--interval-end` via the `from`/`to` filter. |
| `players` | `id` | - | replace | Paginated player rows from `/players`. |
| `match_events` | `event_key` | - | merge | Events per fixture from `/fixtures/events`. Accumulates across runs. |

Nested API-Football objects are preserved as JSON columns in the destination; schema inference derives types from the actual payload.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.api_football.asset.yml
```

As a result of this command, Bruin will ingest data from the configured API-Football endpoint into your destination database.
