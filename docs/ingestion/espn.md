# ESPN

[ESPN](https://www.espn.com/) exposes a public Site API that returns JSON for teams, scores, standings, and news across the major US sports plus international soccer.

Bruin supports ESPN as a source for [Ingestr assets](/assets/ingestr), so you can ingest sports data into your data warehouse without an API key.

For the underlying connector reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/espn.html).

Follow the steps below to set up ESPN as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

ESPN's public endpoints are auth-less, so the connection is purely a routing target — point it at the sport and league you want and reuse it across assets.

```yaml
    connections:
      espn:
        - name: "espn_nfl"
          sport: "football"
          league: "nfl"
        - name: "espn_epl"
          sport: "soccer"
          league: "eng.1"
        - name: "espn_nba_2025"
          sport: "basketball"
          league: "nba"
          season: "2025"
          limit: 50
```

- `name`: Name of the connection.
- `sport` (optional): ESPN sport slug. Defaults to `football`.
- `league` (optional): ESPN league slug. Defaults to `nfl`.
- `season` (optional): Season year, passed to scoreboard and standings requests. Useful for pinning a connection to a specific historical season.
- `limit` (optional): Request limit for scoreboard and news. ESPN defaults to `100` when not set.
- `base_url` (optional): Overrides the ESPN API base URL. Defaults to `https://site.api.espn.com`.

The scoreboard window itself is taken from `--interval-start` / `--interval-end` on the run, which ingestr converts to ESPN's `dates=YYYYMMDD[-YYYYMMDD]` query parameter.

### Step 2: Create an asset file for data ingestion

```yaml
name: public.espn_scoreboard
type: ingestr
connection: postgres

parameters:
  source_connection: espn_nfl
  source_table: 'scoreboard'
  destination: postgres
```

- `name`: Name of the asset.
- `type`: Always `ingestr` for ESPN.
- `connection`: Destination connection name.
- `source_connection`: Name of the ESPN connection defined in `.bruin.yml`.
- `source_table`: One of the tables listed below.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `teams` | `id` | - | `replace` | Loads teams from `/apis/site/v2/sports/{sport}/{league}/teams`. Roster snapshot. |
| `scoreboard` | `id` | - | `merge` | Loads scoreboard events from `/apis/site/v2/sports/{sport}/{league}/scoreboard`. Use `merge` to accumulate events across interval runs. |
| `competitors` | `event_id`, `competition_id`, `team_id` | - | `merge` | Fans out each scoreboard event into one row per competitor/team. |
| `standings` | `league_id`, `group_id`, `season`, `team_id` | - | `replace` | Loads standings from `/apis/v2/sports/{sport}/{league}/standings`. Latest snapshot for the given season. |
| `news` | `id` | - | `merge` | Loads latest league news articles from `/apis/site/v2/sports/{sport}/{league}/news`. Accumulates over runs. |

Nested ESPN objects are preserved as JSON columns in the destination; schema inference derives types from the actual payload, so column shapes vary across sports and leagues.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.espn.asset.yml
```

As a result of this command, Bruin will ingest data from the configured ESPN endpoint into your destination database.
