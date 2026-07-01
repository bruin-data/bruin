# football-data.org

[football-data.org](https://www.football-data.org/) provides soccer competition data, including World Cup teams, fixtures, standings, players, and plan-dependent deep match and squad data.

Bruin supports football-data.org as a source for [Ingestr assets](/assets/ingestr), so you can ingest soccer data into your data warehouse.

For the underlying connector reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/football-data-org.html).

Follow the steps below to set up football-data.org as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

The connection routes to a specific competition and season. The default configuration targets World Cup 2026 with `competition=WC` and `season=2026`.

```yaml
    connections:
      footballdata:
        - name: "footballdata_worldcup"
          api_key: "<your-api-key>"
          competition: "WC"
          season: "2026"
```

- `name`: Name of the connection.
- `api_key`: football-data.org API token (sent in the `X-Auth-Token` header). Required.
- `competition` (optional): Competition code or ID. Defaults to `WC` (FIFA World Cup).
- `season` (optional): Season year. Defaults to `2026`.
- `base_url` (optional): Overrides the API base URL. Defaults to `https://api.football-data.org/v4`.
- `matchday` (optional): Restrict `matches`/`match_events` to a single matchday.
- `status` (optional): Filter matches by status (e.g. `SCHEDULED`, `FINISHED`).
- `stage` (optional): Filter matches by stage (e.g. `GROUP_STAGE`, `FINAL`).
- `group` (optional): Filter matches by group (e.g. `GROUP_A`).
- `unfold_goals`, `unfold_bookings`, `unfold_subs`, `unfold_lineups` (optional): `true`/`false` flags that request deep match arrays via the `X-Unfold-*` headers. Required for `match_events`; need Deep Data plan access.

### Step 2: Create an asset file for data ingestion

```yaml
name: public.football_data_matches
type: ingestr
connection: postgres

parameters:
  source_connection: footballdata_worldcup
  source_table: 'matches'
  destination: postgres
```

- `name`: Name of the asset.
- `type`: Always `ingestr` for football-data.org.
- `connection`: Destination connection name.
- `source_connection`: Name of the football-data.org connection defined in `.bruin.yml`.
- `source_table`: One of the tables listed below.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `teams` | `id` | - | merge | Teams for the configured competition/season from `/competitions/<competition>/teams`. |
| `stadiums` | `venue_key` | - | replace | Venues derived from teams and matches; the originating object is kept under `raw`. |
| `group_standings` | `competition_id, season_id, stage, standing_type, group_name, team_id` | - | replace | Standings table from `/competitions/<competition>/standings`. |
| `matches` | `id` | - | merge | Matches from `/competitions/<competition>/matches`. Honors `--interval-start`/`--interval-end` via the `dateFrom`/`dateTo` filter. |
| `players` | `team_id, id` | - | replace | Squad members hydrated through `/teams/<id>`. Requires plan access. |
| `match_events` | `event_key` | - | merge | Goal, booking, and substitution events normalized from match unfold arrays. Requires the Deep Data plan. |

Nested football-data.org objects are preserved as JSON columns in the destination; schema inference derives types from the actual payload.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.football_data.asset.yml
```

As a result of this command, Bruin will ingest data from the configured football-data.org endpoint into your destination database.
