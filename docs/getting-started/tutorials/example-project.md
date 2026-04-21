<script setup>
const chessFiles = [
  {
    path: '.gitignore',
    language: 'text',
    content: `.env
duckdb.db
duckdb.db.wal
__pycache__/
.venv/`
  },
  {
    path: '.bruin.yml',
    language: 'yaml',
    content: `default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "FabianoCaruana"
            - "Hikaru"
            - "MagnusCarlsen"
            - "GothamChess"
            - "DanielNaroditsky"
            - "AnishGiri"
            - "Firouzja2003"
            - "LevonAronian"
            - "WesleySo"
            - "GarryKasparov"`
  },
  {
    path: 'chess-pipeline/pipeline.yml',
    language: 'yaml',
    content: `name: chess_duckdb
catchup: false
default:
  type: ingestr
  parameters:
    source_connection: chess-default
    destination: duckdb`
  },
  {
    path: 'chess-pipeline/assets/raw/games.asset.yml',
    language: 'yaml',
    content: `name: chess_playground.games
parameters:
  source_table: games`
  },
  {
    path: 'chess-pipeline/assets/raw/profiles.asset.yml',
    language: 'yaml',
    content: `name: chess_playground.profiles
parameters:
  source_table: profiles`
  },
  {
    path: 'chess-pipeline/assets/reports/player_summary.sql',
    language: 'sql',
    content: `/* @bruin

name: chess_playground.player_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.games
   - chess_playground.profiles

columns:
  - name: total_games
    type: integer
    description: "the games"
    checks:
      - name: positive

@bruin */

WITH game_results AS (
    SELECT
        CASE
            WHEN g.white->>'result' = 'win' THEN g.white->>'@id'
            WHEN g.black->>'result' = 'win' THEN g.black->>'@id'
            ELSE NULL
            END AS winner_aid,
        g.white->>'@id' AS white_aid,
    g.black->>'@id' AS black_aid
FROM chess_playground.games g
)

SELECT
    p.username,
    p.aid,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS white_wins,
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS black_wins,
    COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END) AS white_games,
    COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END) AS black_games,
    ROUND(COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END), 0), 2) AS white_win_rate,
    ROUND(COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END), 0), 2) AS black_win_rate
FROM chess_playground.profiles p
LEFT JOIN game_results g
       ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
ORDER BY total_games DESC`
  }
]
</script>

# Explore Example Project

This is a complete Bruin project that ingests chess data from the [Chess.com API](https://www.chess.com/news/view/published-data-api) into a local DuckDB database and builds a player summary report. Browse the files below to see how a real project is structured.

<CodeViewer :files="chessFiles" title="chess-project/" />

## What's Inside

The project has two root config files and a pipeline with assets organized by layer:

- **`.bruin.yml`** — Project-level configuration: DuckDB and Chess.com connections.
- **`chess-pipeline/pipeline.yml`** — Pipeline definition using `ingestr` to pull from Chess.com into DuckDB.
- **`assets/raw/`** — Ingestion assets that land source data as-is:
  - **`games.asset.yml`** — Chess game data.
  - **`profiles.asset.yml`** — Player profile data.
- **`assets/reports/`** — Transformed assets that build on raw data:
  - **`player_summary.sql`** — Joins games and profiles to produce win rates by color.

## Setup

Fill in `.bruin.yml` with your connections. You can read more about connections [here](/connections/overview).

The template comes pre-configured with 10 popular Chess.com players. You can modify the `players` list in your chess connection to track different players.

## Running the Pipeline

Run the whole pipeline:

```shell
bruin run chess-pipeline/pipeline.yml
```

Or run a single asset:

```shell
bruin run chess-pipeline/assets/reports/player_summary.sql
```

You can optionally pass a `--downstream` flag to run an asset with all of its downstreams.
