<script setup>
const chessFiles = [
  {
    path: '.gitignore',
    language: 'text',
    content: `.env
.bruin.yml
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

  // ================================================================
  // chess-basic/ — the minimal pipeline
  // ================================================================
  {
    path: 'chess-basic/pipeline.yml',
    language: 'yaml',
    content: `name: chess_basic
catchup: false
default:
  type: ingestr
  parameters:
    source_connection: chess-default
    destination: duckdb`
  },
  {
    path: 'chess-basic/assets/raw/games.asset.yml',
    language: 'yaml',
    content: `name: chess_playground.games
parameters:
  source_table: games`
  },
  {
    path: 'chess-basic/assets/raw/profiles.asset.yml',
    language: 'yaml',
    content: `name: chess_playground.profiles
parameters:
  source_table: profiles`
  },
  {
    path: 'chess-basic/assets/reports/player_summary.sql',
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
  },

  // ================================================================
  // chess-advance/ — the feature-tour pipeline
  // ================================================================
  {
    path: 'chess-advance/pipeline.yml',
    language: 'yaml',
    content: `name: chess_advance

# SCHEDULING — cron expression or preset ("@daily", "@hourly", "@weekly").
# start_date + catchup control whether missed intervals are backfilled.
schedule: "@daily"
start_date: "2024-01-01"
catchup: false

# RUN BEHAVIOR
retries: 2              # per-asset retry attempts on failure
concurrency: 1          # max concurrent runs of this pipeline (Bruin Cloud)
max_active_steps: 8     # max assets executed in parallel within a single run

# METADATA
owner: data-platform@example.com
tags:
  - daily
  - chess
  - analytics
domains:
  - games
meta:
  cost_center: "analytics-101"

# DEFAULT CONNECTIONS — per-platform fallback when an asset omits 'connection'.
default_connections:
  duckdb: duckdb-default

# NOTIFICATIONS — fan out run outcomes to Slack / Teams / Discord / webhooks.
notifications:
  slack:
    - channel: "#data-alerts"
      success: false
      failure: true

# METADATA PUSH — export asset metadata (descriptions, columns, lineage) to a
# destination's native catalog after a successful run.
metadata_push:
  bigquery: false

# CUSTOM VARIABLES — declared as a JSON-schema-like block at the pipeline level.
# Reference them in SQL with {{ var.name }} and in Python via context.vars["name"]
# (SDK assets) or json.loads(os.environ["BRUIN_VARS"])["name"] (plain Python).
variables:
  min_games:
    type: integer
    default: 100
    minimum: 1
  rating_category:
    type: string
    enum: ["bullet", "blitz", "rapid"]
    default: "rapid"

# PIPELINE-LEVEL ASSET DEFAULTS — every asset inherits these unless it overrides.
default:
  type: ingestr
  parameters:
    source_connection: chess-default
    destination: duckdb`
  },
  {
    path: 'chess-advance/assets/raw/ingestr_games.asset.yml',
    language: 'yaml',
    content: `# FEATURE: ingestr ingestion (type inherited from pipeline defaults).
name: chess_advance.ingestr_games
description: "Ingestr ingestion example. Pulls the 'games' endpoint from the chess-default source into DuckDB. The asset's type defaults to 'ingestr' from pipeline.yml — only the source_table parameter is overridden here."

parameters:
  source_table: games`
  },
  {
    path: 'chess-advance/assets/raw/ingestr_profiles.asset.yml',
    language: 'yaml',
    content: `# FEATURE: ingestr ingestion (type inherited from pipeline defaults).
name: chess_advance.ingestr_profiles
description: "Ingestr ingestion example. Mirrors ingestr_games but loads the 'profiles' endpoint. Shows how multiple ingestr assets can share the same default source_connection + destination."

parameters:
  source_table: profiles`
  },
  {
    path: 'chess-advance/assets/raw/seed_top_players.asset.yml',
    language: 'yaml',
    content: `# FEATURE: seed asset + column-level 'accepted_values' check.
name: chess_advance.seed_top_players
description: "Seed asset example. Loads a static CSV (seed_top_players.csv) into DuckDB as a table. Demonstrates type: duckdb.seed with the 'path' parameter, and an 'accepted_values' column check that restricts the 'title' column to known chess titles."

type: duckdb.seed
parameters:
  path: seed_top_players.csv

columns:
  - name: username
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country
    type: varchar
    checks:
      - name: not_null
  - name: title
    type: varchar
    checks:
      - name: accepted_values
        value: ["GM", "IM", "FM", "WGM", "WIM"]`
  },
  {
    path: 'chess-advance/assets/raw/seed_top_players.csv',
    language: 'text',
    content: `username,country,title
MagnusCarlsen,NO,GM
Hikaru,US,GM
FabianoCaruana,US,GM
AnishGiri,NL,GM
GothamChess,US,IM`
  },
  {
    path: 'chess-advance/assets/sensor/sensor_games_ready.asset.yml',
    language: 'yaml',
    content: `# FEATURE: sensor asset that gates downstream work on a data-readiness condition.
name: chess_advance.sensor_games_ready
description: "Sensor asset example. Polls a DuckDB query every poke_interval seconds and only succeeds once the predicate returns true. Downstream transformations depend on this sensor instead of the raw table, so they won't start until the games data has actually landed."

type: duckdb.sensor.query
parameters:
  query: SELECT COUNT(*) > 0 FROM chess_advance.ingestr_games
  poke_interval: 30

depends:
  - chess_advance.ingestr_games`
  },
  {
    path: 'chess-advance/assets/transformations/python_materialization_player_ratings.py',
    language: 'python',
    content: `"""@bruin
name: chess_advance.python_materialization_player_ratings
description: "Python materialization example. A Python asset whose materialize() function returns a pandas DataFrame — Bruin writes the frame to the destination warehouse as a table. Also demonstrates reading a custom variable (rating_category) from the BRUIN_VARS environment variable and applying column-level checks to the resulting table."

connection: duckdb-default

materialization:
  type: table

depends:
  - chess_advance.seed_top_players

columns:
  - name: username
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: rating
    type: integer
    checks:
      - name: positive
  - name: category
    type: varchar

@bruin"""

import json
import os
import pandas as pd
import requests

def materialize():
    # Custom variables are exposed to plain Python assets via BRUIN_VARS.
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    category = bruin_vars.get("rating_category", "rapid")

    players = ["MagnusCarlsen", "Hikaru", "FabianoCaruana", "AnishGiri", "GothamChess"]
    rows = []
    for p in players:
        stats = requests.get(f"https://api.chess.com/pub/player/{p}/stats").json()
        rating = stats.get(f"chess_{category}", {}).get("last", {}).get("rating")
        if rating is not None:
            rows.append({"username": p, "rating": rating, "category": category})

    return pd.DataFrame(rows)`
  },
  {
    path: 'chess-advance/assets/transformations/incremental_sql_daily_games.sql',
    language: 'sql',
    content: `/* @bruin

name: chess_advance.incremental_sql_daily_games
description: "Incremental SQL materialization example. Uses strategy: delete+insert with incremental_key: game_date so each run only recomputes the partition within the run's time window. Demonstrates the built-in start_date and end_date variables, which Bruin injects from the run's interval."

type: duckdb.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: game_date

depends:
  - chess_advance.sensor_games_ready

columns:
  - name: game_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: game_count
    type: integer
    checks:
      - name: positive

@bruin */

-- {{ start_date }} and {{ end_date }} are built-in variables from the run interval.
SELECT
    CAST(end_time AS DATE) AS game_date,
    COUNT(*)               AS game_count
FROM chess_advance.ingestr_games
WHERE CAST(end_time AS DATE) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY 1`
  },
  {
    path: 'chess-advance/assets/reports/sql_with_checks_player_summary.sql',
    language: 'sql',
    content: `/* @bruin

name: chess_advance.sql_with_checks_player_summary
description: "Column + custom checks and custom variables example. A standard SQL table asset that showcases several column-level checks (not_null, unique, positive, min, max) alongside two custom_checks that run arbitrary SQL and assert on the returned value. Also uses the pipeline-level custom variable 'min_games' via {{ var.min_games }} in the HAVING clause."

type: duckdb.sql
materialization:
  type: table

depends:
  - chess_advance.sensor_games_ready
  - chess_advance.ingestr_profiles
  - chess_advance.seed_top_players

# Column-level checks run after the asset materializes.
columns:
  - name: username
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: total_games
    type: integer
    checks:
      - name: positive
  - name: white_win_rate
    type: float
    checks:
      - name: min
        value: 0
      - name: max
        value: 100

# Custom checks execute arbitrary SQL and assert on the returned value.
custom_checks:
  - name: covers_all_top_players
    description: "Every seeded top player should appear in the summary."
    query: |
      SELECT COUNT(*)
      FROM chess_advance.seed_top_players tp
      LEFT JOIN chess_advance.sql_with_checks_player_summary ps USING (username)
      WHERE ps.username IS NULL
    value: 0
  - name: has_grandmasters
    description: "At least one GM must be present in the summary."
    query: |
      SELECT COUNT(*)
      FROM chess_advance.sql_with_checks_player_summary ps
      JOIN chess_advance.seed_top_players tp USING (username)
      WHERE tp.title = 'GM'
    value: 1
    blocking: true

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
    FROM chess_advance.ingestr_games g
)

SELECT
    p.username,
    p.aid,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS white_wins,
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS black_wins,
    COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END) AS white_games,
    COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END) AS black_games,
    ROUND(COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0
          / NULLIF(COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END), 0), 2) AS white_win_rate,
    ROUND(COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0
          / NULLIF(COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END), 0), 2) AS black_win_rate
FROM chess_advance.ingestr_profiles p
LEFT JOIN game_results g
       ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
-- {{ var.min_games }} is the custom variable declared in pipeline.yml.
HAVING COUNT(*) >= {{ var.min_games }}
ORDER BY total_games DESC`
  },
  {
    path: 'chess-advance/assets/reports/python_sdk_rating_insights.py',
    language: 'python',
    content: `"""@bruin
name: chess_advance.python_sdk_rating_insights
description: "Bruin Python SDK + Python materialization combined. Queries the warehouse via 'from bruin import query, context' and returns the resulting DataFrame from materialize(), so Bruin writes it back as a table. Reads both built-in variables (context.start_date, context.end_date) and custom variables (context.vars['min_games']), and attaches column-level checks to the materialized output."

connection: duckdb-default

materialization:
  type: table

depends:
  - chess_advance.sql_with_checks_player_summary
  - chess_advance.python_materialization_player_ratings

columns:
  - name: username
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: total_games
    type: integer
    checks:
      - name: positive
  - name: rating
    type: integer
  - name: rating_bucket
    type: varchar
    checks:
      - name: accepted_values
        value: ["elite", "strong", "solid", "unrated"]

@bruin"""

import pandas as pd
from bruin import query, context

def materialize():
    # context.vars holds the pipeline's custom variables with any CLI overrides applied.
    min_games = context.vars.get("min_games", 100)

    df = query(f"""
        SELECT
            ps.username,
            ps.total_games,
            ps.white_win_rate,
            ps.black_win_rate,
            pr.rating
        FROM chess_advance.sql_with_checks_player_summary ps
        LEFT JOIN chess_advance.python_materialization_player_ratings pr USING (username)
        WHERE ps.total_games >= {min_games}
    """)

    def bucket(rating):
        if rating is None or pd.isna(rating):
            return "unrated"
        if rating >= 2700:
            return "elite"
        if rating >= 2500:
            return "strong"
        return "solid"

    df["rating_bucket"] = df["rating"].apply(bucket)

    print(f"Analysis window: {context.start_date} -> {context.end_date}")
    print(f"Players meeting threshold: {len(df)}")

    # Returning the DataFrame triggers Bruin's Python materialization — it writes
    # the frame to chess_advance.python_sdk_rating_insights as a table.
    return df.sort_values("rating", ascending=False)`
  },
  {
    path: 'chess-advance/assets/reports/dashboard_chess_overview.asset.yml',
    language: 'yaml',
    content: `# FEATURE: dashboard asset — tracks an external BI artifact in lineage.
name: chess_advance.dashboard_chess_overview
description: "Dashboard asset example. Declares an external Tableau dashboard so it shows up in the Bruin lineage graph as a terminal node fed by its upstream reports. No parameters are required for a lineage-only dashboard; just name + type + depends."

type: tableau
owner: analytics@example.com

depends:
  - chess_advance.sql_with_checks_player_summary
  - chess_advance.python_sdk_rating_insights

tags:
  - dashboard
  - chess`
  }
]
</script>

# Explore Example Project

This tutorial ships as a single Bruin project with two pipelines side by side:

- **`chess-basic/`** — a minimal "hello world" pipeline: connections, one `pipeline.yml`, a couple of ingestr assets, and a single SQL report. A great first look at Bruin.
- **`chess-advance/`** — the same chess dataset expanded into a tour of Bruin's features: sensors, seeds, Python materialization, incremental SQL, the Python SDK, column & custom checks, custom variables, scheduling, notifications, and dashboards.

Both pipelines share the root `.bruin.yml` (connections) and `.gitignore`. Browse the tree on the left to explore each file.

<CodeViewer :files="chessFiles" title="chess-project/" :collapsed-folders="['chess-advance']" />

## What's inside `chess-basic/`

- **`pipeline.yml`** — `name: chess_basic`, with `type: ingestr` as the default asset type.
- **`assets/raw/games.asset.yml`**, **`profiles.asset.yml`** — Ingestr assets that pull the Chess.com `games` and `profiles` endpoints into DuckDB.
- **`assets/reports/player_summary.sql`** — A single SQL report that joins and aggregates into a player-level win-rate table with one column check.

That's it — enough to run an end-to-end ingest + transform flow locally.

## What's inside `chess-advance/`

- **`pipeline.yml`** — `name: chess_advance`, plus pipeline-level `schedule`, `retries`, `concurrency`, `notifications`, `tags`, `default_connections`, and **custom variables** (`min_games`, `rating_category`).
- **`assets/raw/`** — Raw landing layer: ingestion and seeds.
  - **`ingestr_games.asset.yml`**, **`ingestr_profiles.asset.yml`** — Ingestr assets that inherit `type: ingestr` from pipeline defaults.
  - **`seed_top_players.asset.yml`** (+ `seed_top_players.csv`) — `duckdb.seed` loading static CSV data, with an `accepted_values` column check.
- **`assets/sensor/`** — Readiness gates.
  - **`sensor_games_ready.asset.yml`** — `duckdb.sensor.query` that polls until games have landed before downstream work runs.
- **`assets/transformations/`** — Intermediate processing.
  - **`python_materialization_player_ratings.py`** — Python asset with `materialize()` that returns a DataFrame to be written as a table; also reads the `rating_category` custom variable from `BRUIN_VARS`.
  - **`incremental_sql_daily_games.sql`** — Incremental SQL (`delete+insert` keyed on `game_date`) showcasing the built-in <code v-pre>{{ start_date }}</code> / <code v-pre>{{ end_date }}</code> variables.
- **`assets/reports/`** — Consumer-facing outputs.
  - **`sql_with_checks_player_summary.sql`** — Aggregate table demonstrating column checks (`not_null`, `unique`, `positive`, `min`, `max`), `custom_checks` with `value` / `blocking`, and the <code v-pre>{{ var.min_games }}</code> custom variable.
  - **`python_sdk_rating_insights.py`** — Bruin Python SDK **+** Python materialization combined. Uses `from bruin import query, context` to pull data from the warehouse and returns the enriched DataFrame from `materialize()` so Bruin writes it back as a table. Reads both built-in (`context.start_date`) and custom (`context.vars["min_games"]`) variables.
  - **`dashboard_chess_overview.asset.yml`** — `tableau` dashboard asset that appears as a terminal node in lineage.

## Setup

Fill in `.bruin.yml` with your connections. You can read more about connections [here](/connections/overview).

Both pipelines share the pre-configured list of 10 popular Chess.com players. You can modify the `players` list in your chess connection to track different players.

## Running a Pipeline

Run a whole pipeline by pointing at its folder:

```shell
bruin run chess-basic
# or
bruin run chess-advance
```

Or run a single asset:

```shell
bruin run chess-advance/assets/reports/sql_with_checks_player_summary.sql
```

Override a custom variable for a single run (chess-advance only):

```shell
bruin run --var min_games=250 --var rating_category=blitz chess-advance
```

You can optionally pass a `--downstream` flag to run an asset with all of its downstreams.
