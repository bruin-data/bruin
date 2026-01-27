# Overview - End-to-End Data Platform

This hands-on tutorial guides you through building a **complete NYC Taxi data pipeline** from scratch using Bruin—a unified CLI tool for data ingestion, transformation, and quality.

You'll learn to build a production-ready ELT pipeline that:
- **Ingests** real NYC taxi trip data from public APIs using Python
- **Transforms** and cleans raw data with SQL, applying incremental strategies and deduplication
- **Reports** aggregated analytics with built-in quality checks
- **Deploys** to cloud infrastructure (BigQuery)

This is a learn-by-doing experience with AI assistance available through Bruin MCP. Follow the comprehensive step-by-step tutorial section below.

## Learning Goals

- Understand how Bruin projects are structured (`pipeline/pipeline.yml` + `pipeline/assets/`)
- Use **materialization strategies** intentionally (append, time_interval, etc.)
- Declare **dependencies** and explore lineage (`bruin lineage`)
- Apply **metadata** (columns, primary keys, descriptions) and **quality checks**
- Parameterize runs with **pipeline variables**

## Tutorial Outline

- **Part 1**: What is a Data Platform? - Learn about modern data stack components and where Bruin fits in
- **Part 2**: Setting Up Your First Bruin Project - Install Bruin, initialize a project, and configure environments
- **Part 3**: End-to-End NYC Taxi ELT Pipeline - Build ingestion, staging, and reporting layers with real data
- **Part 4**: Data Engineering with AI Agent - Use Bruin MCP to build pipelines with AI assistance
- **Part 5**: Deploy to BigQuery - Deploy your local pipeline to Google BigQuery

## Pipeline Skeleton

The suggested structure separates ingestion, staging, and reporting, but you may structure your pipeline however you like.

The required parts of a Bruin project are:
- `.bruin.yml` in the root directory
- `pipeline.yml` in the `pipeline/` directory (or in the root directory if you keep everything flat)
- `assets/` folder next to `pipeline.yml` containing your Python, SQL, and YAML asset files

```text
zoomcamp/
├── .bruin.yml                              # Environments + connections (local DuckDB, BigQuery, etc.)
├── README.md                               # Learning goals, workflow, best practices
└── pipeline/
    ├── pipeline.yml                        # Pipeline name, schedule, variables
    └── assets/
        ├── ingestion/
        │   ├── trips.py                    # Python ingestion
        │   ├── requirements.txt            # Python dependencies for ingestion
        │   ├── payment_lookup.asset.yml    # Seed asset definition
        │   └── payment_lookup.csv          # Seed data
        ├── staging/
        │   └── trips.sql                   # Clean and transform
        └── reports/
            └── trips_report.sql            # Aggregation for analytics
```

# Step-by-Step Tutorial

This module introduces Bruin as a unified data platform that combines **data ingestion**, **transformation**, and **quality** into a single CLI tool. You will build an end-to-end NYC Taxi data pipeline from scratch.

> **Prerequisites**: Familiarity with SQL, basic Python, and command-line tools. Prior exposure to orchestration and transformation concepts is helpful but not required.

---

## Part 1: What is a Data Platform?

### Learning Goals
- Understand what a data platform is and why you need one
- Learn how Bruin fits into the modern data stack
- Grasp Bruin's core abstractions: assets, pipelines, environments, connections

### 1.1 The Modern Data Stack Components
- **Data extraction/ingestion**: Moving data from sources to your warehouse
- **Data transformation**: Cleaning, modeling, and aggregating data (the "T" in ELT)
- **Data orchestration**: Scheduling and managing pipeline runs
- **Data quality/governance**: Ensuring data accuracy and consistency
- **Metadata management**: Tracking lineage, ownership, and documentation

### 1.2 Where Bruin Fits In
- Bruin = ingestion + transformation + quality + orchestration in one tool
- Handles pipeline orchestration similar to Airflow (dependency resolution, scheduling, retries)
- "What if Airbyte, Airflow, dbt, and Great Expectations had a lovechild"
- Runs locally, on VMs, or in CI/CD—no vendor lock-in
- Apache-licensed open source

### 1.3 Bruin Design Principles (Key Takeaways)
- Everything is version-controllable text (no UI/database configs)
- Real pipelines use multiple technologies (SQL + Python + R)
- Mix-and-match sources and destinations in a single pipeline
- Data quality is a first-class citizen, not an afterthought
- Quick feedback cycle: fast CLI, local development

### 1.4 Core Concepts
- **Asset**: Any data artifact that carries value (table, view, file, ML model, etc.)
- **Pipeline**: A group of assets executed together in dependency order
- **Environment**: A named set of connection configs (e.g., `default`, `production`) so the same pipeline can run locally and in production
- **Connection**: Credentials to authenticate with external data sources & destinations
- **Pipeline run**: A single execution instance with specific dates and configuration

---

## Part 2: Setting Up Your First Bruin Project

### Learning Goals
- Install Bruin CLI
- Initialize a project from a template
- Understand the project file structure
- Configure environments and connections

### 2.1 Installation
- Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
  - Verify installation: `bruin version`

If your terminal prints `To use the installed binaries, please restart the shell`, do one of the following:
- **Restart your terminal** (close + reopen) — simplest and most reliable
- **Reload your shell**:
  - `exec $SHELL -l` (works for most shells)
  - zsh: `source ~/.zshrc`
  - bash: `source ~/.bashrc` (or `source ~/.bash_profile` on some macOS setups)
  - fish: `exec fish`

#### IDE Extension (VS Code, Cursor, etc.)

Please refer to the doc page for more details:
  - https://getbruin.com/docs/bruin/vscode-extension/overview
  - https://getbruin.com/docs/bruin/getting-started/features#vs-code-extension

1. Install the **Bruin VS Code extension**:
   - Open VS Code → Extensions
   - Search: "Bruin" (publisher: bruin)
   - Install, then reload VS Code

2. Open this template folder and run from the Bruin panel:
   - Open `pipeline/pipeline.yml` or any asset file under `pipeline/assets/`
   - Use the Bruin panel to run `validate`, `run`, and see rendered code
   - To open the panel, click the Bruin logo in the top-right corner of the file

3. Set run parameters when creating a run:
   - **Start / end dates** for incremental windows
   - **Custom variables** like `taxi_types=["yellow"]`

### 2.2 Project Initialization
- Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
- Explore the generated structure:
  - `.bruin.yml` — environment and connection configuration
  - `pipeline/pipeline.yml` — pipeline name, schedule, variables
  - `pipeline/assets/` — where your SQL/Python assets live
  - `pipeline/assets/**/requirements.txt` — Python dependencies (scoped to the nearest folder)

**Important**: Bruin CLI requires a git-initialized folder (uses git to detect project root); `bruin init` auto-initializes git if needed

### 2.3 Configuration Files Deep Dive

#### `.bruin.yml`
- Defines environments (e.g., `default`, `production`)
- Contains connection credentials (DuckDB, BigQuery, Snowflake, etc.)
- Lives at the project root and **must be gitignored** because it contains credentials/secrets
  - `bruin init` auto-adds it to `.gitignore`, but double-check before committing anything

#### `pipeline.yml`
- `name`: Pipeline identifier (appears in logs, `BRUIN_PIPELINE` env var)
- `schedule`: When to run (`daily`, `hourly`, `weekly`, or cron expression)
- `start_date`: Earliest date for backfills
- `default_connections`: Platform-to-connection mappings
- `variables`: User-defined variables with JSON Schema validation

### 2.4 Connections
- List connections: `bruin connections list`
- Add a connection: `bruin connections add`
- Test connectivity: `bruin connections ping <connection-name>`
- Default connections reduce repetition across assets

---

## Part 3: End-to-End NYC Taxi ELT Pipeline

### Learning Goals
- Build a complete ELT pipeline: ingestion → staging → reports
- Understand the three asset types: Python, SQL, and Seed
- Apply materialization strategies for incremental processing
- Add quality checks and declare dependencies

### 3.1 Pipeline Architecture
- **Ingestion**: Extract raw data from external sources (Python assets, seed CSVs)
- **Staging**: Clean, normalize, deduplicate, enrich (SQL assets)
- **Reports**: Aggregate for dashboards and analytics (SQL assets)
- Assets form a DAG—Bruin executes them in dependency order

### 3.2 Ingestion Layer
- Python asset to fetch NYC Taxi data from the TLC public endpoint
- Seed asset to load a static payment type lookup table from CSV
- Use `append` strategy for raw ingestion (handle duplicates downstream)
- Follow the TODO instructions in `pipeline/assets/ingestion/trips.py` and `pipeline/assets/ingestion/payment_lookup.asset.yml`

### 3.3 Staging Layer
- SQL asset to clean, deduplicate, and join with lookup to enrich raw trip data
- Use `time_interval` strategy for incremental processing
- Follow the TODO instructions in `pipeline/assets/staging/trips.sql`

### 3.4 Reports Layer
- SQL asset to aggregate staging data into analytics-ready metrics
- Use `time_interval` strategy and same `incremental_key` as staging for consistency
- Follow the TODO instructions in `pipeline/assets/reports/trips_report.sql`

### 3.5 Running and Validating

CLI Commands: https://getbruin.com/docs/bruin/commands/run

```bash
# Validate structure & definitions
bruin validate ./pipeline/pipeline.yml --environment default

# First-time run tip:
# Use --full-refresh to create/replace tables from scratch (helpful on a new DuckDB file).
bruin run ./pipeline/pipeline.yml --environment default --full-refresh

# Run an ingestion asset, then downstream (to test incrementally)
bruin run ./pipeline/assets/ingestion/trips.py \
  --environment default \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --var taxi_types='["yellow"]' \
  --downstream

# Query your tables using `bruin query`
# Docs: https://getbruin.com/docs/bruin/commands/query
bruin query --connection duckdb-default --query "SELECT COUNT(*) FROM ingestion.trips"

# Open DuckDB UI (useful for exploring tables interactively)
# Requires DuckDB CLI installed locally.
duckdb duckdb.db -ui

# Check lineage to understand asset dependencies
bruin lineage ./pipeline/pipeline.yml
```

---

## Part 4: Data Engineering with AI Agent

### Learning Goals
- Set up Bruin MCP to extend AI assistants with Bruin context
- Use an AI agent to build the entire end-to-end pipeline
- Leverage AI for documentation lookup, code generation, and pipeline execution

### 4.1 What is Bruin MCP?
- MCP (Model Context Protocol) connects AI assistants to Bruin's capabilities
- The AI gains access to Bruin documentation, commands, and your pipeline context
- Supported in Cursor, Claude Code, and other MCP-compatible tools

### 4.2 Setting Up Bruin MCP

**Cursor IDE:**
- Go to Cursor Settings → MCP & Integrations → Add Custom MCP
- Add the Bruin MCP server configuration:
  ```json
  {
    "mcpServers": {
      "bruin": {
        "command": "bruin",
        "args": ["mcp"]
      }
    }
  }
  ```

**Claude Code:**
```bash
claude mcp add bruin -- bruin mcp
```

Bruin MCP Docs: https://getbruin.com/docs/bruin/getting-started/bruin-mcp

### 4.3 Building the Pipeline with AI
- Ask the AI to help configure `.bruin.yml` and `pipeline.yml`
- Request asset scaffolding: "Create a Python ingestion asset for NYC taxi data"
- Get help with materialization: "What strategy should I use for incremental loads?"
- Debug issues: "Why is my quality check failing?"
- Execute commands: "Run the staging.trips asset with --full-refresh"

### 4.4 Example Prompts

**Questions about Bruin documentation:**
- "How do I create a DuckDB connection in Bruin?"
- "What does the time_interval materialization strategy do?"
- "What materialization strategies does Bruin support?"

**Commands to build or make changes to pipeline:**
- "Write a Python asset that fetches data from this API endpoint"
- "Generate the SQL for deduplicating trips using a composite key"
- "Add a not_null quality check to the pickup_datetime column"

**Commands to test and validate pipeline:**
- "Validate the entire pipeline"
- "Run the staging.trips asset with --full-refresh"
- "Check the lineage for my reports.trips_report asset"

**Commands to query and analyze the data:**
- "Run a query to show row counts for all my tables"
- "Query the reports table to show top 10 payment types by trip count"
- "Show me the data schema for staging.trips"

### 4.5 AI-Assisted Workflow
- Start with configuration: Let AI help set up `.bruin.yml` and `pipeline.yml`
- Build incrementally: Create one asset at a time, validate, run, iterate
- Use AI for documentation: Ask about Bruin features instead of searching docs
- Debug together: Share error messages and let AI suggest fixes
- Learn by doing: Ask "why" questions to understand Bruin concepts

Example prompt to create the entire pipeline end-to-end and test it:
```text
Build an end-to-end NYC Taxi data pipeline using Bruin.

Start with running `bruin init zoomcamp` to initialize the project.

## Context
- Project folder: @zoomcamp/pipeline
- Reference docs: @zoomcamp/README.md
- Use Bruin MCP tools for documentation lookup and command execution

## Instructions

### 1. Configuration (do this first)
- Create `.bruin.yml` with a DuckDB connection named `duckdb-default`
- Configure `pipeline.yml`: set name, schedule (monthly), start_date, default_connections, and the `taxi_types` variable (array of strings)

### 2. Build Assets (follow TODOs in each file)

NYC Taxi Raw Trip Source Details:
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- **Format**: Parquet files, one per taxi type per month
- **Naming**: `<taxi_type>_tripdata_<year>-<month>.parquet`
- **Examples**:
  - `yellow_tripdata_2022-03.parquet`
  - `green_tripdata_2025-01.parquet`
- **Taxi Types**: `yellow` (default), `green`

Build in this order, validating each with `bruin validate` before moving on:

a) **pipeline/assets/ingestion/payment_lookup.asset.yml** - Seed asset to load CSV lookup table
b) **pipeline/assets/ingestion/trips.py** - Python asset to fetch NYC taxi parquet data from TLC endpoint
   - Use `taxi_types` variable and date range from BRUIN_START_DATE/BRUIN_END_DATE
   - Add requirements.txt with: pandas, requests, pyarrow, python-dateutil
   - Keep the data in its rawest format without any cleaning or transformations
c) **pipeline/assets/staging/trips.sql** - SQL asset to clean, deduplicate (ROW_NUMBER), and enrich with payment lookup
   - Use `time_interval` strategy with `pickup_datetime` as incremental_key
d) **pipeline/assets/reports/trips_report.sql** - SQL asset to aggregate by date, taxi_type, payment_type
   - Use `time_interval` strategy for consistency

### 3. Validate & Run
- Validate entire pipeline: `bruin validate ./pipeline/pipeline.yml`
- Run with: `bruin run ./pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-02-01`
- For faster testing, use `--var 'taxi_types=["yellow"]'` (skip green taxis)

### 4. Verify Results
- Check row counts across all tables
- Query the reports table to confirm aggregations look correct
- Verify all quality checks passed (24 checks expected)
```

---

## Part 5: Deploy to BigQuery

This part takes what you built locally and runs it on **Google BigQuery**.

> **Note on SQL dialects**: BigQuery SQL is not identical to DuckDB SQL. Your pipeline structure stays the same, but you may need to update SQL syntax and types when switching engines.

### 5.1 Create a GCP Project + BigQuery Datasets
1. Create (or pick) a GCP project and enable the BigQuery API
2. Create datasets that match your asset schemas (recommended for this module):
   - `ingestion`
   - `staging`
   - `reports`

### 5.2 Create Credentials (Choose One)
- **Option A (recommended for local dev)**: Application Default Credentials (ADC)
  - Install gcloud and authenticate: `gcloud auth application-default login`
- **Option B**: Service account JSON (for CI/CD)
  - Create a service account with BigQuery permissions and download the JSON key

### 5.3 Add Connection to `.bruin.yml`
```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp-default"
          project_id: "your-gcp-project-id"
          location: "US" # or "EU", or your region
          # Authentication options (choose one):
          use_application_default_credentials: true
          # service_account_file: "/path/to/service-account.json"
          # service_account_json: |
          #   { "type": "service_account", ... }
```

### 5.4 Update Pipeline & Assets
- In `pipeline/pipeline.yml`: change `default_connections.duckdb` → `default_connections.bigquery`
  - Example: `duckdb: duckdb-default` → `bigquery: gcp-default`
- In SQL assets: change the `type` to BigQuery:
  - `duckdb.sql` → `bq.sql`
- In seed assets: change the `type` to BigQuery:
  - `duckdb.seed` → `bq.seed`
- In Python assets that use materialization: set/update `connection:` to `gcp-default`
- Fix any SQL dialect issues:
  - Data types can differ (e.g., `INTEGER` vs `INT64`, timestamp handling, quoting)
  - Some functions/operators may need a BigQuery equivalent

Docs:
- BigQuery platform: https://getbruin.com/docs/bruin/platforms/bigquery
- `.bruin.yml` secrets backend: https://getbruin.com/docs/bruin/secrets/bruinyml

---

## Key Commands Reference

| Command | Purpose |
|---------|---------|
| `bruin init <template> <folder>` | Initialize a new project |
| `bruin validate <path>` | Validate pipeline/asset structure |
| `bruin run <path>` | Execute pipeline or asset |
| `bruin run --downstream` | Run asset and all downstream assets |
| `bruin run --full-refresh` | Truncate and rebuild from scratch |
| `bruin run --only checks` | Run quality checks without asset execution |
| `bruin query --connection <conn> --query "..."` | Execute ad-hoc queries |
| `bruin lineage <path>` | View asset dependencies |
| `bruin render <path>` | Show rendered template output |
| `bruin format <path>` | Format code |
| `bruin connections list` | List configured connections |
| `bruin connections ping <name>` | Test connection connectivity |

---

## Best Practices & Tips

### Choosing the Right `incremental_key`

When using `time_interval` strategy, the `incremental_key` determines which rows to delete and re-insert during each run.

**Key principles:**
1. **Use the same key across all assets** - If staging uses `pickup_datetime` as the incremental key, reports should too. This ensures data flows consistently through your pipeline.

2. **Match the key to your data extraction logic** - In this example, NYC taxi data files are organized by month based on when rides started. Since each file contains rides where `pickup_datetime` falls in that month, `pickup_datetime` is the natural incremental key.

3. **The key should be immutable** - Once a row is extracted, its incremental key value shouldn't change. Event timestamps (like `pickup_datetime`) are better than processing timestamps for this reason.

### Deduplication Strategy

Since there's no unique ID per row in taxi data, you'll need a **composite key** for deduplication:

- Combine columns that together identify a unique trip
- Example: `(pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount)`
- Use these columns as `primary_key: true` in your column definitions
- In SQL, deduplicate using `ROW_NUMBER()` or `QUALIFY` to keep one record per composite key

### Quality-First Development

- Add checks early, not as an afterthought
- Use built-in checks: `not_null`, `unique`, `positive`, `non_negative`, `accepted_values`
- Add custom checks for business-specific invariants

### Project Organization

- Keep assets in `pipeline/assets/`
- Use schemas to organize layers: `ingestion.`, `staging.`, `reports.`
- Put non-asset SQL in separate folders (`/analyses`, `/queries`)

### Local Development

- Always validate before running: `bruin validate ./pipeline/pipeline.yml`
- Use `--full-refresh` for initial runs on new databases
- Query tables directly to verify results: `bruin query --connection duckdb-default --query "..."`
- Check lineage to understand impact of changes: `bruin lineage <asset>`
