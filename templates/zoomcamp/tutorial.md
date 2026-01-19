# Data Platform with Bruin - Tutorial Outline

This module introduces Bruin as a unified data platform that combines **data ingestion**, **transformation**, and **quality** into a single CLI tool. You will build an end-to-end NYC Taxi data pipeline from scratch.

> **Prerequisites**: Familiarity with SQL, basic Python, and command-line tools. Prior exposure to orchestration and transformation concepts is helpful but not required.

---

## Part 1: What is a Data Platform?

### Learning Goals
- Understand what a data platform is and why you need one
- Learn how Bruin fits into the modern data stack
- Grasp Bruin's core abstractions: assets, pipelines, connections

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
- **Connection**: Credentials to communicate with external platforms
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
- Install Bruin Extension

### 2.2 Project Initialization
- Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
- Explore the generated structure:
  - `.bruin.yml` — environment and connection configuration
  - `pipeline.yml` — pipeline name, schedule, variables
  - `assets/` — where your SQL/Python assets live
  - `requirements.txt` — Python dependencies

**Important**: Bruin CLI requires a git-initialized folder (uses git to detect project root); `bruin init` auto-initializes git if needed

### 2.3 Configuration Files Deep Dive

#### `.bruin.yml`
- Defines environments (e.g., `default`, `production`)
- Contains connection credentials (DuckDB, BigQuery, Snowflake, etc.)
- Lives at the project root; auto-added to `.gitignore`

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
- Use Python assets for data extraction
- Use SQL assets for transformation and aggregation
- Apply materialization strategies for incremental processing
- Add quality checks throughout the pipeline

### 3.1 Pipeline Architecture Overview
- **Ingestion layer**: Extract raw data from external sources into your warehouse
- **Staging layer**: Clean, normalize, deduplicate, and enrich raw data
- **Reports layer**: Aggregate staging data for analytics and dashboards
- Each layer depends on the previous, forming a DAG (directed acyclic graph)

### 3.2 Asset Types You'll Use
- **Python assets** (`type: python`): For API extraction and custom logic
  - Runs in isolated environments via `uv`—no global Python needed
  - Implement `materialize()` to return a DataFrame; Bruin loads it to destination
  - Access runtime context via env vars: `BRUIN_START_DATE`, `BRUIN_END_DATE`, `BRUIN_VARS`
- **SQL assets** (`type: duckdb.sql`): For transformations
  - Embedded YAML definition in `/* @bruin ... @bruin */` block
  - Use Jinja templating: `{{ start_datetime }}`, `{{ end_datetime }}`, `{{ var.my_var }}`
- **Seed assets** (`type: duckdb.seed`): For static CSV lookup tables

### 3.3 Materialization Strategies
- `append`: Insert new rows only (good for raw ingestion)
- `time_interval`: Delete + insert rows within a time window (good for incremental transforms)
- `merge`: Upsert based on primary key
- `create+replace`: Full rebuild every run
- Key config: `incremental_key`, `time_granularity`

### 3.4 Dependencies and Lineage
- Declare `depends:` to establish execution order
- Bruin builds the DAG automatically
- Run `bruin lineage <asset>` to visualize upstream/downstream

### 3.5 Quality Checks
- **Column checks**: `not_null`, `unique`, `positive`, `non_negative`, `accepted_values`
- **Custom checks**: SQL query returning a scalar compared to expected value
- Checks run after asset execution; failures block downstream assets

### 3.6 Building the Pipeline
Follow the TODO instructions in each asset file and the README for detailed steps:
- `assets/ingestion/trips.py` — Python ingestion from NYC TLC endpoint
- `assets/ingestion/payment_lookup.asset.yml` — Seed asset for payment types
- `assets/staging/trips.sql` — Clean, dedupe, enrich with lookups
- `assets/reports/trips_report.sql` — Aggregate metrics by dimensions

### 3.7 Running the Pipeline
- Validate: `bruin validate ./pipeline.yml`
- First run (create tables): `bruin run ./pipeline.yml --full-refresh`
- Incremental run: `bruin run ./pipeline.yml --start-date YYYY-MM-DD --end-date YYYY-MM-DD`
- Run single asset + downstream: `bruin run ./assets/ingestion/trips.py --downstream`
- Query results: `bruin query --connection duckdb-default --query "SELECT ..."`

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

### 4.3 Building the Pipeline with AI
- Ask the AI to help configure `.bruin.yml` and `pipeline.yml`
- Request asset scaffolding: "Create a Python ingestion asset for NYC taxi data"
- Get help with materialization: "What strategy should I use for incremental loads?"
- Debug issues: "Why is my quality check failing?"
- Execute commands: "Run the staging.trips asset with --full-refresh"

### 4.4 Example Prompts
- "How do I create a DuckDB connection in Bruin?"
- "Write a Python asset that fetches data from this API endpoint"
- "Add a not_null quality check to the pickup_datetime column"
- "What does the time_interval materialization strategy do?"
- "Run a query to show row counts for all my tables"
- "Generate the SQL for deduplicating trips using a composite key"

### 4.5 AI-Assisted Workflow
- Start with configuration: Let AI help set up `.bruin.yml` and `pipeline.yml`
- Build incrementally: Create one asset at a time, validate, run, iterate
- Use AI for documentation: Ask about Bruin features instead of searching docs
- Debug together: Share error messages and let AI suggest fixes
- Learn by doing: Ask "why" questions to understand Bruin concepts

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

## Additional Resources

- Bruin Documentation: https://getbruin.com/docs
- Bruin GitHub: https://github.com/bruin-data/bruin
- VS Code Extension: Search "Bruin" in Extensions
- Bruin MCP Setup: https://getbruin.com/docs/bruin/getting-started/bruin-mcp

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

- Keep assets in `/assets` folder
- Use schemas to organize layers: `ingestion.`, `staging.`, `reports.`
- Put non-asset SQL in separate folders (`/analyses`, `/queries`)

### Local Development

- Always validate before running: `bruin validate ./pipeline.yml`
- Use `--full-refresh` for initial runs on new databases
- Query tables directly to verify results: `bruin query --connection duckdb-default --query "..."`
- Check lineage to understand impact of changes: `bruin lineage <asset>`