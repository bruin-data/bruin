# Zoomcamp - Data Platform (Bruin) Template

This template is an **educational scaffold** for building an end-to-end data pipeline in Bruin (ingestion → staging → reporting) with **no implementation provided**.

## Learning Goals

- Understand how Bruin projects are structured (`pipeline.yml` + `assets/`)
- Use **materialization strategies** intentionally (append, time_interval, etc.)
- Declare **dependencies** and explore lineage (`bruin lineage`)
- Apply **metadata** (columns, primary keys, descriptions) and **quality checks**
- Parameterize runs with **pipeline variables**

## Pipeline Skeleton

The suggested structure separates ingestion, staging, and reporting, but you may structure your pipeline however you like.

The required parts of a Bruin project are:
- `.bruin.yml` in the root directory
- `pipeline.yml` in the pipeline directory (or root directory if there's no pipeline-specific sub-directory)
- `assets/` folder containing your Python, SQL, and YAML asset files

```text
zoomcamp/
├── .bruin.yml                              # Environment + DuckDB connection config
├── pipeline.yml                            # Pipeline name, schedule, variables
├── requirements.txt                        # Python dependencies placeholder
├── README.md                               # Learning goals, workflow, best practices
└── assets/
    ├── ingestion/
    │   ├── trips.py                        # Python ingestion
    │   ├── payment_lookup.asset.yml        # Seed asset definition
    │   └── payment_lookup.csv              # Seed data
    ├── staging/
    │   └── trips.sql                       # Clean and transform
    └── reports/
        └── trips_report.sql                # Aggregation for analytics
```

## Suggested Workflow

### Step 1: Configure the `.bruin.yml` and `pipeline.yml` files
- Create the `.bruin.yml` file in the root directory
  - Configure environments
  - Create a connection for DuckDB

- Create a `pipeline.yml` file in the same directory
  - Set the pipeline name/schedule/start_date
  - Initialize the `default_connections`
  - Add custom `variables`

### Step 2: Create the pipeline assets
- **ingestion**
  - Python script to extract files from source endpoint
  - Seed assets (.asset.yml + .csv) for lookup tables
- **staging**
  - SQL asset(s) to clean, normalize schema, deduplicate
- **reports**
  - SQL asset(s) to aggregate and transform data

### Step 3: Validate & run the pipeline

CLI Commands: https://getbruin.com/docs/bruin/commands/run

```bash
# Validate structure & definitions
bruin validate ./pipeline.yml --environment default

# First-time run tip:
# Use --full-refresh to create/replace tables from scratch (helpful on a new DuckDB file).
bruin run ./pipeline.yml --environment default --full-refresh

# Run an ingestion asset, then downstream (to test incrementally)
bruin run ./assets/ingestion/trips.py \
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
```

## IDE Extension (VS Code, Cursor, etc.)

Please refer to the doc page for more details:
  - https://getbruin.com/docs/bruin/vscode-extension/overview
  - https://getbruin.com/docs/bruin/getting-started/features#vs-code-extension

1. Install the **Bruin VS Code extension**:
   - Open VS Code → Extensions
   - Search: "Bruin" (publisher: bruin)
   - Install, then reload VS Code

2. Open this template folder and run from the Bruin panel:
   - Open `pipeline.yml` or any asset file
   - Use the Bruin panel to run `validate`, `run`, and see rendered code
   - To open the panel, click the Bruin logo in the top-right corner of the file

3. Set run parameters when creating a run:
   - **Start / end dates** for incremental windows
   - **Custom variables** like `taxi_types=["yellow"]`

## Bruin MCP (AI Assistant Integration)

Bruin MCP extends AI assistants (Claude, Cursor, Codex) to help you build and understand data pipelines.

Docs: https://getbruin.com/docs/bruin/getting-started/bruin-mcp

### Setup

**Cursor IDE:**
Go to Cursor Settings → MCP & Integrations → Add Custom MCP, then add:

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

### What You Can Ask

Once MCP is set up, you can ask your AI assistant questions like:
- "How do I create a BigQuery asset in Bruin?"
- "What materialization strategies does Bruin support?"
- "How do I set up a DuckDB connection?"
- "Run a query on my staging.trips table"

The AI will use Bruin's documentation and can execute commands directly.
