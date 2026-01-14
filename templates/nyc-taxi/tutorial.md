# Data Platforms with Bruin - Tutorial Script Outline

**Total Duration**: ~40 minutes

**Learning Objective**: Understand how Bruin combines orchestration, transformation, and data quality into a unified platform

## Part 1: Introduction to Data Platforms (~5 minutes)

### Learning Objectives
- Understand the evolution from separate tools to unified platforms
- Recognize the challenges of tool fragmentation
- Identify the value proposition of integrated data platforms

### Key Talking Points

#### 1.1 The Fragmented Data Stack (2 minutes)
- **Current State**: Most teams use separate tools for different functions
  - Orchestration: Kestra, Airflow, Prefect
  - Transformation: dbt, Dataform
  - Data Quality: Great Expectations, Soda
  - Ingestion: Airbyte, Fivetran, custom scripts
- **Pain Points**:
  - Multiple configuration files and systems
  - Context switching between tools
  - Difficult dependency management
  - Inconsistent error handling
  - Complex deployment pipelines

#### 1.2 The Unified Platform Approach (2 minutes)
- **What is a Data Platform?**
  - Single tool that handles ingestion, transformation, and quality
  - Unified configuration and execution model
  - Consistent interface across all operations
- **Benefits**:
  - Reduced cognitive load
  - Simplified deployment
  - Better dependency management
  - Integrated lineage and observability
  - Version-controlled everything

#### 1.3 Introduction to Bruin (1 minute)
- **Bruin's Positioning**: "If dbt, Airbyte, and Great Expectations had a lovechild"
- **Core Philosophy**:
  - Everything version-controllable (text files, no UI configs)
  - Multi-technology support (SQL, Python, R)
  - Multi-source/destination support
  - Mix-and-match capabilities
  - Avoid vendor lock-in (Apache-licensed, runs anywhere)

### Transition
"Now that we understand why unified platforms matter, let's dive into Bruin and build our first pipeline."

---

## Part 2: Introduction to Bruin (~10 minutes)

### Learning Objectives
- Understand Bruin's core concepts (Assets, Pipelines, Connections)
- Set up a Bruin project
- Build and run a basic pipeline
- Understand Bruin's materialization strategies

### Key Talking Points

#### 2.1 Bruin Core Concepts (3 minutes)

**Assets**
- Definition: Anything that carries value derived from data
- Examples: Tables, views, files, ML models
- Components:
  - Definition: Metadata (name, type, dependencies, materialization)
  - Content: The actual query/logic (SQL, Python, R)

**Pipelines**
- Definition: Group of assets executed together in dependency order
- Structure:
  ```
  my-pipeline/
  ├── pipeline.yml
  └── assets/
      ├── asset1.sql
      └── asset2.py
  ```

**Connections**
- Definition: Credentials to communicate with external platforms
- Stored in `.bruin.yml` (not committed to git)
- Examples: DuckDB, BigQuery, Snowflake, PostgreSQL

**Materialization Strategies**
- `table`: Create/replace table on each run
- `view`: Create/replace view on each run
- `merge`: Upsert based on primary key (incremental)
- `time_interval`: Incremental based on time ranges
- `append`: Append-only incremental

#### 2.2 Hands-On: Initialize First Bruin Project (4 minutes)

**Step 1: Initialize Project**
```bash
bruin init duckdb
```
- Creates project structure
- Sets up basic DuckDB connection
- Includes example assets

**Step 2: Explore Project Structure**
- Show `pipeline.yml`: Pipeline configuration
- Show `.bruin.yml`: Connection configuration (explain why it's gitignored)
- Show `assets/`: Where assets live
- Show example SQL asset with metadata

**Step 3: Understand Asset Metadata**
- Show `@bruin` comment block in SQL asset
- Explain key fields:
  - `name`: Asset identifier
  - `type`: Platform and language (e.g., `duckdb.sql`)
  - `depends`: Dependency declarations
  - `materialization`: How data is stored
  - `columns`: Schema definition with constraints

**Step 4: Run the Pipeline**
```bash
bruin run pipeline.yml
```
- Show execution output
- Explain dependency resolution
- Show how assets execute in order

#### 2.3 Key Differences from dbt (3 minutes)

**Similarities**
- SQL-based transformations
- Dependency management
- Jinja templating
- Version-controlled

**Key Differences**
- **Multi-language**: Native Python and R support (not just SQL)
- **Built-in Ingestion**: No need for separate Airbyte/Fivetran
- **Unified Execution**: Single command for everything
- **Materialization**: More strategies (merge, time_interval)
- **Platform Agnostic**: Same code works across platforms

### Transition
"Now that we understand Bruin basics, let's build a real-world ELT pipeline from scratch."

---

## Part 3: End-to-End ELT/ETL Pipeline (~15 minutes)

### Learning Objectives
- Build a complete ELT pipeline from scratch
- Understand multi-tier data architecture (raw → staging → reports)
- Use Python for ingestion and SQL for transformation
- Implement incremental processing strategies
- Apply data quality checks

### Key Talking Points

#### 3.1 Project Overview: NYC Taxi Pipeline (2 minutes)

**Business Context**
- Ingest NYC taxi trip data from public HTTP sources
- Transform through multiple tiers
- Generate analytical reports

**Pipeline Architecture**
- **Raw Layer**: Ingestion and raw data storage
  - `trips_raw`: Python ingestion from HTTP parquet files
  - `taxi_zone_lookup`: SQL ingestion from HTTP CSV
  - `payment_lookup`: Seed data from local CSV
- **Staging Layer**: Cleaned and enriched data
  - `trips_summary`: Normalize columns, clean, enrich
- **Reports Layer**: Aggregated analytics
  - `report_trips_monthly`: Monthly summary reports

**Data Source**
- Public NYC TLC Trip Record Data
- HTTP endpoints with parquet files
- Format: `<taxi_type>_tripdata_<year>-<month>.parquet`

#### 3.2 Hands-On: Build the Pipeline (12 minutes)

**Step 1: Initialize Project Structure (1 minute)**
```bash
mkdir nyc-taxi
cd nyc-taxi
bruin init duckdb  # Start with template, then customize
```
- Create directory structure
- Set up `.bruin.yml` with DuckDB connection

**Step 2: Configure Pipeline (1 minute)**
- Create `pipeline.yml`:
  - Set `name`, `schedule`, `start_date`
  - Configure `default_connections`
  - Define `variables` (taxi_types array)
- Explain pipeline-level variables and how they're used

**Step 3: Build Raw Layer - Python Ingestion (4 minutes)**

**Create `assets/raw/trips_raw.py`**
- Explain Python materialization concept
- Show how to:
  - Read `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables
  - Access pipeline variables via `BRUIN_VARS`
  - Loop through date ranges and taxi types
  - Download parquet files from HTTP
  - Return Pandas DataFrame
- Explain `merge` materialization strategy:
  - Why use merge? (handles re-ingestion, no duplicates)
  - Composite primary key design
  - Automatic upsert behavior

**Create `assets/raw/taxi_zone_lookup.sql`**
- Show DuckDB's `read_csv()` with HTTP URL
- Explain `create+replace` strategy (lookup tables refresh each run)
- Add primary key and constraints

**Create `assets/raw/payment_lookup.asset.yml`**
- Introduce seed assets for static reference data
- Show local CSV file + YAML configuration
- Explain when to use seeds vs SQL assets

**Step 4: Build Staging Layer (3 minutes)**

**Create `assets/staging/trips_summary.sql`**
- Explain column normalization:
  - COALESCE for yellow vs green taxi columns
  - Renaming for consistency
- Show data enrichment:
  - JOIN with lookup tables
  - Calculate derived fields (trip_duration_seconds)
- Implement data quality filters:
  - Positive duration
  - Reasonable trip length
  - Non-negative amounts
- Explain `time_interval` materialization:
  - How it works (delete + insert for date range)
  - Why use it? (efficient reprocessing)
  - Incremental key selection

**Step 5: Build Reports Layer (2 minutes)**

**Create `assets/reports/report_trips_monthly.sql`**
- Show aggregation patterns
- Group by taxi_type and month
- Calculate metrics (count, revenue, averages)
- Use `time_interval` with month-level granularity

**Step 6: Run and Verify (1 minute)**
```bash
# Test individual asset
bruin run assets/raw/trips_raw.py --start-date 2021-01-01 --end-date 2021-01-31

# Run full pipeline
bruin run pipeline.yml --start-date 2021-01-01 --end-date 2021-02-28

# Query results
bruin query --connection duckdb-default --query "SELECT * FROM reports.report_trips_monthly LIMIT 10"
```

#### 3.3 Key Concepts Demonstrated (1 minute)

**Multi-Tier Architecture**
- Separation of concerns
- Raw preserves source data
- Staging standardizes schema
- Reports provide analytics

**Incremental Processing**
- `merge`: For ingestion (handles duplicates)
- `time_interval`: For transformations (efficient updates)

**Python + SQL Integration**
- Python for complex ingestion logic
- SQL for declarative transformations
- Seamless data flow between layers

**Data Quality**
- Built-in column checks
- Custom SQL quality checks
- Data filtering in transformations

### Transition
"Now that we've built a pipeline manually, let's see how AI can accelerate this process."

---

## Part 4: AI Data Engineering (~10 minutes)

### Learning Objectives
- Understand Bruin MCP (Model Context Protocol)
- Configure Bruin MCP for Cursor IDE
- Use AI agents to build pipelines from natural language
- Understand the future of AI-assisted data engineering

### Key Talking Points

#### 4.1 Introduction to Bruin MCP (2 minutes)

**What is MCP?**
- Model Context Protocol: Standard for AI agents to interact with tools
- Allows AI editors (Cursor, Claude Code) to understand and use Bruin
- Bridges the gap between CLI and AI editors

**What Can Bruin MCP Do?**
- **Analyze Data**: Query databases and understand schemas
- **Ingest Data**: Set up ingestion from 50+ sources
- **Build Pipelines**: Create assets and pipelines from descriptions
- **Compare Data**: Diff tables between environments
- **Answer Questions**: Access Bruin documentation

#### 4.2 Configure Bruin MCP for Cursor (3 minutes)

**Step 1: Verify Bruin CLI Installation**
```bash
bruin --version
```

**Step 2: Configure Cursor IDE**
- Open Cursor Settings
- Navigate to: MCP & Integrations > Add Custom MCP
- Add configuration:
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

**Step 3: Verify Setup**
- Restart Cursor
- Check that Bruin MCP appears in available MCP servers
- Test by asking: "What is Bruin?"

#### 4.3 Hands-On: AI-Assisted Pipeline Creation (4 minutes)

**Scenario**: Build the NYC Taxi pipeline using AI

**Step 1: Create Project Structure**
- Ask AI: "Create a new Bruin project for NYC taxi data analysis"
- AI should:
  - Initialize project with `bruin init`
  - Set up `.bruin.yml` with DuckDB connection
  - Create basic `pipeline.yml`

**Step 2: Build Pipeline with AI Prompt**
Provide this prompt to the AI agent:

```
I want to build a Bruin pipeline for NYC taxi data analysis. The pipeline should:

1. Ingest NYC taxi trip data from HTTP parquet files:
   - URL pattern: https://d37ci6vzurychx.cloudfront.net/trip-data/<taxi_type>_tripdata_<year>-<month>.parquet
   - Taxi types: yellow and green (configurable via pipeline variable)
   - Use Python materialization with merge strategy
   - Primary key: (pickup_datetime, dropoff_datetime, pulocationid, dolocationid, taxi_type)
   - Add extracted_at timestamp

2. Ingest taxi zone lookup from:
   - URL: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
   - Use SQL with create+replace strategy
   - Primary key: location_id

3. Create a seed asset for payment lookup with payment types 0-6

4. Build staging layer that:
   - Normalizes yellow/green taxi column names (tpep_* vs lpep_*)
   - Enriches with zone and payment lookups
   - Calculates trip_duration_seconds
   - Applies data quality filters
   - Uses time_interval materialization

5. Build monthly reports aggregating by taxi_type and month

The pipeline should process data from 2021-01-01 onwards, monthly schedule.
```

**Step 3: Review AI-Generated Code**
- Show how AI creates assets
- Explain that AI uses Bruin MCP to:
  - Understand Bruin concepts
  - Access documentation
  - Generate correct metadata
  - Follow best practices

**Step 4: Refine and Test**
- Ask AI to fix any issues
- Run the pipeline
- Compare with manually built version

#### 4.4 The Future of AI Data Engineering (1 minute)

**Current Capabilities**
- Natural language to pipeline code
- Documentation-aware code generation
- Schema-aware query generation
- Best practice enforcement

**Future Possibilities**
- Automatic optimization suggestions
- Data quality issue detection
- Performance tuning recommendations
- Self-documenting pipelines

**Best Practices**
- AI is a powerful assistant, not a replacement
- Always review and understand generated code
- Use AI for boilerplate, focus on business logic
- Test thoroughly before production

### Wrap-Up

**Key Takeaways**
1. Unified platforms reduce complexity vs. fragmented tools
2. Bruin combines ingestion, transformation, and quality
3. Multi-language support (SQL, Python, R) in one tool
4. AI can accelerate pipeline development significantly

**Next Steps**
- Explore more Bruin features (lineage, data diff, environments)
- Try building pipelines for your own data sources
- Experiment with different materialization strategies
- Leverage AI for rapid prototyping

**Resources**
- Bruin Documentation: https://getbruin.com/docs
- Bruin MCP Setup: https://getbruin.com/docs/bruin/getting-started/bruin-mcp.html
- GitHub: https://github.com/bruin-data/bruin

---

## Appendix: Detailed Talking Points (For Instructor Reference)

### Common Questions & Answers

**Q: How does Bruin compare to dbt?**
A: Bruin is similar to dbt for SQL transformations, but adds Python/R support, built-in ingestion, and unified execution. You can think of it as dbt + Airbyte + Great Expectations in one tool.

**Q: Can I use Bruin with my existing dbt models?**
A: Yes, you can gradually migrate. Bruin supports SQL assets that are very similar to dbt models. You can run both tools side-by-side during migration.

**Q: What about orchestration? Do I still need Airflow?**
A: Bruin handles pipeline execution and dependencies. For complex scheduling across multiple systems, you might still use Airflow/Kestra, but many teams find Bruin's built-in scheduling sufficient.

**Q: How does Python materialization work?**
A: You write Python that returns a Pandas DataFrame. Bruin automatically handles database connections, schema inference, and table creation/insertion based on your materialization strategy.

**Q: Can I use Bruin with cloud data warehouses?**
A: Yes! Bruin supports BigQuery, Snowflake, Redshift, Databricks, and many others. The same pipeline code works across platforms.

### Timing Notes
- Part 1: Keep it high-level, don't get into tool comparisons
- Part 2: Focus on hands-on, let students type along
- Part 3: This is the core learning, take time to explain concepts
- Part 4: Show the "wow" factor of AI, but emphasize understanding

### Troubleshooting Tips
- If `bruin init` fails: Check Go installation and PATH
- If MCP doesn't work: Verify Cursor restart and JSON syntax
- If pipeline fails: Check `.bruin.yml` connection configuration
- If Python assets fail: Verify `requirements.txt` and dependencies

