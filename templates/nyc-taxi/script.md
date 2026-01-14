# Data Platforms with Bruin - Video Script

## Video 1: Introduction to Data Platforms (~5 minutes)

### Opening (30 seconds)
- "Welcome to the Data Platforms with Bruin module. In previous modules, you've learned about Kestra for orchestration, BigQuery for data warehousing, and dbt for transformations. Today, we're going to explore how Bruin brings all of these capabilities together into a unified platform."
- "By the end of this tutorial, you'll understand how to build complete data pipelines that handle ingestion, transformation, and quality checks—all in one tool."

### Section 1.1: The Fragmented Data Stack (2 minutes)

**Opening statement:**
- "Let's start by understanding the current state of most data teams. Typically, you need different tools for different jobs."

**List the tools:**
- "For orchestration, you might use Kestra, Airflow, or Prefect."
- "For transformations, dbt or Dataform."
- "For data quality, Great Expectations or Soda."
- "And for ingestion, Airbyte, Fivetran, or custom Python scripts."

**Pain points - say these directly:**
- "This fragmentation creates real problems. You end up with multiple configuration files scattered across different systems."
- "You're constantly context switching between tools, which slows you down."
- "Dependency management becomes a nightmare—how do you ensure your dbt models run after your Airbyte syncs complete?"
- "Error handling is inconsistent—each tool has its own way of reporting failures."
- "And deployment? You need separate CI/CD pipelines for each tool."

**Visual cue:**
- "Imagine trying to debug a pipeline failure when you have to check logs in three different places. It's not fun."

### Section 1.2: The Unified Platform Approach (2 minutes)

**Transition:**
- "This is where unified data platforms come in. Instead of managing multiple tools, what if one tool could handle ingestion, transformation, and quality?"

**Define data platform:**
- "A data platform is a single tool that handles all these functions—ingestion, transformation, and quality—with a unified configuration and execution model."
- "Everything uses the same interface, the same way of defining dependencies, the same error handling."

**Benefits - emphasize these:**
- "The biggest benefit? Reduced cognitive load. You only need to learn one tool, not four or five."
- "Deployment becomes simple—one command runs everything."
- "Dependency management is built-in. The platform knows that your transformations depend on your ingestion, and it handles the execution order automatically."
- "You get integrated lineage and observability. See your entire data flow in one place."
- "And everything is version-controlled. No more wondering which UI configuration is the source of truth."

**Real-world example:**
- "Think about it: instead of writing a dbt model, then configuring Airbyte, then setting up Great Expectations, you define everything in one place, and it just works."

### Section 1.3: Introduction to Bruin (1 minute)

**Positioning statement:**
- "Bruin is what you'd get if dbt, Airbyte, and Great Expectations had a lovechild. It combines the best of all three into a single, unified platform."

**Core philosophy - say these points:**
- "First principle: everything is version-controllable. No UI configurations, no hidden databases. Everything is text files in your repository."
- "Second: multi-technology support. SQL, Python, and R—all in the same pipeline."
- "Third: multi-source and multi-destination. Connect to dozens of sources and run on any data warehouse."
- "Fourth: mix and match. Use Python for ingestion, SQL for transformation, all in one pipeline."
- "And finally: avoid lock-in. Bruin is Apache-licensed and runs anywhere—locally, on your servers, or in the cloud."

**Transition to next video:**
- "Now that we understand why unified platforms matter, let's dive into Bruin and build our first pipeline."

---

## Video 2: Introduction to Bruin (~10 minutes)

### Opening (30 seconds)
- "In this video, we're going to get hands-on with Bruin. We'll learn the core concepts, set up our first project, and run a basic pipeline."
- "By the end, you'll understand how Bruin organizes data work and how it differs from tools like dbt."

### Section 2.1: Bruin Core Concepts (3 minutes)

**Opening:**
- "Let's start with Bruin's core concepts. There are four main ideas you need to understand: Assets, Pipelines, Connections, and Materialization."

**Assets:**
- "An Asset is anything that carries value derived from data. It could be a table in your database, a view, a file in S3, or even a machine learning model."
- "Every asset has two parts: the definition—that's the metadata that tells Bruin what the asset is—and the content—that's the actual SQL query or Python code that creates it."
- "The definition includes things like the asset name, what type it is, what it depends on, and how it should be materialized."

**Pipelines:**
- "A Pipeline is simply a group of assets that are executed together in the right order."
- "The structure is simple: you have a `pipeline.yml` file at the root, and an `assets` folder with all your asset files."
- "Bruin automatically figures out the execution order based on the dependencies you declare."

**Connections:**
- "Connections are your credentials to communicate with external platforms—your databases, your data warehouses, your cloud storage."
- "These are stored in a `.bruin.yml` file, which is gitignored because it contains sensitive information."
- "You can connect to DuckDB, BigQuery, Snowflake, PostgreSQL, and many others."

**Materialization Strategies:**
- "Materialization is how your data gets stored. Bruin offers several strategies:"
- "`table`—create or replace the table on each run."
- "`view`—create or replace a view."
- "`merge`—upsert based on a primary key, perfect for incremental loads."
- "`time_interval`—incremental processing based on time ranges."
- "And `append`—for append-only incremental loads."

### Section 2.2: Hands-On: Initialize First Bruin Project (4 minutes)

**Step 1: Initialize Project**
- "Let's create our first Bruin project. I'll use the DuckDB template to get started quickly."
- [Type command] "I'm running `bruin init duckdb`."
- "This creates a new project structure with a basic DuckDB connection and some example assets."

**Step 2: Explore Project Structure**
- "Let's look at what was created. First, the `pipeline.yml` file—this is where we configure our pipeline."
- "Next, `.bruin.yml`—this is where our database connections live. Notice it's already in `.gitignore` because it contains sensitive information."
- "And the `assets` folder—this is where all our asset files go."
- "Let's look at one of the example assets. See this `@bruin` comment block at the top? This is where we define the asset metadata."

**Step 3: Understand Asset Metadata**
- "Let me explain the key fields in this metadata block:"
- "`name`—this is the asset identifier, like `example.hello`."
- "`type`—this tells Bruin what platform and language to use. Here it's `duckdb.sql`."
- "`depends`—this lists other assets this one depends on."
- "`materialization`—this defines how the data is stored. Here it's a `table`."
- "And `columns`—this is where we define the schema with types, descriptions, and constraints like `not_null` or `unique`."

**Step 4: Run the Pipeline**
- "Now let's run this pipeline and see what happens."
- [Type command] "I'll run `bruin run pipeline.yml`."
- "Watch the output—Bruin is resolving dependencies, figuring out the execution order, and running each asset."
- "See how it executed the assets in the right order? That's dependency resolution in action."

### Section 2.3: Key Differences from dbt (3 minutes)

**Similarities:**
- "If you know dbt, a lot of this will feel familiar. Both use SQL for transformations."
- "Both have dependency management."
- "Both use Jinja templating."
- "And both are version-controlled."

**Key Differences - emphasize these:**
- "But here's where Bruin differs: First, multi-language support. Bruin natively supports Python and R, not just SQL. You can write a Python asset that ingests data, then use SQL assets to transform it—all in the same pipeline."
- "Second, built-in ingestion. You don't need a separate tool like Airbyte or Fivetran. Bruin can ingest from 50-plus sources directly."
- "Third, unified execution. One command runs everything—ingestion, transformation, quality checks. No need to coordinate between multiple tools."
- "Fourth, more materialization strategies. Beyond table and view, you get merge and time_interval for sophisticated incremental processing."
- "And finally, platform agnostic. The same pipeline code works on DuckDB, BigQuery, Snowflake—you just change the connection."

**Transition:**
- "Now that we understand Bruin basics, let's build a real-world ELT pipeline from scratch."

---

## Video 3: End-to-End ELT/ETL Pipeline (~15 minutes)

### Opening (30 seconds)
- "In this video, we're going to build a complete ELT pipeline from scratch. We'll process NYC taxi trip data, going from raw ingestion all the way to analytical reports."
- "This will demonstrate multi-tier architecture, Python and SQL integration, incremental processing, and data quality checks."

### Section 3.1: Project Overview (2 minutes)

**Business context:**
- "We're going to build a pipeline that ingests NYC taxi trip data from public HTTP sources, transforms it through multiple tiers, and generates analytical reports."
- "This is real data from the NYC Taxi and Limousine Commission, available publicly for analysis."

**Pipeline architecture:**
- "Our pipeline will have three layers:"
- "The Raw layer handles ingestion and raw data storage. We'll have a Python asset that downloads parquet files, plus lookup tables for taxi zones and payment types."
- "The Staging layer cleans and enriches the data—normalizing column names, joining with lookups, applying quality filters."
- "The Reports layer creates aggregated analytics—monthly summaries by taxi type."

**Data source:**
- "The data comes from public HTTP endpoints. The files are named like `yellow_tripdata_2021-01.parquet`—one file per taxi type per month."

### Section 3.2: Hands-On: Build the Pipeline (12 minutes)

**Step 1: Initialize Project Structure (1 minute)**
- "Let's start by creating our project. I'll create a new directory and initialize it with the DuckDB template."
- [Type commands] "`mkdir nyc-taxi`, `cd nyc-taxi`, `bruin init duckdb`."
- "This gives us a starting structure. Now let's customize it for our NYC taxi pipeline."

**Step 2: Configure Pipeline (1 minute)**
- "First, let's set up our `pipeline.yml`. I'll set the name to `nyc-taxi-pipelines`, schedule to monthly, and start date to 2021-01-01."
- "I'll configure the default DuckDB connection."
- "And I'll add a pipeline variable for `taxi_types`—this lets us configure which taxi types to process without changing code. We'll default to yellow and green."
- "Pipeline variables are powerful—you can access them in Python via the `BRUIN_VARS` environment variable, and in SQL via Jinja templating."

**Step 3: Build Raw Layer - Python Ingestion (4 minutes)**

**Create trips_raw.py:**
- "Now let's build our first asset—the Python ingestion. I'm creating `assets/raw/trips_raw.py`."
- "Here's how Python materialization works in Bruin: you write a function called `materialize()` that returns a Pandas DataFrame. Bruin handles all the database operations for you."
- "I'll read the start and end dates from environment variables—`BRUIN_START_DATE` and `BRUIN_END_DATE`. Bruin sets these automatically based on your run parameters."
- "I'll also read the `taxi_types` variable from `BRUIN_VARS`, which Bruin provides as a JSON string."
- "Then I'll loop through each month in the date range and each taxi type, download the parquet file from the HTTP URL, and combine everything into one DataFrame."
- "I'll add a `taxi_type` column and an `extracted_at` timestamp, then return the DataFrame."
- "For materialization, I'm using `merge` strategy with a composite primary key. This means if we re-run for the same month, it will update existing records instead of creating duplicates. That's perfect for incremental ingestion."

**Create taxi_zone_lookup.sql:**
- "Next, let's create the taxi zone lookup table. This is a SQL asset that uses DuckDB's `read_csv()` function with an HTTP URL."
- "DuckDB can read directly from HTTP—no need to download first. I'll use `create+replace` strategy because lookup tables should refresh on each run to get the latest zone information."
- "I'll add a primary key on `location_id` and filter out any NULL values."

**Create payment_lookup.asset.yml:**
- "For the payment lookup, I'm using a seed asset. Seeds are perfect for static reference data that you want to version control."
- "I'll create a CSV file with the payment type mappings, then create a YAML asset file that points to it."
- "This is simpler than writing SQL for static data, and it keeps the data in your repository."

**Step 4: Build Staging Layer (3 minutes)**

**Create trips_summary.sql:**
- "Now for the staging layer. This is where we clean and enrich the data."
- "First, column normalization. Yellow taxis use `tpep_pickup_datetime`, green taxis use `lpep_pickup_datetime`. I'll use COALESCE to handle both, creating a unified `pickup_time` column."
- "Same for dropoff times and other columns that differ between taxi types."
- "Then enrichment—I'll LEFT JOIN with the taxi zone lookup to get borough and zone names, and with the payment lookup to get payment type descriptions."
- "I'll calculate derived fields like `trip_duration_seconds`."
- "And I'll apply data quality filters—trips must have positive duration, reasonable trip lengths, non-negative amounts, and valid payment types."
- "For materialization, I'm using `time_interval` strategy. This is perfect for transformations because it deletes data for the date range being processed, then inserts the new results. This handles late-arriving data and corrections efficiently."
- "The incremental key is `pickup_time`, and the granularity is `timestamp`."

**Step 5: Build Reports Layer (2 minutes)**

**Create report_trips_monthly.sql:**
- "Finally, the reports layer. I'll create monthly aggregations grouped by taxi type and month."
- "I'll calculate metrics like total trips, total revenue, average fare, and average trip duration."
- "I'm also using `time_interval` materialization here, but with month-level granularity. The incremental key is `month_date`, which is the first day of each month."
- "This allows us to reprocess specific months if needed without affecting others."

**Step 6: Run and Verify (1 minute)**
- "Let's test our pipeline. First, I'll test just the ingestion asset to make sure it works."
- [Type command] "`bruin run assets/raw/trips_raw.py --start-date 2021-01-01 --end-date 2021-01-31`."
- "Good, that worked. Now let's run the full pipeline."
- [Type command] "`bruin run pipeline.yml --start-date 2021-01-01 --end-date 2021-02-28`."
- "Perfect. Let's verify the results by querying our monthly report."
- [Type command] "`bruin query --connection duckdb-default --query \"SELECT * FROM reports.report_trips_monthly LIMIT 10\"`."
- "Excellent! We can see our monthly summaries."

### Section 3.3: Key Concepts Demonstrated (1 minute)

**Summarize what we learned:**
- "Let's recap what we just built:"
- "Multi-tier architecture—we separated raw ingestion, staging transformations, and analytical reports. Each layer has a clear purpose."
- "Incremental processing—we used `merge` for ingestion to handle duplicates, and `time_interval` for transformations to efficiently update specific time ranges."
- "Python and SQL integration—Python handled the complex ingestion logic with loops and HTTP downloads, while SQL handled declarative transformations. They work seamlessly together."
- "And data quality—we applied filters in our transformations and could add more sophisticated checks using Bruin's built-in quality features."

**Transition:**
- "Now that we've built a pipeline manually, let's see how AI can accelerate this process."

---

## Video 4: AI Data Engineering (~10 minutes)

### Opening (30 seconds)
- "In this final video, we're going to explore how AI can help you build data pipelines faster. We'll set up Bruin MCP and use an AI agent to build the same NYC taxi pipeline we just created manually."
- "This is the future of data engineering—AI-assisted development that lets you focus on business logic while the AI handles the boilerplate."

### Section 4.1: Introduction to Bruin MCP (2 minutes)

**What is MCP:**
- "MCP stands for Model Context Protocol. It's a standard that allows AI agents to interact with tools and understand their capabilities."
- "Bruin MCP bridges the gap between the Bruin CLI and AI editors like Cursor or Claude Code."
- "When you configure Bruin MCP, your AI agent can understand Bruin's concepts, access documentation, and even execute commands."

**What Bruin MCP can do:**
- "With Bruin MCP enabled, your AI agent can:"
- "Analyze data—query your databases and understand schemas."
- "Ingest data—set up ingestion from 50-plus sources with natural language."
- "Build pipelines—create assets and pipelines from descriptions."
- "Compare data—diff tables between environments to validate changes."
- "And answer questions—access Bruin's complete documentation to give you accurate answers."

**Real-world example:**
- "Imagine saying 'bring all my Shopify order data into BigQuery' and having the AI set up the entire ingestion pipeline for you. That's what Bruin MCP enables."

### Section 4.2: Configure Bruin MCP for Cursor (3 minutes)

**Step 1: Verify Installation**
- "First, let's make sure Bruin CLI is installed and accessible."
- [Type command] "I'll run `bruin --version` to verify."
- "Good, Bruin is installed."

**Step 2: Configure Cursor IDE**
- "Now let's configure Cursor to use Bruin MCP."
- "I'll open Cursor Settings, navigate to MCP & Integrations, and click Add Custom MCP."
- "I'll add this configuration:"
- [Show JSON] "A JSON object with `mcpServers`, then `bruin` as the server name, with `command` set to `bruin` and `args` set to `[\"mcp\"]`."
- "This tells Cursor to start the Bruin MCP server when needed."

**Step 3: Verify Setup**
- "After saving, I need to restart Cursor for the changes to take effect."
- "Once restarted, I can verify that Bruin MCP appears in the available MCP servers."
- "Let's test it by asking the AI: 'What is Bruin?'"
- "The AI should be able to answer using Bruin's documentation through MCP."

### Section 4.3: Hands-On: AI-Assisted Pipeline Creation (4 minutes)

**Step 1: Create Project Structure**
- "Now let's use AI to build our NYC taxi pipeline. First, I'll ask the AI to create a new Bruin project."
- [Type in chat] "I'll say: 'Create a new Bruin project for NYC taxi data analysis'."
- "The AI should initialize the project, set up the `.bruin.yml` with DuckDB connection, and create a basic `pipeline.yml`."
- "Let's see what it creates... Good, it's setting up the project structure."

**Step 2: Build Pipeline with AI Prompt**
- "Now for the fun part. I'll give the AI a detailed prompt describing what we want to build."
- [Show prompt] "I'll paste this prompt:"
- "'I want to build a Bruin pipeline for NYC taxi data analysis. The pipeline should: Ingest NYC taxi trip data from HTTP parquet files with this URL pattern, use Python materialization with merge strategy, ingest taxi zone lookup from CSV, create a seed asset for payment lookup, build a staging layer that normalizes columns and enriches data, and build monthly reports. The pipeline should process data from 2021-01-01 onwards.'"
- "Watch as the AI uses Bruin MCP to understand the concepts and generate the code."
- "It's creating the assets, setting up the metadata correctly, and following Bruin best practices."

**Step 3: Review AI-Generated Code**
- "Let's review what the AI created. Notice how it:"
- "Used the correct asset types—Python for ingestion, SQL for transformations."
- "Set up materialization strategies correctly—merge for ingestion, time_interval for transformations."
- "Added proper metadata—primary keys, column definitions, dependencies."
- "The AI is using Bruin MCP to access documentation and understand the correct patterns."

**Step 4: Refine and Test**
- "The AI might have missed a few details. Let's ask it to fix any issues."
- [Type in chat] "I'll say: 'The staging layer needs to filter out trips with negative amounts.'"
- "Good, it's updating the code. Now let's run the pipeline."
- [Type command] "`bruin run pipeline.yml --start-date 2021-01-01 --end-date 2021-01-31`."
- "Perfect! The AI-generated pipeline works. Compare this to how long it took us to build it manually—the AI did it in minutes."

### Section 4.4: The Future of AI Data Engineering (1 minute)

**Current capabilities:**
- "What we just saw is available today:"
- "Natural language to pipeline code."
- "Documentation-aware code generation."
- "Schema-aware query generation."
- "Best practice enforcement."

**Future possibilities:**
- "But this is just the beginning. Soon, AI will:"
- "Automatically suggest optimizations."
- "Detect data quality issues before they become problems."
- "Recommend performance tuning."
- "Create self-documenting pipelines."

**Best practices:**
- "Remember: AI is a powerful assistant, not a replacement. Always review and understand the generated code."
- "Use AI for boilerplate and repetitive tasks, so you can focus on business logic."
- "And always test thoroughly before putting anything into production."

### Wrap-Up (1 minute)

**Key takeaways:**
- "Let's recap what we've learned:"
- "Unified platforms reduce complexity compared to fragmented tools."
- "Bruin combines ingestion, transformation, and quality into one tool."
- "Multi-language support means you can use SQL, Python, and R in the same pipeline."
- "And AI can accelerate pipeline development significantly when configured properly."

**Next steps:**
- "To continue learning:"
- "Explore more Bruin features like lineage visualization and data diff."
- "Try building pipelines for your own data sources."
- "Experiment with different materialization strategies."
- "And leverage AI for rapid prototyping."

**Resources:**
- "Check out the Bruin documentation at getbruin.com/docs."
- "The Bruin MCP setup guide is at getbruin.com/docs/bruin/getting-started/bruin-mcp.html."
- "And the GitHub repository is github.com/bruin-data/bruin."

**Closing:**
- "Thanks for watching! You now have the foundation to build production-ready data pipelines with Bruin. Happy building!"

---

## Script Notes

### Timing Guidelines
- **Video 1**: Keep it high-level and conceptual. Don't get into tool comparisons.
- **Video 2**: Pause frequently for students to type along. Show, don't just tell.
- **Video 3**: This is the core learning. Take time to explain each concept. Pause for questions.
- **Video 4**: Show the "wow" factor, but emphasize that understanding is still important.

### Visual Cues
- **Video 1**: Use diagrams or slides to show fragmented vs unified approach.
- **Video 2**: Screen share the entire time. Show file structure, code, and terminal output.
- **Video 3**: Screen share with code editor and terminal side-by-side. Show the data at each step.
- **Video 4**: Show Cursor IDE with chat panel. Demonstrate the AI generating code in real-time.

### Common Mistakes to Address
- "If you see an error about connections, check your `.bruin.yml` file."
- "Remember, Python assets need a `requirements.txt` file in the pipeline root."
- "If MCP isn't working, make sure you restarted Cursor after configuration."
- "The `@bruin` comment block must be at the very top of SQL assets."

### Engagement Tips
- Ask rhetorical questions: "Why do you think we use merge instead of append here?"
- Use real-world analogies: "Think of materialization like choosing between a table and a view in SQL."
- Show mistakes: "Oops, I forgot to add the dependency. Let me fix that."
- Celebrate wins: "Perfect! Look at that beautiful output."

