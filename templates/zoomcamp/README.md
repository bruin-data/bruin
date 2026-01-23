# Build Your First End-to-End Data Platform

This hands-on tutorial guides you through building a **complete NYC Taxi data pipeline** from scratch using Bruin—a unified CLI tool for data ingestion, transformation, and quality.

You'll learn to build a production-ready ELT pipeline that:
- **Ingests** real NYC taxi trip data from public APIs using Python
- **Transforms** and cleans raw data with SQL, applying incremental strategies and deduplication
- **Reports** aggregated analytics with built-in quality checks
- **Deploys** to cloud infrastructure (MotherDuck)

This is a learn-by-doing experience with AI assistance available through Bruin MCP. Follow the comprehensive `tutorial.md` for step-by-step guidance, or use the AI agent to build the entire pipeline interactively.

## Learning Goals

- Understand how Bruin projects are structured (`pipeline.yml` + `assets/`)
- Use **materialization strategies** intentionally (append, time_interval, etc.)
- Declare **dependencies** and explore lineage (`bruin lineage`)
- Apply **metadata** (columns, primary keys, descriptions) and **quality checks**
- Parameterize runs with **pipeline variables**

## Tutorial Outline

- **Part 1**: What is a Data Platform? - Learn about modern data stack components and where Bruin fits in
- **Part 2**: Setting Up Your First Bruin Project - Install Bruin, initialize a project, and configure environments
- **Part 3**: End-to-End NYC Taxi ELT Pipeline - Build ingestion, staging, and reporting layers with real data
- **Part 4**: Data Engineering with AI Agent - Use Bruin MCP to build pipelines with AI assistance
- **Part 5**: Deploy to MotherDuck - Deploy your local pipeline to cloud-hosted DuckDB

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
