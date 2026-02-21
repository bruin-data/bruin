# Core Concepts

Bruin is built around a few simple but powerful concepts that enable you to build, run, and manage data pipelines effectively. Understanding these concepts will help you make the most out of Bruin.

## Overview

| Concept | Description |
|---------|-------------|
| [Asset](/assets/definition-schema) | Anything that carries value derived from data (tables, views, files, models) |
| [Pipeline](/pipelines/definition) | A group of assets that are executed together in dependency order |
| [Commands](/commands/overview) | CLI operations to run, validate, and manage your pipelines |
| [Project](/core-concepts/project) | A Git repository containing your pipelines, configured via `.bruin.yml` |
| [Variables](/core-concepts/variables) | Dynamic values injected into your asset code during execution |

## Asset

An **asset** is anything that carries value derived from data:

- A table or view in your database
- A file in S3 or GCS
- A machine learning model
- A document in Excel, Google Sheets, or Notion

Assets consist of a **definition** (metadata) and **content** (the actual query or logic). Bruin supports multiple asset types including SQL, Python, R, and ingestr.

[Learn more about Assets →](/assets/definition-schema)

## Pipeline

A **pipeline** is a collection of assets that execute together in the correct dependency order. Pipelines are defined using a `pipeline.yml` file and provide:

- Scheduling configuration
- Default connection settings
- Pipeline-level variables
- Notification settings

```text
my-pipeline/
├── pipeline.yml
└── assets/
    ├── ingest_data.asset.yml
    ├── transform.sql
    └── export.py
```

[Learn more about Pipelines →](/pipelines/definition)

## Commands

Bruin provides a comprehensive CLI for managing your data pipelines. Commands can be executed in:

- **Terminal**: Direct CLI usage
- **VS Code Extension**: Visual interface with integrated features
- **AI Agents**: Via [Bruin MCP](/getting-started/bruin-mcp) for programmatic access

Key commands include:

| Command | Description |
|---------|-------------|
| [`run`](/commands/run) | Execute pipelines or individual assets |
| [`validate`](/commands/validate) | Check pipeline configuration and syntax |
| [`init`](/commands/init) | Create new Bruin projects |
| [`lineage`](/commands/lineage) | Visualize asset dependencies |

[Learn more about Commands →](/commands/overview)

## Project

A **project** is a Git repository that contains your data pipelines. The project is configured via the `.bruin.yml` file located at the root of your repository.

The `.bruin.yml` file contains:

- **[Connections](/core-concepts/connections)**: Built-in connection configurations for data platforms (BigQuery, Snowflake, etc.) and ingestion sources
- **[Secrets](/core-concepts/secrets)**: Custom credentials and API keys that can be injected into your assets
- **Environments**: Configuration contexts that enable you to run the same pipeline code against different targets (local, staging, production)

[Learn more about Project →](/core-concepts/project)

## Variables

Variables are dynamic values provided during execution and injected into your asset code. There are two types:

- **[Built-in Variables](/core-concepts/variables#built-in-variables)**: Automatically injected by Bruin (e.g., `start_date`, `end_date`, `pipeline`, `run_id`)
- **[Custom Variables](/core-concepts/variables#custom-variables)**: User-defined variables specified in `pipeline.yml`

Variables enable parameterized pipelines—for example, processing data for specific date ranges or customer segments without modifying code.

[Learn more about Variables →](/core-concepts/variables)
