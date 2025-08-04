# AGENTS.md - AI Agent Contribution Guide

This document provides comprehensive information for AI agents to understand and contribute to the Bruin project effectively.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Core Concepts](#architecture--core-concepts)
3. [Development Environment](#development-environment)
4. [Build System](#build-system)
5. [CLI Commands & Structure](#cli-commands--structure)
6. [Codebase Organization](#codebase-organization)
7. [Testing Strategy](#testing-strategy)
8. [Contributing Guidelines](#contributing-guidelines)
9. [Common Development Tasks](#common-development-tasks)

## Project Overview

Bruin is an end-to-end data framework that combines data ingestion, transformation, and quality into a single tool. Think of it as "if dbt, Airbyte, and Great Expectations had a lovechild."

### Core Features
- **Data Ingestion**: Using `ingestr` and Python scripts
- **Transformations**: SQL & Python on multiple platforms (BigQuery, Snowflake, DuckDB, etc.)
- **Data Quality**: Built-in quality checks and validations
- **Materializations**: Table/view materializations, incremental tables
- **Python Isolation**: Using `uv` for isolated Python environments
- **Templating**: Jinja templating for reusable code
- **Lineage**: Dependency visualization and tracking
- **Multi-platform**: Runs locally, on EC2, or GitHub Actions
- **Secrets Management**: Environment variable injection
- **VS Code Extension**: Enhanced developer experience

### Design Principles
1. **Version-controllable text**: Everything configured via text files, no UI/database configs
2. **Multi-technology support**: SQL and Python natively, with pre-built binaries for complex use cases
3. **Multi-source/destination**: Support diverse sources and destinations
4. **Mix-and-match**: Single pipelines can combine different technologies, sources, and destinations
5. **Avoid lock-in**: Open-source Apache-licensed, runs anywhere

## Architecture & Core Concepts

### Assets
Anything that carries value derived from data:
- Tables/views in databases
- Files in S3/GCS
- Machine learning models
- Documents (Excel, Google Sheets, Notion, etc.)

Assets consist of:
- **Definition**: Metadata enabling Bruin to understand the asset
- **Content**: The actual query/logic that creates the asset

### Pipelines
Groups of assets executed together in dependency order. Structure:
```
my-pipeline/
├─ pipeline.yml
└─ assets/
   ├─ asset1.sql
   └─ asset2.py
```

### Pipeline Runs
Execution instances containing one or more asset instances with specific configuration and timing.

## Development Environment

### Prerequisites
- **Go**: Version 1.23.0+ (see `go.mod`)
- **Python**: For Python asset development and formatting
- **CGO**: Required for DuckDB support
- **Git**: For version control and repository detection

### Dependencies
The project uses extensive Go dependencies including:
- CLI framework: `github.com/urfave/cli/v2`
- Database drivers: BigQuery, Snowflake, PostgreSQL, MySQL, DuckDB, etc.
- Cloud SDKs: AWS, GCP
- Templating: Jinja via `github.com/nikolalohinski/gonja/v2`
- Testing: `github.com/stretchr/testify`

## Build System

The Makefile provides comprehensive build and development targets:

### Core Targets

#### Build Targets
```bash
make build          # Build with DuckDB support (CGO_ENABLED=1)
make build-no-duckdb # Build without DuckDB (CGO_ENABLED=0)
```

#### Development Targets
```bash
make deps           # Install dependencies and tools
make clean          # Remove build artifacts
make format         # Format Go and Python code
make tools          # Install development tools (gci, gofumpt, golangci-lint)
make tools-update   # Update development tools
```

#### Testing Targets
```bash
make test                      # Run unit tests
make test-unit                 # Run unit tests specifically
make integration-test          # Full integration tests with ingestr
make integration-test-light    # Light integration tests without ingestr
make integration-test-cloud    # Cloud-specific integration tests
```

#### Development Utilities
```bash
make lint-python                     # Format and lint Python code
make refresh-integration-expectations # Update integration test expectations
```

### Build Configuration
- **Version**: Set via `main.Version` variable, defaults to `dev-$(git describe --tags --abbrev=0)`
- **Telemetry**: Controlled via `TELEMETRY_KEY` and `TELEMETRY_OPTOUT` environment variables
- **Tags**: Uses `no_duckdb_arrow` for standard builds, `bruin_no_duckdb` for no-DuckDB builds

## CLI Commands & Structure

### Main Application Structure (`main.go`)
The CLI is built using `github.com/urfave/cli/v2` with these core commands:

```go
Commands: []*cli.Command{
    cmd.Lint(&isDebug),           // Lint pipelines and assets
    cmd.Run(&isDebug),            // Run pipelines/assets
    cmd.Render(),                 // Render Jinja templates
    cmd.Lineage(),                // Generate lineage graphs
    cmd.CleanCmd(),               // Clean up resources
    cmd.Format(&isDebug),         // Format code
    cmd.Docs(),                   // Open documentation
    cmd.Init(),                   // Initialize new projects
    cmd.Internal(),               // Internal/debugging commands
    cmd.Environments(&isDebug),   // Manage environments
    cmd.Connections(),            // Manage connections
    cmd.Query(),                  // Execute queries
    cmd.Patch(),                  // Patch assets
    cmd.DataDiffCmd(),            // Compare data between connections
    cmd.Import(),                 // Import database tables as assets
    versionCommand,               // Version information
}
```

### Key Command Categories

#### Primary Commands
- **`run`**: Execute pipelines with flags for workers, dates, environments, full-refresh
- **`lint`**: Validate pipeline syntax and configuration
- **`init`**: Bootstrap new Bruin projects
- **`lineage`**: Generate dependency graphs

#### Management Commands
- **`connections`**: List, add, delete, ping database connections
- **`environments`**: Manage deployment environments
- **`import`**: Import existing database tables as Bruin assets

#### Development Commands  
- **`format`**: Code formatting
- **`render`**: Template rendering for debugging
- **`docs`**: Open documentation (with `--open` flag for browser)

#### Internal Commands (Hidden)
- **`internal parse-asset`**: Parse individual assets
- **`internal parse-pipeline`**: Parse entire pipelines
- **`internal connections`**: Connection schema operations

## Codebase Organization

### Package Structure (`pkg/`)
The codebase is organized into focused packages:

#### Core Packages
- **`pipeline/`**: Pipeline parsing, execution, and management
- **`config/`**: Configuration file handling (.bruin.yml)
- **`connection/`**: Database connection management
- **`executor/`**: Asset execution engine
- **`lineage/`**: Dependency tracking and visualization
- **`query/`**: Query execution and management

#### Data Platform Packages
Each supported platform has its own package:
- **Database platforms**: `bigquery/`, `snowflake/`, `postgres/`, `mysql/`, `duckdb/`, `clickhouse/`, `athena/`, `mssql/`, `databricks/`, `oracle/`, `sqlite/`, `trino/`, `synapse/`, `hana/`, `spanner/`
- **Cloud storage**: `s3/`, `gcs/`
- **Ingestion sources**: 50+ packages for different data sources (e.g., `shopify/`, `hubspot/`, `salesforce/`, `stripe/`, etc.)

#### Utility Packages
- **`jinja/`**: Template processing
- **`python/`**: Python asset execution
- **`lint/`**: Code linting and validation
- **`diff/`**: Data comparison functionality
- **`path/`**: File system utilities
- **`git/`**: Git repository operations
- **`telemetry/`**: Usage analytics
- **`secrets/`**: Secret management
- **`logger/`**: Logging utilities

### Command Implementation (`cmd/`)
Each CLI command is implemented in its own file:
- Command structure definition
- Flag parsing and validation
- Business logic delegation to appropriate packages
- Error handling and output formatting

## Testing Strategy

### Test Types

#### Unit Tests
- **Location**: Throughout `pkg/` packages with `*_test.go` files
- **Execution**: `make test-unit`
- **Coverage**: Race detection enabled, 10-minute timeout
- **Scope**: Excludes cloud integration tests

#### Integration Tests
- **Light Integration**: `make integration-test-light` (excludes ingestr)
- **Full Integration**: `make integration-test` (includes ingestr)
- **Cloud Integration**: `make integration-test-cloud` (cloud platforms)

#### Test Data
- **Location**: `integration-tests/test-pipelines/`
- **Coverage**: Parse tests, lineage tests, execution tests
- **Expectations**: JSON files with expected outputs
- **Refresh**: `make refresh-integration-expectations` updates expectations

### Test Patterns
- Mock databases using `github.com/DATA-DOG/go-sqlmock`
- PostgreSQL mocking with `github.com/pashagolub/pgxmock/v3`
- Concurrent testing with `github.com/sourcegraph/conc`
- File system abstraction with `github.com/spf13/afero`

## Contributing Guidelines

### Code Style & Formatting

#### Go Code
Tools automatically installed and run via `make format`:
- **`gci`**: Import organization
- **`gofumpt`**: Stricter Go formatting
- **`golangci-lint`**: Comprehensive linting (10m timeout)
- **`go vet`**: Static analysis

#### Python Code
Tools run via `make lint-python`:
- **`ruff format`**: Code formatting
- **`ruff check --fix`**: Linting with auto-fixes

### Development Workflow

1. **Setup**: `make deps` to install tools and dependencies
2. **Development**: Edit code with VS Code extension for enhanced experience  
3. **Formatting**: `make format` before committing
4. **Testing**: `make test` for unit tests, integration tests as appropriate
5. **Building**: `make build` to verify compilation

### Adding New Data Platforms

1. **Create package**: `pkg/newplatform/`
2. **Implement interfaces**: Connection, query execution, schema introspection
3. **Add CLI command**: Register in main command list
4. **Add tests**: Unit and integration tests
5. **Update documentation**: Add to supported platforms list

### Adding New CLI Commands

1. **Create command file**: `cmd/newcommd.go`
2. **Implement command structure**: Using `cli.Command` pattern
3. **Add business logic**: In appropriate `pkg/` package
4. **Register command**: In `main.go` commands slice
5. **Add tests**: Command and business logic tests

## Common Development Tasks

### Running Locally
```bash
# Basic build and run
make build
./bin/bruin --help

# Development mode with debug
make build
./bin/bruin --debug [command]
```

### Adding New Asset Types
1. Define asset type in `pkg/pipeline/asset.go`
2. Implement execution logic in `pkg/executor/`
3. Add parsing logic if needed
4. Update lineage detection if applicable
5. Add tests and integration tests

### Debugging Integration Tests
```bash
# Run specific test pipeline
cd integration-tests
../bin/bruin run test-pipelines/your-test

# Refresh expectations after changes
make refresh-integration-expectations
```

### Working with Templates
```bash
# Test template rendering
./bin/bruin render path/to/template.sql

# Test complete pipeline parsing
./bin/bruin internal parse-pipeline path/to/pipeline
```

### Database Connection Testing
```bash
# List connections
./bin/bruin connections list

# Test connection
./bin/bruin connections ping connection-name

# Add new connection
./bin/bruin connections add
```

---

This guide provides the foundational knowledge needed to contribute effectively to the Bruin project. For specific implementation details, refer to the extensive documentation in the `docs/` directory and examine existing patterns in the codebase. 