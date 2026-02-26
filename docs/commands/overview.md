# Commands Overview

Bruin provides a comprehensive CLI for managing your data pipelines. Commands can be executed in multiple ways:

- **Terminal**: Direct CLI usage via `bruin <command>`
- **VS Code Extension**: Visual interface with integrated command execution
- **AI Agents**: Programmatic access via [Bruin MCP](/getting-started/bruin-mcp)

## Getting Help

```bash
# List all available commands
bruin --help

# Get help for a specific command
bruin run --help
bruin validate --help
```

## Command Reference

### Pipeline Execution

| Command | Description |
|---------|-------------|
| [`run`](/commands/run) | Execute pipelines or individual assets |
| [`validate`](/commands/validate) | Check pipeline configuration and syntax without executing |

### Project Management

| Command | Description |
|---------|-------------|
| [`init`](/commands/init) | Create a new Bruin project from a template |
| [`clean`](/commands/clean) | Remove temporary files and build artifacts |
| [`format`](/commands/format) | Format asset files for consistency |

### Connections & Environments

| Command | Description |
|---------|-------------|
| [`connections`](/commands/connections) | List, add, delete, and test connections |
| [`environments`](/commands/environments) | Manage deployment environments |

### Development & Debugging

| Command | Description |
|---------|-------------|
| [`render`](/commands/render) | Preview rendered Jinja templates |
| [`lineage`](/commands/lineage) | Visualize asset dependencies |
| [`query`](/commands/query) | Execute ad-hoc queries against connections |
| [`data-diff`](/commands/data-diff) | Compare data between connections |

### Asset Operations

| Command | Description |
|---------|-------------|
| [`import`](/commands/import) | Import existing database tables as Bruin assets |
| [`patch`](/commands/patch) | Apply patches to asset definitions |
| [`ai-enhance`](/commands/ai-enhance) | Enhance asset metadata using AI |

## Common Workflows

### Running a Pipeline

```bash
# Run the pipeline in the current directory
bruin run

# Run a specific pipeline
bruin run ./pipelines/analytics/

# Run a specific asset
bruin run ./pipelines/analytics/assets/daily_summary.sql

# Run with a specific environment
bruin run --environment production

# Run for a specific date range
bruin run --start-date 2024-01-01 --end-date 2024-01-31
```

### Validating Before Running

```bash
# Validate pipeline syntax and configuration
bruin validate

# Validate a specific pipeline
bruin validate ./pipelines/analytics/
```

### Creating a New Project

```bash
# Initialize with the default template
bruin init default my-project

# Initialize with a specific template
bruin init chess my-chess-project
```

### Testing Connections

```bash
# List all configured connections
bruin connections list

# Test a specific connection
bruin connections test --name my-postgres-connection
```

### Debugging Templates

```bash
# Render a SQL asset to see the final query
bruin render ./assets/my_query.sql

# Render with specific date parameters
bruin render ./assets/my_query.sql --start-date 2024-01-01 --end-date 2024-01-02
```

## Global Flags

These flags work with most commands:

| Flag | Description |
|------|-------------|
| `--debug` | Enable debug logging |
| `--environment` | Specify the environment to use |
| `--config-file` | Path to a custom `.bruin.yml` file |
| `--help` | Show help for the command |

## VS Code Extension

The [Bruin VS Code Extension](/vscode-extension/overview) provides a visual interface for many CLI commands:

- Run pipelines and assets with a single click
- View real-time execution logs
- Explore asset lineage graphically
- Preview rendered queries

## Bruin MCP (AI Agent Integration)

[Bruin MCP](/getting-started/bruin-mcp) enables AI agents and tools to interact with Bruin programmatically. This allows:

- Natural language pipeline execution
- Automated pipeline management
- Integration with AI-powered development workflows

## Related Topics

- [Run Command](/commands/run) - Detailed execution options
- [Validate Command](/commands/validate) - Pipeline validation
- [VS Code Extension](/vscode-extension/overview) - Visual interface
- [Bruin MCP](/getting-started/bruin-mcp) - AI agent integration
