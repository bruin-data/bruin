# Bruin MCP (Model Context Protocol)

Bruin MCP is a feature that acts as a bridge between AI editors (like Cursor) and the Bruin CLI. This feature allows AI assistants to directly access Bruin documentation and provide better assistance when creating pipelines.


## Setup in Cursor IDE

To use Bruin MCP in Cursor IDE, go to Settings > Tools and Integrations:

1. Add the following configuration to your `mcp.json` file:

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

## Usage

After completing the setup, the AI assistant in Cursor IDE will automatically have access to Bruin documentation and can help you with:

- Using correct syntax when creating pipelines
- Learning about supported data sources and their configurations
- Understanding asset types and properties
- Using Bruin commands correctly
- Connecting to databases using existing connections
- Running SQL queries against connected databases
- Performing data ingestion tasks
- Creating tables in target databases
- Building complete data pipelines

## Example Usage

Once MCP setup is complete, you can ask questions in Cursor IDE like:

- "How do I create an asset for BigQuery in Bruin?"
- "How is a pipeline.yml file configured?"
- "What data sources do you support?"
- "How do I set up a Snowflake connection?"
- "Connect to my PostgreSQL database and run a query"
- "Create a table in my data warehouse"
- "Help me build a data pipeline for ingesting CSV files"
- "Run data quality checks on my tables"

The AI assistant will answer these questions using up-to-date Bruin documentation and provide you with accurate examples. It can also execute Bruin commands directly to help you connect to databases, run queries, perform ingestion tasks, and create complete data pipelines.

