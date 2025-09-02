# Bruin MCP (Model Context Protocol)

Bruin MCP is an integration layer that connects AI editors (like Cursor) with Bruin CLI. It enables AI assistants to directly access Bruin documentation and provide better guidance when building pipelines


## Setup in Claude Code

To use Bruin MCP in Claude Code, run the following command in your terminal:

```
claude mcp add bruin -- bruin mcp
```

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

- "How do I create a BigQuery asset in Bruin?"
- "How is a pipeline.yml file configured in Bruin?"
- "What data sources does Bruin support for ingestion?"
- "How do I set up a Snowflake connection in Bruin?"
- "How can I connect to my PostgreSQL database and run a query with Bruin?"
- "How do I create a table in my data warehouse using Bruin?"
- "How can I build a data pipeline in Bruin for ingesting CSV files?"
- "How do I run data quality checks on my tables in Bruin?"

Or you can give direct commands like:

- "Create a BigQuery asset in Bruin"
- "Configure a pipeline.yml file in Bruin"
- "Show me the data sources Bruin supports for ingestion"
- "Set up a Snowflake connection in Bruin"
- "Connect to my PostgreSQL database and run a query with Bruin"
- "Create a table in my data warehouse using Bruin"
- "Build a data pipeline in Bruin for ingesting CSV files"
- "Run data quality checks on my tables in BVruin"

The AI assistant will answer these questions using up-to-date Bruin documentation and provide you with accurate examples. It can also execute Bruin commands directly to help you connect to databases, run queries, perform ingestion tasks, and create complete data pipelines.