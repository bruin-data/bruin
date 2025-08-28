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
      "args": ["mcp"],
      "env": {}
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

## Example Usage

Once MCP setup is complete, you can ask questions in Cursor IDE like:

- "How do I create an asset for BigQuery in Bruin?"
- "How is a pipeline.yml file configured?"
- "What data sources do you support?"
- "How do I set up a Snowflake connection?"

The AI assistant will answer these questions using up-to-date Bruin documentation and provide you with accurate examples.

