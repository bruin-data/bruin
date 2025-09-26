# Claude Code Asset Type for Bruin

This example demonstrates the new `agent.claude_code` asset type that integrates Claude AI into your Bruin data pipelines.

## Features

- **AI-Powered Analysis**: Use Claude's advanced language model to analyze data, generate code, and provide insights
- **Automatic CLI Installation**: The operator automatically installs Claude CLI if not present using the official installation script
- **Jinja Template Support**: Prompts support all Bruin Jinja variables for dynamic content generation
- **Cross-Platform**: Works on macOS, Linux, and Windows (with WSL)

## Installation

The Claude CLI is automatically installed when you first run an `agent.claude_code` asset. The installation uses the official script:

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

**Note for Windows users**: You'll need WSL (Windows Subsystem for Linux) or manually install Claude CLI.

## Usage

Create an asset file with type `agent.claude_code`:

```yaml
name: my_ai_assistant
type: agent.claude_code
parameters:
  prompt: |
    Your prompt here...
```

### Advanced Configuration

For advanced features like JSON output, model selection, session management, and security controls, see **[ADVANCED.md](./ADVANCED.md)**.

### Simple Example

```yaml
name: my_agentic_workload
type: agent.claude_code
description: Simple AI analysis task
parameters:
  prompt: |
    Analyze the data pipeline and provide insights.
    
    Please provide:
    1. A summary of the data processed
    2. Any anomalies or patterns detected
    3. Recommendations for optimization
```

### Advanced Example with Jinja Variables

```yaml
name: data_analysis_agent
type: agent.claude_code
description: AI-powered data analysis with context
parameters:
  prompt: |
    Perform a comprehensive data analysis for:
    
    Date Range: {{start_date}} to {{end_date}}
    DateTime Range: {{start_datetime}} to {{end_datetime}}
    Pipeline: {{pipeline}}
    Run ID: {{run_id}}
    
    Tasks:
    1. Analyze data quality metrics
    2. Identify anomalies or outliers
    3. Suggest performance optimizations
    4. Generate a summary report
    
    Format your response as structured JSON.
```

## Available Jinja Variables

The prompt parameter supports all standard Bruin Jinja variables:

- `{{start_date}}` - Start date in YYYY-MM-DD format
- `{{start_date_nodash}}` - Start date in YYYYMMDD format
- `{{start_datetime}}` - Start datetime in YYYY-MM-DDTHH:MM:SS format
- `{{start_timestamp}}` - Start timestamp with timezone
- `{{end_date}}` - End date in YYYY-MM-DD format
- `{{end_date_nodash}}` - End date in YYYYMMDD format
- `{{end_datetime}}` - End datetime in YYYY-MM-DDTHH:MM:SS format
- `{{end_timestamp}}` - End timestamp with timezone
- `{{pipeline}}` - Name of the current pipeline
- `{{run_id}}` - Unique identifier for this run

You can also use custom variables passed through the `var` context and apply various filters like `add_days`, `add_hours`, `add_minutes`, etc.

## Running the Example

1. Build the updated Bruin CLI:
   ```bash
   go build -o bruin .
   ```

2. Validate the pipeline:
   ```bash
   ./bruin validate examples/claude-code
   ```

3. Run the pipeline:
   ```bash
   ./bruin run examples/claude-code
   ```

## Requirements

- Bash and curl (for automatic installation)
- Claude API key (will be prompted during first use)
- Internet connection for API calls

## Troubleshooting

If Claude CLI installation fails:
1. Ensure bash and curl are installed
2. Check internet connectivity
3. Manually install Claude CLI from https://claude.ai/
4. For Windows, use WSL or install manually

## Use Cases

- **Data Quality Analysis**: Analyze datasets for quality issues
- **Code Generation**: Generate SQL queries or Python scripts
- **Documentation**: Auto-generate documentation from data schemas
- **Anomaly Detection**: Identify unusual patterns in data
- **Report Generation**: Create summaries and insights from pipeline runs
- **Optimization Suggestions**: Get AI-powered recommendations for pipeline improvements