# Additional Features

The Bruin VS Code extension includes several additional features that enhance your development workflow and provide convenient utilities for common tasks.

## Cron Schedule Preview

The extension provides human-readable cron schedule previews directly in your pipeline YAML files using CodeLens functionality.

### Features
- **Inline Preview**: Cron expressions are automatically translated to human-readable format
- **CodeLens Integration**: Preview appears directly above cron expressions in pipeline.yml files
- **Real-time Updates**: Previews update automatically as you modify cron expressions
- **Easy Understanding**: Complex cron syntax becomes immediately understandable

### How to Use
1. **Open Pipeline File**: Navigate to your `pipeline.yml` file
2. **Add Cron Expression**: Include a cron schedule in your pipeline configuration
3. **View Preview**: The human-readable preview appears automatically above the cron expression
4. **Modify Schedule**: Edit the cron expression to see the preview update in real-time

![Cron Preview CodeLens](../public/vscode-extension/additional-features/cron-preview.gif)

## Pipeline Asset and Dependency Management

Enhanced pipeline management capabilities with collapsible sections and external dependency support.

### Features
- **Collapsible Sections**: Organize complex pipeline configurations with expandable/collapsible sections
- **Improved Layout**: Better visual organization of pipeline components and dependencies
- **External Dependencies**: Full support for managing external pipeline dependencies
- **Visual Hierarchy**: Clear visual representation of asset relationships and dependencies

### Benefits
- **Better Organization**: Large pipelines become more manageable with collapsible sections
- **External Integration**: Seamlessly work with assets from external pipelines
- **Visual Clarity**: Improved layout makes complex dependencies easier to understand
- **Efficient Navigation**: Quickly collapse/expand sections to focus on relevant parts

## Query Export to CSV

Export query results directly to CSV files for further analysis or sharing.

### Features
- **One-Click Export**: Export query results with a single button click
- **CSV Format**: Results are exported in standard CSV format for compatibility
- **Large Dataset Support**: Handle export of large query results efficiently
- **File Management**: Choose export location and filename through standard file dialogs

### How to Use
1. **Run Query**: Execute a query in the Query Preview Panel
2. **Export Results**: Click the "Export" button in the query results interface
3. **Choose Location**: Select where to save the CSV file
4. **Complete Export**: The query results are saved as a CSV file

## Fill from Query

Automatically populate asset metadata from query results for SQL files.

### Features
- **SQL File Integration**: Available when working with SQL files containing valid queries
- **Conditional Display**: Button appears only when the active file is a SQL file
- **Metadata Population**: Automatically extract and populate asset information from query structure
- **Smart Detection**: Intelligently analyze query structure to infer asset properties

### How to Use
1. **Open SQL File**: Navigate to a SQL file with a valid query
2. **Fill Button**: The "Fill from Query" button appears when applicable
3. **Auto-populate**: Click the button to automatically fill asset metadata based on the query
4. **Review Results**: Verify and adjust the populated metadata as needed

## Run Command Formatting

Enhanced formatting for Bruin run commands to improve readability.

### Features
- **Multi-line Format**: Long run commands are formatted across multiple lines for better readability
- **Readable Structure**: Command parameters are organized in a clear, structured format
- **Copy-friendly**: Formatted commands can be easily copied and used in other contexts
- **Visual Clarity**: Complex commands become easier to understand and verify

This feature makes it easier to:
- **Review Commands**: Understand what a run command will do before execution
- **Share Commands**: Copy formatted commands for documentation or sharing
- **Debug Issues**: Identify problems in complex command structures
- **Learn Syntax**: Better understand Bruin command syntax and options