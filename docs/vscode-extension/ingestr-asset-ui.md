# Ingestr Asset UI

The Ingestr Asset UI provides a visual interface for creating and managing data ingestion assets, allowing you to set up data pipelines between different sources and destinations with intuitive dropdown selections.

## Overview

Ingestr assets are used to move data from various sources (databases, APIs, files) to destination systems (data warehouses, databases). The Ingestr Asset UI simplifies the creation and configuration of these data ingestion pipelines through a user-friendly interface.

## Features

### Source and Destination Selection
- **Dropdown Interface**: Choose from available source and destination connections using intuitive dropdown menus
- **Connection Validation**: Automatic validation of selected connections to ensure compatibility
- **Visual Feedback**: Clear indicators show connection status and supported data types

### Supported Sources and Destinations
- **Wide Range of Sources**: Support for various database types, APIs, and file formats
- **Flexible Destinations**: Multiple destination options including data warehouses and databases
- **GCS Support**: Google Cloud Storage (GCS) available as a destination option
- **Dynamic Options**: Available sources and destinations update based on your configured connections

### Asset Configuration
- **Interactive Setup**: Configure ingestion parameters through form inputs rather than manual YAML editing
- **Schema Management**: Define and manage data schemas for ingested data
- **Validation Rules**: Built-in validation ensures configuration correctness before asset creation

## How to Use

### Creating an Ingestr Asset

1. **Access Asset Creation**: Navigate to the asset creation interface in the extension
2. **Select Asset Type**: Choose "Ingestr" as the asset type
3. **Choose Source**: Use the source dropdown to select your data source connection
4. **Select Destination**: Use the destination dropdown to choose where the data should be ingested
5. **Configure Parameters**: Set up additional ingestion parameters:
   - Table/collection names
   - Data transformation rules
   - Scheduling options
   - Error handling preferences
6. **Validate Configuration**: The UI will validate your selections and show any issues
7. **Create Asset**: Save the configuration to create your new ingestr asset

### Managing Existing Ingestr Assets

1. **Open Asset File**: Navigate to an existing ingestr asset file
2. **Visual Interface**: The Side Panel will display the current configuration in a visual format
3. **Edit Settings**: Modify source, destination, or other parameters using the UI controls
4. **Update Configuration**: Changes are automatically applied to the underlying asset definition

### Available Destinations

The Ingestr Asset UI supports various destination types, including:
- **Google Cloud Storage (GCS)**: Store data in GCS buckets with configurable paths and formats
- **Data Warehouses**: BigQuery, Snowflake, Redshift, and other warehouse solutions
- **Databases**: PostgreSQL, MySQL, and other database systems
- **File Systems**: Local and cloud-based file storage options

## Benefits

- **Visual Configuration**: No need to manually write complex YAML configurations
- **Error Prevention**: Dropdown selections and validation prevent configuration mistakes
- **Quick Setup**: Rapidly create data ingestion pipelines with minimal manual input
- **Connection Integration**: Seamlessly uses your existing connection configurations
- **Schema Awareness**: Automatic schema detection and validation for supported sources

## Example Use Cases

- **Database Replication**: Sync data between different database systems
- **Data Lake Ingestion**: Move data from operational systems to data lakes
- **API Data Collection**: Regularly fetch data from REST APIs and store in warehouses
- **File Processing**: Ingest and process files from various storage systems
- **Cloud Migration**: Move data between different cloud platforms

![Ingestr Asset UI](../public/vscode-extension/ingestr-asset-ui/create-ingestr-asset.gif)