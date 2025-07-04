# Tableau Refresh Assets

Bruin supports refreshing Tableau data sources as an asset type. This allows you to trigger immediate extract refreshes for your Tableau data sources as part of your data pipeline.

## Overview

The `tableau.refresh` asset type enables you to:
* Trigger immediate refreshes of Tableau data source extracts
* Integrate Tableau refresh operations into your data pipeline workflows
* Automate data source updates after data processing completes

## Prerequisites

Before using Tableau refresh assets, you need to:

1. **Configure a Tableau connection** in your `.bruin.yml` file
2. **Obtain the data source ID** from your Tableau instance
3. **Ensure proper authentication** (Personal Access Token recommended for MFA-enabled accounts)

## Asset Structure

```yaml
name: string
type: tableau.refresh
connection: string # Your Tableau connection name
description: string # Optional description
parameters:
  datasource_id: string # The ID of the data source to refresh
```

## Connection Configuration

You can configure Tableau connections using either Personal Access Token (recommended) or username/password:

### Using Personal Access Token 

```yaml
environments:
  development:
    connections:
      tableau:
        - name: tableau-default
          host: "your-tableau-server.com"
          personal_access_token_name: "your-token-name"
          personal_access_token_secret: "your-token-secret"
          site_id: "your-site-content-url"
          api_version: "3.4"  # Optional, defaults to 3.4
```

### Using Username/Password

```yaml
environments:
  development:
    connections:
      tableau:
        - name: tableau-default
          host: "your-tableau-server.com"
          username: "your-username"
          password: "your-password"
          site_id: "your-site-content-url"
          api_version: "3.4"  # Optional, defaults to 3.4
```

> [!WARNING]
> Username/password authentication will not work if MFA is enabled on your Tableau account. Use Personal Access Token for MFA-enabled accounts.

## Examples

### Basic Data Source Refresh

```yaml
name: refresh_sales_datasource
type: tableau.refresh
description: Refresh the sales data source after data processing
parameters:
  datasource_id: "12345678-1234-1234-1234-123456789012"
```

### Refresh After Data Pipeline

```yaml
name: refresh_analytics_dashboard
type: tableau.refresh
description: Refresh analytics dashboard data source
connection: tableau-prod
parameters:
  datasource_id: "87654321-4321-4321-4321-210987654321"
```


## API Details

The Tableau refresh asset uses the Tableau REST API:
* **Endpoint**: `POST /api/{version}/sites/{site-id}/datasources/{datasource-id}/refresh`
* **Authentication**: X-Tableau-Auth header with session token
* **Request Body**: JSON payload with datasource ID
* **Response**: Success/failure status 