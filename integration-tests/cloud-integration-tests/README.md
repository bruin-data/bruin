# Cloud Integration Tests

This directory contains integration tests for cloud data platforms supported by Bruin. These tests verify that Bruin works correctly with real cloud databases and data warehouses.

## Overview

The cloud integration tests are designed to test Bruin's functionality against actual cloud platforms, including:

- **BigQuery**
- **Snowflake**
- **PostgreSQL** 
- **Amazon Redshift**
- **Amazon Athena**
- **Databricks**

## Prerequisites

1. **Build Bruin**: Ensure the Bruin binary is built before running tests
   ```bash
   make build
   ```

2. **Cloud Configuration**: Create a `.bruin.cloud.yml` file in this directory with your cloud platform credentials (see [Configuration](#configuration) below)

3. **Access**: You need valid credentials and access to the cloud platforms you want to test

## Configuration

### Creating `.bruin.cloud.yml`

Create a `.bruin.cloud.yml` file in the `integration-tests/cloud-integration-tests/` directory. This file should follow the standard Bruin configuration format with connection definitions for the platforms you want to test. For details on the YAML format and structure, see the [Credentials Documentation](../../docs/getting-started/credentials.md). 

### Required Connection Names

**These names must match exactly** for the tests to run:

| Platform | Connection Name | Connection Type in Config |
|----------|----------------|---------------------------|
| BigQuery | `gcp-default` | `google_cloud_platform` |
| Snowflake | `snowflake-default` | `snowflake` |
| PostgreSQL | `postgres-default` | `postgres` |
| Redshift | `redshift-default` | `redshift` |
| Athena | `athena-default` | `athena` |
| Databricks | `databricks-default` | `databricks` |

### Configuration File Structure

Here's an example `.bruin.cloud.yml` structure:

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          project_id: "project-id"
          location: 'your-gcp-region'
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```

### Platform-Specific Notes

For detailed connection setup instructions and all available configuration options, refer to the platform-specific documentation:

- **BigQuery**: [Connection Documentation](../../docs/platforms/bigquery.md#connection)
- **Snowflake**: [Connection Documentation](../../docs/platforms/snowflake.md#connection)
- **PostgreSQL**: [Connection Documentation](../../docs/platforms/postgres.md#connection)
- **Redshift**: [Connection Documentation](../../docs/platforms/redshift.md#connection)
- **Athena**: [Connection Documentation](../../docs/platforms/athena.md#connection)
- **Databricks**: [Connection Documentation](../../docs/platforms/databricks.md#connection)

### Security Note

⚠️ **Important**: The `.bruin.cloud.yml` file contains sensitive credentials. **Never commit this file to version control**. It should be listed in `.gitignore` or similar.

## Running Tests

### Run All Cloud Integration Tests

From the project root:

```bash
make integration-test-cloud
```

This command will:
1. Build the Bruin binary
2. Navigate to the cloud-integration-tests directory
3. Run all available platform tests. Only platforms with configured connections will run. If no connection is configured the test for that platform will be skipped. 

### Run Tests Directly

From the `integration-tests/cloud-integration-tests/` directory:

```bash
# Run all cloud integration tests
go test -count=1 -v .

# Run tests for a specific platform (e.g., BigQuery)
cd bigquery
go test -count=1 -v .
```

## Test Structure

Each platform has its own subdirectory containing:
- Platform-specific test files (e.g., `bigquery_test.go`)
- Test pipelines in `test-pipelines/` subdirectories
- Expected outputs and test data

The main test orchestrator (`cloud-integration_test.go`) automatically:
1. Detects available platforms from the configuration
2. Runs tests for each configured platform in parallel
3. Skips platforms without connections

## See Also

- [Main Integration Tests README](../README.md) - For general integration test information
- [Bruin Documentation](../../docs/) - For detailed platform connection documentation
