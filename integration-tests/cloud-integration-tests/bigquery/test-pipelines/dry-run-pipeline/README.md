# Dry Run Test Pipeline

This pipeline is designed to test the BigQuery dry run metadata functionality.

## Structure

- **Pipeline**: `dry-run-test-pipeline`
- **Asset**: `sample_data.sql` - Contains 10 rows with 4 columns (id, name, value, category)

## Test Data

The `sample_data` asset contains:
- 10 rows of sample data
- 4 columns: id (INTEGER), name (VARCHAR), value (FLOAT), category (VARCHAR)
- Mixed categories (A, B, C) for testing aggregation queries

## Purpose

This pipeline is used by the integration test in `bigquery_test.go` to test:

1. **Valid Simple Query**: Basic SELECT with WHERE clause
2. **Valid Complex Query**: CTE with aggregations and GROUP BY
3. **Invalid Query**: Query with non-existent table for error validation

## Expected Dry Run Metadata

The tests validate the BigQuery dry-run functionality and JSON output structure by testing against non-existent tables, which is the expected behavior for integration tests (no need to create actual BigQuery tables).

### Test Scenarios:
All tests expect JSON output for invalid queries (non-existent tables):

- `"is_valid": false` - Query validation fails (table doesn't exist)
- `"total_bytes_processed": 0` - No data processed for invalid query
- `"estimated_cost_usd": 0` - No cost for invalid query  
- `"validation_error": "..."` - Error message about table not found

### Test Coverage:
1. **Simple Query**: Basic SELECT with WHERE clause on non-existent table
2. **Complex Query**: CTE with aggregations on non-existent table  
3. **Invalid Query**: Clearly malformed query referencing non-existent table

This approach validates the dry-run functionality without requiring actual BigQuery table setup, ensuring the CLI correctly handles BigQuery dry-run responses and formats the JSON output properly.

## Integration Test Workflows

### 1. Dry Run Metadata Workflow
Tests the basic dry-run functionality against non-existent tables to validate error handling and JSON structure.

### 2. Dry Run Asset Cost Estimation Workflow  
A comprehensive workflow that demonstrates real-world cost estimation:

**Steps:**
1. **Create Real Table**: Runs `bruin run` on the dry-run-pipeline to create the actual `dataset.sample_data` table in BigQuery
2. **Asset-Based Cost Estimation**: Uses `bruin query --asset --dry-run` to estimate the cost of querying the created asset  
3. **Custom Query Cost Estimation**: Tests `bruin query --asset --query --dry-run` with a custom query on the real table

**Features Demonstrated:**
- **Real Data Cost Calculation**: Uses actual BigQuery tables for realistic cost estimates
- **Asset Integration**: Shows how dry-run works with Bruin asset files (`.sql` files with metadata)
- **Custom Query on Assets**: Demonstrates running custom queries against asset-detected connections
- **Actual vs Estimated Costs**: Provides real-world cost estimation scenarios

**Expected Outputs:**
- `"is_valid": true` for valid queries on existing tables
- Realistic `total_bytes_processed` values based on actual table data
- Corresponding `estimated_cost_usd` calculations based on actual data volume
- Proper error handling for invalid custom queries

## Usage

This pipeline requires the `--dry-run` flag implementation in the `bruin query` command to function properly. 