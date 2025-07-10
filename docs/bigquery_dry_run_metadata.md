# BigQuery Dry Run Metadata

This feature allows you to get detailed metadata about BigQuery queries without actually executing them, including cost estimation and data scanning information.

## Overview

The `GetDryRunMetadata` method performs a dry run of your BigQuery query and returns valuable metadata such as:

- **Data scanning**: How much data will be processed
- **Cost estimation**: Estimated cost in USD based on current pricing
- **Query validation**: Whether the query is syntactically and semantically correct
- **Error details**: Specific validation errors if the query is invalid

## Usage

```go
import (
    "context"
    "github.com/bruin-data/bruin/pkg/bigquery"
    "github.com/bruin-data/bruin/pkg/query"
)

// Create BigQuery client
config := &bigquery.Config{
    ProjectID:       "your-project-id",
    CredentialsJSON: "your-credentials-json",
}

client, err := bigquery.NewDB(config)
if err != nil {
    // handle error
}

// Create your query
q := &query.Query{
    Query: "SELECT name, number FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 100",
}

// Get dry run metadata
metadata, err := client.GetDryRunMetadata(context.Background(), q)
if err != nil {
    // handle error
}

// Use the metadata
if metadata.IsValid {
    fmt.Printf("Data to be scanned: %d bytes\n", metadata.TotalBytesProcessed)
    fmt.Printf("Estimated cost: $%.4f\n", metadata.EstimatedCostUSD)
} else {
    fmt.Printf("Query validation failed: %s\n", metadata.ValidationError)
}
```

## DryRunMetadata Structure

```go
type DryRunMetadata struct {
    // TotalBytesProcessed is the total amount of data that will be processed
    TotalBytesProcessed int64 `json:"total_bytes_processed"`
    
    // TotalBytesBilled is the total amount of data that will be billed
    // Note: Currently not available from BigQuery dry runs, kept for future use
    TotalBytesBilled int64 `json:"total_bytes_billed"`
    
    // TotalSlotMs is the total slot milliseconds for capacity planning
    // Note: Currently not available from BigQuery dry runs, kept for future use
    TotalSlotMs int64 `json:"total_slot_ms"`
    
    // EstimatedCostUSD is a rough estimate of the query cost in USD
    // Based on current on-demand pricing ($6.25 per TB as of 2024)
    EstimatedCostUSD float64 `json:"estimated_cost_usd"`
    
    // IsValid indicates whether the query passed validation
    IsValid bool `json:"is_valid"`
    
    // ValidationError contains any validation errors found
    ValidationError string `json:"validation_error,omitempty"`
}
```

## Cost Calculation

The cost estimation is based on BigQuery's on-demand pricing model:

- **Rate**: $6.25 per TB of data processed (as of 2024)
- **Minimum**: BigQuery has a minimum charge, but this is handled server-side
- **Caching**: Cached query results don't incur charges, but this is reflected in the dry run

## Use Cases

1. **Cost Control**: Check query costs before execution to avoid expensive queries
2. **Query Optimization**: Compare different query variants to find the most efficient one
3. **Budget Management**: Estimate costs for batch processing or scheduled queries
4. **Query Validation**: Validate SQL syntax and semantics without execution
5. **Data Scanning Insights**: Understand how much data your queries are processing

## Limitations

- Cost estimates are based on current pricing and may not reflect exact billing
- Some advanced BigQuery features may not be fully reflected in dry run results
- Network and compute costs are not included in the estimation
- The estimation doesn't account for slot-based pricing models

## Error Handling

The method returns metadata even for invalid queries. Check the `IsValid` field to determine if the query passed validation, and use `ValidationError` for specific error details.

```go
metadata, err := client.GetDryRunMetadata(ctx, query)
if err != nil {
    // Handle unexpected errors (network, auth, etc.)
    return err
}

if !metadata.IsValid {
    // Handle query validation errors
    fmt.Printf("Query failed validation: %s\n", metadata.ValidationError)
    return
}

// Query is valid, proceed with metadata analysis
fmt.Printf("This query will scan %d bytes\n", metadata.TotalBytesProcessed)
``` 