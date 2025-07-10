package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
)

func main() {
	// Example of how to use the new BigQuery dry run metadata feature
	config := &bigquery.Config{
		ProjectID:       "your-project-id",
		CredentialsJSON: "your-credentials-json", // or use CredentialsFilePath
	}

	client, err := bigquery.NewDB(config)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Example query - replace with your actual query
	q := &query.Query{
		Query: `
			SELECT 
				name, 
				number 
			FROM 
				` + "`bigquery-public-data.usa_names.usa_1910_2013`" + `
			WHERE 
				year = 2010 
			LIMIT 100
		`,
	}

	// Get dry run metadata without actually executing the query
	metadata, err := client.GetDryRunMetadata(context.Background(), q)
	if err != nil {
		log.Fatalf("Failed to get dry run metadata: %v", err)
	}

	// Display the results
	if metadata.IsValid {
		fmt.Printf("✅ Query is valid!\n")
		fmt.Printf("📊 Data to be processed: %s\n", formatBytes(metadata.TotalBytesProcessed))
		fmt.Printf("💰 Estimated cost: $%.4f USD\n", metadata.EstimatedCostUSD)

		if metadata.EstimatedCostUSD > 1.0 {
			fmt.Printf("⚠️  Warning: This query is estimated to cost more than $1.00\n")
		}
	} else {
		fmt.Printf("❌ Query validation failed: %s\n", metadata.ValidationError)
	}
}

// formatBytes converts bytes to human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
