package enhance

import (
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

const systemPromptTemplate = `You are a data quality expert analyzing data assets for a data pipeline tool called Bruin.
Your task is to suggest improvements to asset definitions including:
1. Meaningful descriptions for assets and columns based on their names and context
2. Appropriate data quality checks based on column names, types, and common patterns
3. Relevant tags based on the asset's purpose and domain
4. Domain classifications for the asset
5. Owner suggestions if determinable from context (email format)

YOU HAVE ACCESS TO BRUIN MCP TOOLS:
You have access to the Bruin MCP server which provides documentation and database connectivity.

DOCUMENTATION TOOLS:
- bruin_get_overview: Understand Bruin's capabilities
- bruin_get_docs_tree: See available documentation files
- bruin_get_doc_content: Read specific documentation about platforms and best practices

DATABASE TOOLS (use these to make data-driven suggestions):
- bruin_list_connections: List all available database connections in the project
- bruin_get_table_schema: Get column names and types for a table (params: connection, table)
- bruin_get_column_stats: Get statistics for a column including null_count, distinct_count, min/max values (params: connection, table, column)
- bruin_sample_column_values: Get sample distinct values from a column (params: connection, table, column, limit)

RECOMMENDED WORKFLOW:
1. Use bruin_list_connections to see available connections
2. If the asset has a materialization connection, use bruin_get_table_schema to understand the actual table structure
3. For key columns (IDs, status fields, etc.), use bruin_get_column_stats to check:
   - If null_count is 0, suggest not_null check
   - If distinct_count equals total_rows, suggest unique check
   - Min/max values can inform range checks
4. For enum-like columns (status, type, category), use bruin_sample_column_values to get actual values for accepted_values checks

These tools can help you understand:
- Available data quality checks and their proper usage
- Platform-specific features (BigQuery, Snowflake, PostgreSQL, etc.)
- Best practices for data asset definitions
- Actual data characteristics from the database

IMPORTANT RULES:
- Respond ONLY with valid JSON matching the specified schema
- Do not include any explanations or text outside the JSON
- Only suggest checks that make sense for the column type and name
- Do not suggest checks that already exist on the asset
- Be conservative - only suggest checks you're confident about
- For descriptions, be concise but informative
- Use database tools when available to validate suggestions with actual data`

const systemPromptTemplateWithoutMCP = `You are a data quality expert analyzing data assets for a data pipeline tool called Bruin.
Your task is to suggest improvements to asset definitions including:
1. Meaningful descriptions for assets and columns based on their names and context
2. Appropriate data quality checks based on column names, types, and common patterns
3. Relevant tags based on the asset's purpose and domain
4. Domain classifications for the asset
5. Owner suggestions if determinable from context (email format)

IMPORTANT RULES:
- Respond ONLY with valid JSON matching the specified schema
- Do not include any explanations or text outside the JSON
- Only suggest checks that make sense for the column type and name
- Do not suggest checks that already exist on the asset
- Be conservative - only suggest checks you're confident about
- For descriptions, be concise but informative`

// BuildEnhancePrompt constructs the prompt for Claude to analyze an asset.
func BuildEnhancePrompt(asset *pipeline.Asset, pipelineName string) string {
	// Serialize current asset state
	assetSummary := buildAssetSummary(asset)

	return fmt.Sprintf(`Analyze this data asset and suggest enhancements.

Asset Name: %s
Pipeline: %s
Asset Type: %s

Current Asset Definition:
%s

Respond with a JSON object in this exact format:
{
  "asset_description": "Clear, concise description of what this asset represents (or empty string if already has one)",
  "column_descriptions": {
    "column_name": "What this column contains and represents"
  },
  "column_checks": {
    "column_name": [
      {"name": "check_type", "value": null, "reasoning": "Brief explanation why this check is appropriate"}
    ]
  },
  "suggested_tags": ["tag1", "tag2"],
  "suggested_owner": "owner@example.com or empty string if unknown",
  "suggested_domains": ["domain1"],
  "custom_checks": []
}

Available check types and when to use them:
- not_null: Required fields, IDs, foreign keys, dates that should always exist
- unique: Primary keys, natural keys, identifiers that should be unique
- positive: Counts, quantities, prices that must be > 0
- negative: Values that must be < 0 (rare)
- non_negative: Amounts, counts, quantities that must be >= 0
- min: Use with {"value": N} for minimum threshold
- max: Use with {"value": N} for maximum threshold
- accepted_values: Use with {"value": ["val1", "val2"]} for enum-like columns
- pattern: Use with {"value": "regex"} for format validation (emails, phones, etc.)

Column naming patterns to consider:
- *_id, *Id columns: usually need unique + not_null
- email columns: pattern check with email regex
- phone columns: pattern check
- amount, price, cost, *_amt: non_negative or positive
- status, state, type: accepted_values if you can infer valid values
- *_at, *_date, created*, updated*: not_null for required timestamps
- percentage, rate, *_pct: min 0, max 100
- count, *_count, qty, quantity: non_negative

Only include fields in your response that have suggestions. Empty arrays/objects can be omitted.
Do NOT suggest checks for columns that already have that check type.`,
		asset.Name,
		pipelineName,
		asset.Type,
		assetSummary,
	)
}

// buildAssetSummary creates a readable summary of the asset for the prompt.
func buildAssetSummary(asset *pipeline.Asset) string {
	summary := struct {
		Name           string            `json:"name"`
		Type           string            `json:"type"`
		Description    string            `json:"description,omitempty"`
		Owner          string            `json:"owner,omitempty"`
		Tags           []string          `json:"tags,omitempty"`
		Domains        []string          `json:"domains,omitempty"`
		Columns        []columnSummary   `json:"columns,omitempty"`
		ExistingChecks []string          `json:"existing_custom_checks,omitempty"`
	}{
		Name:        asset.Name,
		Type:        string(asset.Type),
		Description: asset.Description,
		Owner:       asset.Owner,
		Tags:        asset.Tags,
		Domains:     asset.Domains,
	}

	// Add column summaries
	for _, col := range asset.Columns {
		colSum := columnSummary{
			Name:        col.Name,
			Type:        col.Type,
			Description: col.Description,
		}
		// List existing checks
		for _, check := range col.Checks {
			colSum.ExistingChecks = append(colSum.ExistingChecks, check.Name)
		}
		summary.Columns = append(summary.Columns, colSum)
	}

	// Add existing custom check names
	for _, check := range asset.CustomChecks {
		summary.ExistingChecks = append(summary.ExistingChecks, check.Name)
	}

	jsonBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error serializing asset: %v", err)
	}

	return string(jsonBytes)
}

type columnSummary struct {
	Name           string   `json:"name"`
	Type           string   `json:"type,omitempty"`
	Description    string   `json:"description,omitempty"`
	ExistingChecks []string `json:"existing_checks,omitempty"`
}

// GetSystemPrompt returns the system prompt for Claude.
// If useMCP is true, includes instructions about available MCP tools.
func GetSystemPrompt(useMCP bool) string {
	if useMCP {
		return systemPromptTemplate
	}
	return systemPromptTemplateWithoutMCP
}
