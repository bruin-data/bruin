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
You have access to the Bruin MCP server which provides documentation and context about Bruin pipelines.
Use the bruin_get_overview tool to understand Bruin's capabilities.
Use the bruin_get_docs_tree tool to see available documentation.
Use the bruin_get_doc_content tool to read specific documentation about platforms, data sources, and best practices.

These tools can help you understand:
- Available data quality checks and their proper usage
- Platform-specific features (BigQuery, Snowflake, PostgreSQL, etc.)
- Best practices for data asset definitions
- Ingestion source documentation for understanding data structures

IMPORTANT RULES:
- Respond ONLY with valid JSON matching the specified schema
- Do not include any explanations or text outside the JSON
- Only suggest checks that make sense for the column type and name
- Do not suggest checks that already exist on the asset
- Be conservative - only suggest checks you're confident about
- For descriptions, be concise but informative`

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
