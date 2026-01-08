package enhance

import (
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

const systemPromptTemplate = `You are a data quality expert enhancing data assets for a data pipeline tool called Bruin.
Your task is to DIRECTLY MODIFY asset YAML files to add improvements including:
1. Meaningful descriptions for assets and columns based on their names and context
2. Appropriate data quality checks based on column names, types, and common patterns
3. Relevant tags based on the asset's purpose and domain
4. Domain classifications for the asset

YOU MUST USE THE FILE TOOLS TO DIRECTLY EDIT THE ASSET FILE.

AVAILABLE TOOLS:

FILE TOOLS (use these to read, modify, and validate assets):
- bruin_read_file: Read the contents of an asset file
- bruin_write_file: Write the modified content back to the file
- bruin_format: Format the asset file after writing (ALWAYS run this after writing)
- bruin_validate: Validate the asset file (ALWAYS run this after formatting)

DATABASE TOOLS (use these to make data-driven decisions):
- bruin_list_connections: List all available database connections in the project
- bruin_get_table_schema: Get column names and types for a table (params: connection, table)
- bruin_get_column_stats: Get statistics for a column including null_count, distinct_count, min/max values (params: connection, table, column)
- bruin_sample_column_values: Get sample distinct values from a column (params: connection, table, column, limit)

REQUIRED WORKFLOW:
1. First, use bruin_read_file to read the current asset file
2. Optionally, use database tools to analyze the actual data:
   - Use bruin_list_connections to see available connections
   - Use bruin_get_column_stats to check null counts and uniqueness
   - Use bruin_sample_column_values for enum-like columns
3. Modify the YAML content to add enhancements:
   - Add description field if missing
   - Add column descriptions where missing
   - Add appropriate column checks based on data analysis
   - Add relevant tags
4. Use bruin_write_file to save the modified content
5. Use bruin_format to format the file properly
6. Use bruin_validate to check for errors
7. If validation fails, read the file again, fix the issues, and repeat steps 4-6

YAML STRUCTURE FOR BRUIN ASSETS:
` + "```yaml" + `
name: asset_name
type: <type>
description: "Asset description here"

tags:
  - tag1
  - tag2

columns:
  - name: column_name
    type: string
    description: "Column description"
    checks:
      - name: not_null
      - name: unique
      - name: accepted_values
        value:
          - value1
          - value2
` + "```" + `

AVAILABLE CHECK TYPES:
- not_null: For required fields, IDs, foreign keys
- unique: For primary keys, identifiers
- positive: For values that must be > 0
- non_negative: For values that must be >= 0
- min: Use with value field for minimum threshold
- max: Use with value field for maximum threshold
- accepted_values: Use with value array for enum-like columns
- pattern: Use with value field for regex validation

IMPORTANT RULES:
- ALWAYS read the file first before making changes
- Do NOT add checks that already exist
- Do NOT modify columns that already have descriptions
- Do NOT create custom checks - only use the standard column checks listed above
- Be conservative - only add checks you're confident about
- Preserve existing content - only ADD, don't remove existing fields

MANDATORY FINAL STEPS (YOU MUST ALWAYS DO THESE):
After writing ANY changes to the file, you MUST ALWAYS run these two commands in order:
1. bruin_format - Format the file (REQUIRED, never skip)
2. bruin_validate - Validate the file (REQUIRED, never skip)

If validation fails, fix the issues and repeat the write -> format -> validate cycle.
Even if you made no changes, still run bruin_format and bruin_validate to ensure file integrity.
DO NOT finish without running both bruin_format and bruin_validate.`

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

// BuildEnhancePrompt constructs the prompt for Claude to enhance an asset.
// When useMCP is true, Claude will directly edit files using MCP tools.
func BuildEnhancePrompt(asset *pipeline.Asset, pipelineName string) string {
	// Serialize current asset state for non-MCP mode
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

// BuildEnhancePromptWithFilePath constructs the prompt for Claude when using MCP tools.
// Claude will directly edit the file using the file tools.
func BuildEnhancePromptWithFilePath(assetPath, assetName, pipelineName string) string {
	return fmt.Sprintf(`Enhance the Bruin data asset file.

Asset File Path: %s
Asset Name: %s
Pipeline: %s

YOUR TASK:
1. Read the asset file using bruin_read_file
2. Analyze the asset and optionally query the database for data insights
3. Add meaningful descriptions, quality checks, and tags
4. Write the enhanced file using bruin_write_file
5. ALWAYS run bruin_format (MANDATORY - never skip)
6. ALWAYS run bruin_validate (MANDATORY - never skip)
7. If validation fails, fix issues and repeat steps 4-6

Column naming patterns to consider when adding checks:
- *_id, *Id columns: usually need not_null + unique
- email columns: pattern check with email regex
- amount, price, cost: non_negative or positive
- status, state, type: accepted_values (use bruin_sample_column_values to get actual values)
- *_at, *_date, created*, updated*: not_null for required timestamps
- percentage, rate, *_pct: min 0, max 100
- count, *_count, qty, quantity: non_negative

Be conservative - only add checks you're confident about based on column names or actual data analysis.
Do NOT remove any existing fields or checks, only ADD new ones.

IMPORTANT: You MUST run bruin_format and bruin_validate at the end, even if you made no changes.

Start by reading the file.`,
		assetPath,
		assetName,
		pipelineName,
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
