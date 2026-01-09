package enhance

import (
	"fmt"
)

const systemPromptTemplate = `You are a data quality expert enhancing data assets for a data pipeline tool called Bruin.
Your task is to DIRECTLY MODIFY asset YAML files to add improvements including:
1. Meaningful descriptions for assets and columns based on their names and context
2. Appropriate data quality checks based on column names, types, and the provided statistics
3. Relevant tags based on the asset's purpose and domain
4. Domain classifications for the asset

YOU MUST DIRECTLY EDIT THE ASSET FILE.

REQUIRED WORKFLOW:
1. First, read the current asset file
2. Use the PRE-FETCHED TABLE STATISTICS provided in the prompt to make data-driven decisions
3. Modify the YAML content to add enhancements:
   - Add description field if missing
   - Add column descriptions where missing
   - Add appropriate column checks based on the provided statistics
   - Add relevant tags
4. Edit the file to save the modified content

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
- Use the PRE-FETCHED statistics provided in the prompt
- Do NOT add checks that already exist
- Do NOT modify columns that already have descriptions
- Do NOT create custom checks - only use the standard column checks listed above
- Be conservative - only add checks you're confident about
- Preserve existing content - only ADD, don't remove existing fields`

// systemPromptTemplateWithStats is used when table statistics are pre-fetched.
// This reduces tool calls since Claude doesn't need to query the database.
const systemPromptTemplateWithStats = `You are a data quality expert enhancing data assets for a data pipeline tool called Bruin.
Your task is to DIRECTLY MODIFY asset YAML files to add improvements including:
1. Meaningful descriptions for assets and columns based on their names and context
2. Appropriate data quality checks based on column names, types, and the provided statistics
3. Relevant tags based on the asset's purpose and domain
4. Domain classifications for the asset

YOU MUST DIRECTLY EDIT THE ASSET FILE.

REQUIRED WORKFLOW:
1. First, read the current asset file
2. Use the PRE-FETCHED TABLE STATISTICS provided in the prompt (includes sample values for enum-like columns)
3. Modify the YAML content to add enhancements:
   - Add description field if missing
   - Add column descriptions where missing
   - Add appropriate column checks based on the provided statistics
   - Use the provided sample_values for accepted_values checks on enum-like columns
   - Add relevant tags
4. Edit the file to save the modified content

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
- accepted_values: Use with value array for enum-like columns (use the sample_values from the statistics)
- pattern: Use with value field for regex validation

IMPORTANT RULES:
- ALWAYS read the file first before making changes
- Use the PRE-FETCHED statistics provided in the prompt
- For accepted_values checks, use the sample_values provided in the statistics
- Do NOT add checks that already exist
- Do NOT modify columns that already have descriptions
- Do NOT create custom checks - only use the standard column checks listed above
- Be conservative - only add checks you're confident about
- Preserve existing content - only ADD, don't remove existing fields`

// BuildEnhancePrompt constructs the prompt for Claude.
// Claude will directly edit the file using its native tools.
// If tableSummaryJSON is provided, it will be included in the prompt.
func BuildEnhancePrompt(assetPath, assetName, pipelineName, tableSummaryJSON string) string {
	if tableSummaryJSON != "" {
		// Stats are pre-fetched, include them in the prompt
		return fmt.Sprintf(`Enhance the Bruin data asset file.

Asset File Path: %s
Asset Name: %s
Pipeline: %s

PRE-FETCHED TABLE STATISTICS (includes sample values for enum-like columns):
%s

YOUR TASK:
1. Read the asset file
2. Use the PRE-FETCHED TABLE STATISTICS above to make data-driven decisions
3. Add meaningful descriptions, quality checks, and tags based on the statistics
4. Edit the file with your enhancements

Use statistics to determine checks:
- null_count = 0 → add not_null check
- distinct_count = total_rows → add unique check
- For enum-like columns, use the sample_values from the statistics for accepted_values check

Column naming patterns to consider when adding checks:
- *_id, *Id columns: usually need not_null + unique
- email columns: pattern check with email regex
- amount, price, cost: non_negative or positive
- status, state, type: accepted_values (use sample_values from statistics)
- *_at, *_date, created*, updated*: not_null for required timestamps
- percentage, rate, *_pct: min 0, max 100
- count, *_count, qty, quantity: non_negative

Be conservative - only add checks you're confident about based on column names or actual data analysis.
Do NOT remove any existing fields or checks, only ADD new ones.

Start by reading the file.`,
			assetPath,
			assetName,
			pipelineName,
			tableSummaryJSON,
		)
	}

	// No pre-fetched stats available
	return fmt.Sprintf(`Enhance the Bruin data asset file.

Asset File Path: %s
Asset Name: %s
Pipeline: %s

YOUR TASK:
1. Read the asset file
2. Add meaningful descriptions, quality checks, and tags based on column names and types
3. Edit the file with your enhancements

Column naming patterns to consider when adding checks:
- *_id, *Id columns: usually need not_null + unique
- email columns: pattern check with email regex
- amount, price, cost: non_negative or positive
- status, state, type: consider accepted_values if values are obvious
- *_at, *_date, created*, updated*: not_null for required timestamps
- percentage, rate, *_pct: min 0, max 100
- count, *_count, qty, quantity: non_negative

Be conservative - only add checks you're confident about based on column names.
Do NOT remove any existing fields or checks, only ADD new ones.

Start by reading the file.`,
		assetPath,
		assetName,
		pipelineName,
	)
}

// GetSystemPrompt returns the system prompt for Claude.
// If hasPreFetchedStats is true, uses the prompt that references pre-fetched statistics.
func GetSystemPrompt(hasPreFetchedStats bool) string {
	if hasPreFetchedStats {
		return systemPromptTemplateWithStats
	}
	return systemPromptTemplate
}
