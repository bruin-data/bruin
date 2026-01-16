# `ai enhance` Command

The `ai enhance` command uses AI to automatically enhance your asset definitions with meaningful metadata, quality checks, descriptions, and tags. It analyzes your asset files and database schema to generate intelligent suggestions that improve data documentation and quality.

## Usage

```bash
bruin ai enhance [path to asset] [flags]
```

**Arguments:**

- **[path to asset]:** Path to a single asset file (e.g., `assets/my_asset.sql` or `assets/my_asset.asset.yml`). This argument is required.

## Flags

| Flag | Alias | Description |
|------|-------|-------------|
| `--output` | `-o` | Output format: `plain` (default) or `json` |
| `--environment` | `--env` | Target environment name as defined in `.bruin.yml` |
| `--model` | | AI model to use for suggestions |
| `--claude` | | Use Claude CLI for AI enhancement (default) |
| `--opencode` | | Use OpenCode CLI for AI enhancement |
| `--codex` | | Use Codex CLI for AI enhancement |
| `--debug` | | Show debug information during enhancement |

> [!NOTE]
> Only one provider flag (`--claude`, `--opencode`, or `--codex`) can be specified at a time.

## How It Works

The `ai enhance` command runs a 4-step process to intelligently enhance your asset:

### Step 1: Fill Columns from Database
The command first attempts to retrieve column metadata from your database and adds any missing columns to the asset definition. This ensures the AI has complete schema information to work with.

### Step 2: AI Enhancement
The selected AI provider analyzes your asset file and adds:
- **Descriptions**: Meaningful descriptions for assets and columns based on their names and context
- **Quality Checks**: Appropriate data quality checks based on column names, types, and statistics
- **Tags**: Relevant tags based on the asset's purpose and domain

If your asset is connected to a database, the command pre-fetches table statistics (row counts, null counts, distinct values, min/max, etc.) to make data-driven decisions about which quality checks to add.

### Step 3: Format
The enhanced asset file is automatically formatted to ensure consistent styling.

### Step 4: Validate
The modified asset is validated using Bruin's validation rules. If validation fails, the original file is restored (rollback) to prevent corrupted assets.

## AI Providers

The command supports three AI providers. You must have the corresponding CLI tool installed:

### Claude (Default)
Uses the [Claude CLI](https://docs.anthropic.com/en/docs/claude-cli) from Anthropic.

```bash
bruin ai enhance assets/my_asset.sql --claude
```

Default model: `claude-sonnet-4-20250514`

You can set your API key in the `.bruin.yml` configuration file under Anthropic connections, or via the `ANTHROPIC_API_KEY` environment variable.

### OpenCode
Uses the [OpenCode CLI](https://github.com/opencode-ai/opencode).

```bash
bruin ai enhance assets/my_asset.sql --opencode
```

Default model: `anthropic/claude-sonnet-4-20250514`

### Codex
Uses the [Codex CLI](https://github.com/openai/codex).

```bash
bruin ai enhance assets/my_asset.sql --codex
```

Default model: `gpt-5-codex`

## Quality Checks Added

The AI enhancement adds standard Bruin column checks based on column patterns and statistics:

| Check Type | When Applied |
|------------|--------------|
| `not_null` | Required fields, IDs, foreign keys, columns with 0 null count |
| `unique` | Primary keys, identifiers, columns where distinct count equals total rows |
| `positive` | Values that must be > 0 (amounts, prices) |
| `non_negative` | Values that must be >= 0 (counts, quantities) |
| `min` / `max` | Threshold validations based on statistics |
| `accepted_values` | Enum-like columns (status, type, category) using sample values |
| `pattern` | Regex validation (e.g., email patterns) |

The AI uses naming conventions to infer appropriate checks:
- `*_id` columns → `not_null` + `unique`
- `email` columns → `pattern` check with email regex
- `amount`, `price`, `cost` → `non_negative` or `positive`
- `status`, `state`, `type` → `accepted_values`
- `*_at`, `*_date`, `created*`, `updated*` → `not_null` for timestamps
- `percentage`, `rate`, `*_pct` → `min: 0`, `max: 100`
- `count`, `*_count`, `qty`, `quantity` → `non_negative`

## Examples

### Basic Enhancement

Enhance a single SQL asset:

```bash
bruin ai enhance assets/orders.sql
```

### Specify Environment

Use a specific environment for database connections:

```bash
bruin ai enhance assets/orders.sql --environment production
```

### Use a Specific Model

Override the default AI model:

```bash
bruin ai enhance assets/orders.sql --model claude-opus-4-20250514
```

### JSON Output

Get structured JSON output (useful for automation):

```bash
bruin ai enhance assets/orders.sql --output json
```

**Example JSON output:**
```json
{
  "status": "success",
  "asset": "orders"
}
```

### Debug Mode

Show detailed information during enhancement:

```bash
bruin ai enhance assets/orders.sql --debug
```

## Example Output

When running in plain mode, the command shows progress and a diff of changes:

```
Step 1/4: Filling columns from database...
  Columns updated from database schema.
Step 2/4: Enhancing asset with AI...
  Using environment: production
  Pre-fetching table statistics for 'orders'...
  Pre-fetched statistics for 12 columns (including sample values for 3 enum-like columns)
Step 3/4: Formatting asset...
Step 4/4: Validating asset...

✓ Enhanced 'orders'

Changes:
+ description: "Customer orders containing purchase information and order status"
+ tags:
+   - ecommerce
+   - orders
  columns:
    - name: order_id
+     description: "Unique identifier for the order"
+     checks:
+       - name: not_null
+       - name: unique
    - name: status
+     description: "Current status of the order"
+     checks:
+       - name: accepted_values
+         value:
+           - pending
+           - processing
+           - shipped
+           - delivered
+           - cancelled

+15 additions, -0 deletions
```

## Important Notes

- **Conservative approach**: The AI only adds checks it's confident about based on column names or actual data analysis
- **Preservation**: Existing content is preserved - the AI only adds new fields, it doesn't remove or modify existing ones
- **Rollback on failure**: If validation fails after enhancement, the original file is automatically restored
- **CLI requirement**: You must have the selected AI provider's CLI installed and authenticated before using this command

## Prerequisites

1. Install one of the supported AI CLI tools:
   - Claude CLI: `npm install -g @anthropic/claude-cli`
   - OpenCode CLI: See [OpenCode installation](https://github.com/opencode-ai/opencode)
   - Codex CLI: See [Codex installation](https://github.com/openai/codex)

2. Authenticate with your chosen provider

3. Optionally, configure your Anthropic API key in `.bruin.yml`:
   ```yaml
   environments:
     default:
       connections:
         anthropic:
           - name: default
             api_key: "your-api-key"
   ```

## See Also

- [Patch Command](/commands/patch) - For filling columns from database without AI
- [Format Command](/commands/format) - For formatting assets
- [Validate Command](/commands/validate) - For validating assets
- [Quality Checks](/quality/overview) - Overview of available quality checks
