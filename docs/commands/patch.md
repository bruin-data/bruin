# Patch Command

The `patch` command provides utilities for updating asset metadata and dependencies. It has two subcommands: `fill-asset-dependencies` and `fill-columns-from-db`.

## Subcommands

### fill-asset-dependencies

Updates the dependencies of assets based on their SQL queries. This command analyzes the SQL queries in assets and adds any missing upstream dependencies.

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--output [format]`  | `-o`  | Specifies the output type, possible values: `plain`, `json`. Default: `plain` |

**Example:**
```bash
# Update dependencies for a single asset
bruin patch fill-asset-dependencies path/to/asset.yml

# Update dependencies for all assets in a pipeline
bruin patch fill-asset-dependencies path/to/pipeline
```

**Example output (JSON):**
```json
{
    "status": "success",
    "skipped_assets": 0,
    "updated_assets": 2,
    "failed_assets": 0,
    "processed_assets": 2
}
```

### fill-columns-from-db

Retrieves column metadata from the database and updates the asset's column definitions. For existing assets, it only adds new columns that don't already exist.

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--output [format]`  | `-o`  | Specifies the output type, possible values: `plain`, `json`. Default: `plain` |
| `--environment`      | `-e`  | Target environment name as defined in .bruin.yml                            |

**Example:**
```bash
# Update columns for a single asset
bruin patch fill-columns-from-db path/to/asset.yml 

# Update columns for all assets in a pipeline
bruin patch fill-columns-from-db path/to/pipeline 
```

**Example output (JSON):**
```json
{
    "status": "partial",
    "skipped_assets": 1,
    "updated_assets": 2,
    "failed_assets": 1,
    "processed_assets": 4
}
```

**Status values:**
- `success`: All assets were successfully updated
- `partial`: Some assets were updated, some failed or were skipped
- `failed`: All assets failed to update
- `skipped`: No assets needed updates

**Notes:**
- Column names are compared case-insensitively to prevent duplicates
- Existing column metadata (descriptions, checks, etc.) is preserved
