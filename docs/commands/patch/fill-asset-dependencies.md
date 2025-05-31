# `bruin patch fill-asset-dependencies` Command

## Description

The `bruin patch fill-asset-dependencies` command is a utility designed to automatically populate the `depends` field in Bruin asset definition files (typically `.sql` files with accompanying `.yaml` metadata or header blocks). These assets represent dbt-like models or SQL transformations within a Bruin data pipeline.

This command analyzes the SQL content of your assets to identify upstream dependencies (e.g., references to other models or sources) that are not explicitly declared in the `depends` field. It helps maintain an accurate and up-to-date dependency graph for your data pipeline, which is crucial for correct execution order and understanding data lineage.

**This command directly modifies Bruin asset files (or their corresponding metadata) by updating the `depends` field.**

## Usage

```bash
bruin patch fill-asset-dependencies [options] <asset_path_or_pipeline_dir>
```

## Arguments

-   `<asset_path_or_pipeline_dir>`: (Required) The path to a single Bruin asset file (e.g., `models/my_model.sql`) or a directory containing multiple Bruin assets (e.g., `models/`). If a directory is provided, the command will recursively search for asset files to process, effectively operating on all assets in that part of the pipeline.

## Options

-   `--dry-run`: (Optional) If set, the command will simulate the changes without actually modifying any asset files. It will print the proposed changes to standard output. This is useful for previewing the potential updates.
-   `--verbose`: (Optional) Enables verbose logging, providing more detailed information about the process, such as which files are being scanned and which dependencies are found.
-   `--output <format>`: (Optional) Specifies the output format for the summary of changes.
    *   `plain`: (Default) Outputs a human-readable summary.
    *   `json`: Outputs the summary in JSON format, suitable for programmatic consumption.
-   `--help`: (Optional) Displays help information for the command.

## Behavior

The command performs the following actions:

1.  **Asset Discovery**: It identifies Bruin assets within the given `<asset_path_or_pipeline_dir>`. Assets are typically SQL files (e.g., `my_model.sql`) that may have a corresponding YAML file (e.g., `my_model.yaml`) or a YAML header block within the SQL file itself for metadata.
2.  **SQL Dependency Analysis**: For each discovered asset, it parses the SQL content to find references to other assets (models) or data sources. This typically involves looking for:
    *   `ref('model_name')` function calls.
    *   `source('source_name', 'table_name')` function calls.
    *   Direct table names that correspond to other models in the pipeline.
3.  **Metadata Update**: It compares the discovered dependencies against the existing `depends` list in the asset's metadata.
    *   If the `depends` field does not exist, it will be created.
    *   Missing dependencies found in the SQL are added to the `depends` list.
    *   The command typically does not remove existing entries from `depends` unless they are clearly invalid or a specific option for cleanup is provided (behavior may vary based on implementation).
4.  **Output**: The command outputs a summary of the changes made (or proposed, if `--dry-run` is used), formatted according to the `--output` flag. This includes the number of assets processed, assets updated, and any errors encountered.

## Example

### Example 1: Process a single SQL asset file

Suppose you have an asset `models/core/dim_users.sql` with the following content:

```sql
-- models/core/dim_users.sql
SELECT
    id,
    name,
    email,
    created_at
FROM {{ ref('stg_users') }}
LEFT JOIN {{ ref('stg_user_profiles') }} USING (id)
```

And its metadata (either in a YAML header or `dim_users.yaml`) is:
```yaml
name: dim_users
type: model
# 'depends' field is missing or incomplete
```

Running the command:
```bash
bruin patch fill-asset-dependencies models/core/dim_users.sql
```

This will analyze `models/core/dim_users.sql`, identify `stg_users` and `stg_user_profiles` as dependencies from the `ref()` calls. The command will then update its metadata:

**After:** (metadata for `dim_users.sql`)
```yaml
name: dim_users
type: model
depends:
  - "stg_users"
  - "stg_user_profiles"
```

### Example 2: Process all assets in a pipeline directory

```bash
bruin patch fill-asset-dependencies models/staging/ --output json
```

This will recursively find and process all Bruin SQL assets within the `models/staging/` directory. It will update the `depends` field for any asset where the declared dependencies do not match the ones found by parsing the SQL. The summary of operations will be printed in JSON format.

### Example 3: Dry run to preview changes for an entire pipeline

```bash
bruin patch fill-asset-dependencies --dry-run .
```

This will scan all assets from the current directory downwards, showing what changes would be made to their `depends` fields without actually modifying the files. This is useful for a full pipeline audit.

## Error Handling

-   If an asset's SQL file is malformed or cannot be parsed, an error message will be displayed, and the command will skip that file.
-   If metadata files are present but malformed, errors will be reported.
-   If the command encounters issues accessing files or directories (e.g., permission errors), appropriate error messages will be provided.

## Related Commands

-   `bruin validate`: For validating the entire Bruin pipeline, including dependencies.
-   `bruin run`: For executing the Bruin pipeline, respecting the dependency order.
-   `bruin graph`: For visualizing the pipeline's dependency graph.

## Best Practices

-   Run this command after making changes to your SQL models, especially when adding or removing `ref()` or `source()` calls.
-   Consider integrating this command into your pre-commit hooks or CI/CD pipeline to ensure `depends` fields are kept up-to-date automatically.
-   Use the `--dry-run` option to review changes before applying them, particularly when running on a large number of assets or an entire pipeline.
-   Ensure that your asset files (SQL and any accompanying YAML) are writable by the user or process running the command.
-   Maintain clear and consistent naming conventions for your assets, as this aids in the accuracy of dependency detection.
```
