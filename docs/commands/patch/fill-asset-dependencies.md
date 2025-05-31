# `bruin patch fill-asset-dependencies` Command

## Description

The `bruin patch fill-asset-dependencies` command is a utility designed to automatically populate the `dependencies` field in Bruin asset metadata files (e.g., `image.yaml`, `video.yaml`, `material.yaml`). This command is crucial for maintaining accurate and up-to-date dependency tracking within the Bruin asset pipeline. **This command directly modifies Bruin asset files by updating their `dependencies` field.**

## Usage

```bash
bruin patch fill-asset-dependencies [options] <asset_path>
```

## Arguments

-   `<asset_path>`: (Required) The path to a Bruin asset file or a directory containing Bruin asset files. If a directory is provided, the command will recursively search for asset files to process.

## Options

-   `--dry-run`: (Optional) If set, the command will simulate the changes without actually modifying any Bruin asset files. This is useful for previewing the potential updates.
-   `--verbose`: (Optional) Enables verbose logging, providing more detailed information about the process.
-   `--help`: (Optional) Displays help information for the command.

## Behavior

The command performs the following actions:

1.  **Asset Discovery**: It identifies Bruin asset files (typically `.yaml` files representing assets like images, materials, models, etc.) within the given `<asset_path>`.
2.  **Dependency Analysis**: For each discovered Bruin asset, it analyzes the asset's content and its relationships with other files to determine its dependencies. This can involve:
    *   Parsing shader files (e.g., `.glsl`, `.hlsl`) for `#include` directives or texture lookups.
    *   Checking material files for texture asset references.
    *   Inspecting model files for linked skeletons, animations, or material assets.
    *   Analyzing scene files for references to other assets.
3.  **Metadata Update**: It updates the `dependencies` field in the Bruin asset's metadata file with the list of identified dependencies.
    *   If the `dependencies` field does not exist, it will be created.
    *   Existing dependencies that are no longer valid (e.g., a texture is no longer referenced by a material) will be removed.
    *   New dependencies will be added.
4.  **Output**: The command will output a summary of the changes made, including the number of Bruin asset files processed and any errors encountered.

## Example

### Example 1: Process a single Bruin asset file

Suppose you have a material asset `assets/materials/character_skin.material.yaml` that uses a texture `assets/textures/character_albedo.image.yaml`.

Running the command:
```bash
bruin patch fill-asset-dependencies assets/materials/character_skin.material.yaml
```

This will analyze `assets/materials/character_skin.material.yaml`. If the dependency on `assets/textures/character_albedo.image.yaml` is found (e.g., by parsing the material file content), the command will ensure that the `dependencies` field in `character_skin.material.yaml` is updated to include a reference to `character_albedo.image.yaml`.

**Before:** `assets/materials/character_skin.material.yaml`
```yaml
type: material
shader: "shaders/standard_lit.glsl"
# dependencies field might be missing or outdated
```

**After:** `assets/materials/character_skin.material.yaml`
```yaml
type: material
shader: "shaders/standard_lit.glsl"
dependencies:
  - "assets/textures/character_albedo.image.yaml"
  # Other dependencies might also be listed here
```

### Example 2: Process all Bruin assets in a directory

```bash
bruin patch fill-asset-dependencies assets/environments/forest/
```

This will recursively find and process all Bruin asset metadata files within the `assets/environments/forest/` directory, updating their `dependencies` fields as needed.

### Example 3: Dry run to preview changes for multiple assets

```bash
bruin patch fill-asset-dependencies --dry-run assets/props/ assets/vehicles/
```

This will show what changes would be made to asset dependencies in both the `assets/props/` and `assets/vehicles/` directories without actually modifying the files. This is useful for verifying the detected dependencies across a larger set of assets.


## Error Handling

-   If a Bruin asset file is malformed or cannot be parsed, an error message will be displayed, and the command will skip that file.
-   If the command encounters any issues accessing files or directories (e.g., permission errors), appropriate error messages will be provided.

## Related Commands

-   `bruin validate-assets`: (TODO: Link to this command's documentation once created) - For validating the integrity and correctness of asset files.
-   `bruin list-unused-assets`: (TODO: Link to this command's documentation once created) - For identifying assets that are no longer referenced by any other assets.

## Best Practices

-   Run this command regularly, especially after making changes that affect asset relationships (e.g., assigning new textures to a material, modifying shader includes, adding models to a scene).
-   Integrate this command into your automated asset processing workflows (e.g., as part of a pre-commit hook or a CI/CD pipeline).
-   Use the `--dry-run` option to review changes before applying them, particularly when working with a large number of assets or making significant structural changes to your project.
-   Ensure that your Bruin asset metadata files are writable by the user or process running the command.
-   Version control your asset files. This allows you to track changes made by this command and revert them if necessary.
```
