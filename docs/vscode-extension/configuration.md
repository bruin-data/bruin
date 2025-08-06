## Configuration

You can configure the Bruin VS Code extension to suit your preferences, adjusting settings for folding behavior and the default path separator.

### Folding Behavior

Choose whether Bruin sections are folded or expanded by default when opening an asset. Adjust this in the extension settings under `Bruin > Default Folding State`. The options are:
  - **Folded**: All Bruin sections will be collapsed by default.
  - **Expanded**: All Bruin sections will be open by default.

This feature helps you focus on relevant sections, keeping the rest of the code minimized.

### Path Separator

Set your preferred path separator in the extension settings under `Bruin > Path Separator`. While the extension detects your operating system's default (`\` for Windows and `/` for Unix-based systems), you can override this if you have specific path formatting needs.

### Additional Settings

#### Checkbox Settings
The extension provides checkbox settings to control default enabled/disabled states for various features:

- **Apply Interval Modifiers**: Controls the default state of the `--apply-interval-modifiers` flag
- **Full Refresh Confirmation**: Enable/disable confirmation prompts before running with `full-refresh`
- **Auto-save Materialization**: Control whether materialization changes are auto-saved

#### Exclude Tags
- **Default Exclude Tag**: Set a default tag input to exclude assets with specific tags from being validated
- This helps filter out assets during validation processes based on their tags

### Accessing Configuration Settings

To access and modify the Bruin extension settings:

1. Open VS Code.
2. Click the **Settings** gear icon in the lower-left corner.
3. In the search bar, type "Bruin" to filter the Bruin extension settings.
4. Adjust the settings for:
   - **Default Folding State** and **Path Separator**
   - **Checkbox Settings** for feature defaults
   - **Exclude Tags** for validation filtering


![Extension Config](../public/vscode-extension/extension-config.png)
