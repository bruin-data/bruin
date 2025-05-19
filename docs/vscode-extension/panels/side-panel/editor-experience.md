# Editor Experience
These in-editor features appear automatically based on file context or environment setup, helping you take quick action without leaving the editor.

## CLI Installation Prompt
If the Bruin CLI is not installed, the extension helps you get set up with minimal friction.

* **Trigger Condition**:
  * Triggered when you click the Render button and the CLI is missing.

* **How It Works**:
  * An alert is shown in the side panel.
  * A **“Install CLI”** button allows you to install the CLI immediately.

![Install Bruin CLI](../../../public/vscode-extension/panels/side-panel/install-cli.gif)

## File Conversion
When you open a file that isn’t yet a Bruin asset but is eligible for conversion, the extension displays an alert offering a conversion option.

* **Eligible File Types**:
  * `.sql` and `.py`: Converted using the `patch-asset` command.
  * `.yml`: Converted by renaming to `*.asset.yml`.

* **How It Works**:
  * An alert appears when an eligible file is opened.
  * A **“Convert to Asset”** button is provided to initiate the conversion directly.

  ![Convert file to Bruin asset](../../../public/vscode-extension/panels/side-panel/convert-file-to-asset.gif)
