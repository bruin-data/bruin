## Getting Started

Once you have installed the Bruin VSCode extension and configured your settings, you can begin using its features to enhance your workflow. Follow these steps to get started:

### 1. Open a Supported File

The Bruin extension works with SQL and Python files. To begin, open a file with one of these extensions in VSCode. You can create a new file or open an existing one.

### 2. Create a Bruin Section

To take advantage of Bruin features, you need to create a Bruin section in your code. This can be done either manually or by using the predefined snippets provided by the extension.

- **Using Snippets**: The Bruin extension offers convenient snippets to help you quickly create Bruin sections:
  - **For SQL Files**: Type `!fullsqlasset` and select the snippet to automatically insert a Bruin section. Alternatively, you can create it manually by enclosing your code within the following delimiters:
    ```sql
    /* @bruin
      Your Bruin-specific SQL commands or configurations go here.
    @bruin */
    ```

  - **For Python Files**: Type `!fullpythonasset` and select the snippet to automatically insert a Bruin section. You can also create it manually using these delimiters:
    ```python
    """
    @bruin
    Your Bruin-specific Python logic or configurations go here.
    @bruin
    """
    ```

### 3. Utilize Syntax Highlighting and Autocompletion

When working within a Bruin section, you will automatically benefit from syntax highlighting, which enhances the readability of your code.

In addition, autocompletion is available for both `pipeline.yaml` and `.bruin.yml` files. This feature allows you to quickly access relevant properties and values as you type, utilizing a JSON schema for validation. This ensures that your configurations are both accurate and complete.

### 4. Execute Bruin CLI Commands

You can execute Bruin CLI commands directly from the extension. To do this:

- Use the command palette (`Ctrl + Shift + P`) and type the command you want to run, such as `Bruin: Render SQL`.
- Alternatively, you can use the buttons provided in the UI to execute commands without needing to open a terminal.


### 5. Explore the Bruin Panels

The Bruin extension features two main panels to enhance your workflow:

- **Side Panel**: This panel displays all the details of the current asset and includes three tabs for easy navigation. You can view asset details, columns, and settings within this panel.

- **Lineage Panel**: Located at the bottom of the VSCode interface, near the terminal tab, this panel shows the current asset lineage. It provides a visual representation of how the asset is connected to other components, helping you understand the flow of data.

