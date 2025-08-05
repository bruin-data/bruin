

# Tabs Overview
These tabs give you direct access to view and manage details of the currently open Bruin asset.

## 1. General
This tab provides a comprehensive view of your asset, allowing you to manage key information and execute actions directly from the UI.

- **Pipeline & Asset Name**: Displays the current pipeline and asset names. The asset name can be updated from the UI.
- **Pipeline Schedule & Asset Type**: Displays the current pipeline schedule and asset type as tags.
- **Description**: View or edit the asset’s description to document its purpose.
- **Environment Dropdown**: Seamlessly switch between environments (e.g., `development`, `production`).
- **Date Inputs**: 
    - Customize the date/time for runs directly within the UI. 
    - A refresh button allows synchronization with the pipeline schedule.
    - *Note*: The time is specified in `UTC`.
- **SQL Preview**: Displays a preview of the SQL query related to the asset for quick inspection.

![Bruin Side Panel](../../../public/vscode-extension/panels/side-panel/asset-details-tab-new.gif)

## 2. Columns
- Lists the columns associated with the asset.
- *Each column shows*:
  - **Name**: The name of the column.
  - **Type**: The data type of the column (e.g., string, number).
  - **Quality Checks**: Any quality checks linked to the column.
  - **Description**: A brief description of the column's purpose.
  - **Glossary Integration**:
    - Columns sourced from the Glossary are marked with a link icon.
    - Clicking the link opens the corresponding glossary for more details.
- *Edit Capability*:
  - **Edit Name, Type, and Description**: Modify column properties directly within the UI.
  - **Add or Remove Columns**: Easily insert new columns or delete existing ones.
  - **Manage Quality Checks**: Add or remove quality checks.

### Fill from DB
The Columns tab includes a "Fill from DB" feature that allows you to automatically populate column information from your database:

- **Auto-populate Columns**: Retrieve column definitions directly from the connected database
- **Background Processing**: The fill operation runs in the background rather than in the terminal for a smoother experience
- **Column Sync**: Automatically sync column names, types, and constraints from your database schema
- **Conditional Availability**: The "Fill from DB" button appears based on connection availability and asset type

![Bruin Columns Tab](../../../public/vscode-extension/panels/side-panel/manage-columns.gif)

## 3. Details

The Details tab is your central hub for configuring crucial metadata and operational aspects of your asset within the database. It allows you to manage everything from how your asset is stored to who owns it and how its data intervals are defined.

### Features
- **Owner and Tags**:
  - **Owner**: Set or edit the owner of the asset. This can be useful for identifying who is responsible for the asset.
  - **Tags**: Add or remove tags to categorize and filter assets.

- **Partitioning**:
  - Select a column or manually enter an expression to partition the table by. Supports expressions such as date(col_name) or timestamp_trunc(col2, hour).

- **Clustering**:
  - Select multiple columns to cluster the table by. Clustering organizes data storage based on the specified columns, which can improve query performance.

- **Interval Modifiers**: Define the time window or validity period for your asset's data. See [Interval Modifiers](../../../assets/interval-modifiers.md) for more details.

  - **Start**: Set the beginning of the interval using a numerical value and select a specific unit from the dropdown (e.g., -2 days).
  - **End**: Set the end of the interval using a numerical value and select a specific unit from the dropdown (e.g., 1 month).

- **Materialization**: Control how your asset is materialized in the database.
  - **None**: No materialization.
  - **Table**: Materialize the asset as a table.
  - **View**: Materialize the asset as a view.

- **Strategy** (for Tables):
  - **Create + Replace**: Drop and recreate the table completely.
  - **Delete + Insert**: Delete existing data using an incremental key and insert new records.
  - **Append**: Add new rows without modifying existing data.
  - **Merge**: Update existing rows and insert new ones using primary keys.
  - **Time Interval**: Process time-based data using an incremental key.
  - **DDL**: Use DDL to create a new table using the information provided in the embedded Bruin section.

### How to Use

1. **Set Owner and Tags**:
   - Edit the owner by clicking on the edit icon next to the owner field. Changes are saved immediately.
   - Add or remove tags by clicking on the add or remove icons next to the tags field. Changes are saved immediately.

2. **Set Partitioning**:
   - Click on the partitioning input field and select a column from the dropdown list to partition your table.

3. **Set Clustering**:
   - Click on the clustering input field and select one or more columns from the dropdown list to cluster your table.

4. **Set Interval Modifiers**:
   - Edit the interval modifiers by clicking on the edit icon next to the interval modifiers field. Changes are saved immediately.

5. **Select Materialization Type**:
   - Choose between `None`, `Table`, or `View` using the radio buttons.

6. **Configure Strategy** (if Table is selected):
   - Select a strategy from the dropdown menu. Each strategy has a description to help you understand its use case.

7. **Save Changes**:
   - Click the "Save Changes" button to apply your materialization settings for partitioning, clustering, and strategy.

### Fill Asset Dependencies and Columns from DB
The Details tab also provides functionality to automatically populate asset dependencies and columns from your database:

- **Fill Dependencies**: Automatically detect and fill asset dependencies based on database relationships
- **Database Integration**: Pull dependency information directly from your connected database systems
- **Smart Detection**: Intelligent detection of upstream and downstream dependencies
- **Bulk Operations**: Fill multiple dependencies at once for complex asset relationships

![Details Tab](../../../public/vscode-extension/panels/side-panel/details-tab.gif)

## 4. Custom Checks
The Custom Checks tab allows you to manage custom checks for your assets directly from the UI.

### How to Use

1. **Add a Custom Check**:
   - Click the "Add Check" button.
   - Enter the details for your custom check:
     - **Name**: Provide a descriptive name.
     - **Value**: Set the expected value or threshold.
     - **Description**: Add a brief description of the check.
     - **Query**: Input the SQL query for validation.

2. **Edit a Custom Check**:
   - Click the edit icon (pencil) next to the check you wish to modify.
   - Make your changes in the provided fields.
   - Save your changes by clicking the save icon (checkmark), or cancel by clicking the cancel icon (cross).

3. **Delete a Custom Check**:
   - Click the delete icon (trash can) next to the check you want to remove.
   - Confirm the deletion in the dialog that appears.

4. **View Custom Checks**:
   - All custom checks are displayed in a table, showing their name, value, description, and query.
   - SQL queries are syntax-highlighted for clarity.

![Custom Checks Tab](../../../public/vscode-extension/panels/side-panel/custom-checks-tab.gif)

## 5. Settings
The Settings tab has two main sections:

### a. Bruin CLI Management
- **Install & Update**: Easily install or update the Bruin CLI directly from the UI.
- **Quick Documentation Access**: A dedicated button redirects you to the Bruin documentation for reference.
- **Version Details & Debugging**: A chevron down arrow expands to reveal:
  - Bruin CLI version
  - Your operating system version
  - VS Code extension version
  - These details can be copied for debugging purposes.

![Bruin Settings Tab](../../../public/vscode-extension/panels/side-panel/settings-tab.png)

### b. Connection Management
- You can manage your connections, including:
  - **Add Connection**: Add new connections by entering the required credentials. If the connection name already exists in the same environment, an error will be displayed.
  - **Duplicate Connection**:  If some connections share similar credentials, it's easier to duplicate and modify them as needed. This is fully supported.
  - **Update Connection**: Update existing connections.
  - **Delete Connection**: Delete existing connections.  
  - **Test Connection**: This allows you to test your connection directly from the UI. Unsupported connections will display a message indicating they cannot be tested.

![Bruin Connections Manager](../../../public/vscode-extension/panels/side-panel/manage-connections.gif)

