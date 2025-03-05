# Features  

The Bruin VSCode extension offers various features designed to simplify your daily workflow.

## Side Panel Tabs
The side panel organizes everything you need into four easy-to-use tabs:  

#### 1. **Asset Details**  
Get a full snapshot of your asset in one place:  
- **Pipeline & Asset Name**: See which pipeline and asset you’re working on.  You can also update the asset name directly from the UI.
- **Description**: Understand the asset’s purpose at a glance. If there is no description, you can create one or update an exsiting one directly from the UI.  
- **Environment Dropdown**: Switch between environments (like *development* or *production*) seamlessly.  

**Validate Button**  
- **Check for Errors**: Validate a single asset, the current pipeline, or your entire project.  
- **Instant Feedback**: A green check means success. Errors or warnings appear with clear messages to fix.  

**Run Button**  
- **Run Options**:  
  - **Run**: This is the default behavior and it runs the asset you’re viewing.  
  - **Run With Downstream**: Trigger `bruin run --downstream` to include dependent assets.  
  - **Whole Pipeline**: Run the entire pipeline.  
  - **Continue From Last Failure**: Re-run only failed parts of your pipeline.  


#### 2. **Columns**  
Manage your data columns effortlessly:  
- **Add/Edit Columns**: Define or update columns directly in the UI.  
- **Quality Checks**: Set rules (like data type validation) to ensure column accuracy.  


#### 3. **Custom Checks**  
Tailor validations to your needs:  
- **Add Checks**: Create rules like “values must be unique” or “dates must be in the past.”  
- **Track Warnings vs. Errors**: See which checks block execution and which are just alerts.  

#### 4. **Settings**  
Customize Bruin to fit your workflow:  
- **Connections**: Add or edit database links (BigQuery, Snowflake, etc.).  
- **CLI Management**: Install or update the Bruin CLI without leaving VS Code.  
- **Preferences**: Adjust auto-folding, theme sync, or telemetry settings.  
