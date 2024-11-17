# Templates 

We simplify the creation of pipelines through the use of **templates**. Templates provide a starting point with predefined structures, configurations, and connections to help you quickly set up and standardize your pipelines.

---
## What are templates?

Templates in Bruin are blueprints for initializing new pipelines. They:
- define the structure and essential files for a pipeline.
- contain boilerplate code and configurations tailored to specific use cases.
- allow users to quickly set up pipelines without starting from scratch.

By using templates,teams can focus on their pipeline logic rather than worrying about setup and configuration.

---

## How to use templates ?

Using templates in Bruin is straightforward with the `bruin init` command. It offers both
an interactive wizard and direct command-line options.

### Running the wizard

To initialize a project using the wizard, simply run:
```bash
bruin init 
```
As shown below, this will launch a terminal-based wizard:
```plaintext
bin/bruin init
[ ] default
[ ] duckdb
[ ] firebase
[ ] gorgias
[x] notion
[ ] python
[ ] redshift
[ ] shopify-bigquery
[ ] shopify-duckdb

(press q to quit)
```
You can navigate through the available templates using the up and down arrow keys. Once you've selected your desired template, press Enter to confirm. 
The wizard will automatically create the folder and set up the project for you.
### Manual Template Selection
If you'd prefer to specify your choices directly, you can use:
```bash
bruin init [template-name] [folder-name]
```
Arguments:

- **template-name** (optional):  
  The name of the template to use. If omitted, the default template is used.

- **folder-name** (optional):  
  The name of the folder where the project will be created. If omitted, the folder name will be:  
    - `bruin-pipeline` for the default template.  
    - The template name for custom templates.

### Available Templates

To see a list of available templates, run:

```bash
bruin init --help
```
The output will display a list of available templates.

---

## Example Template: Notion-to-BigQuery Pipeline

The **Notion-to-BigQuery Pipeline** template is an example of a Bruin pipeline designed to copy data from Notion to BigQuery. This template demonstrates how to structure a simple pipeline with Bruin and provides prebuilt configurations for both Notion and BigQuery assets.

### Template Overview

This template includes:

- **Preconfigured Assets**:
  - `raw.notion`: An ingestr asset that extracts data from a Notion table and loads it into BigQuery.
  - `myschema.example`: A SQL asset that creates a table in BigQuery.

- **Project Structure**:
  A structured layout with folders and files to organize pipeline assets, configurations, and documentation.

### Folder Structure

```plaintext
notion/
├── assets/
│   ├── example.sql               # Example SQL script for BigQuery table creation
│   ├── notion.asset.yml          # Configuration for the Notion asset
├── pipeline.yml                  # Main pipeline configuration
└── README.md                     # Documentation for the pipeline
```

### How to Use This Template

1. **Initialize the Project**:  
   Use the template to create a new pipeline project:
   ```bash
   bruin init notion 
2. **Configure Connections**:


Update the `.bruin.yml` file with your Notion API key and Google Cloud credentials.
3. **Run the Pipeline**:

Use the `bruin` CLI to execute the pipeline, moving data from Notion to BigQuery based on the preconfigured assets.

---

This example template provides a starting point for building similar data ingestion pipelines with Notion and BigQuery. Modify the assets or add custom scripts to expand its functionality as needed.
