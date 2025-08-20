# Fluxx
[Fluxx](https://www.fluxx.io/) is a cloud-based grants management platform designed to streamline and automate the entire grantmaking process for foundations, corporations, governments, and other funding organizations.

Bruin supports Fluxx as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Fluxx into your data warehouse.

In order to set up Fluxx connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `instance`, `client_id`, and `client_secret`. For details on how to obtain these credentials, please refer to your Fluxx administrator.

Follow the steps below to correctly set up Fluxx as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Fluxx, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  fluxx:
    - name: "my-fluxx"
      instance: "mycompany.preprod"
      client_id: "your_client_id"
      client_secret: "your_client_secret"
```

- `instance`: Your Fluxx instance subdomain (e.g., `mycompany.preprod` for `https://mycompany.preprod.fluxxlabs.com`)
- `client_id`: OAuth 2.0 client ID for authentication
- `client_secret`: OAuth 2.0 client secret for authentication

### Step 2: Create an asset file for data ingestion

To ingest data from Fluxx, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., fluxx_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.fluxx
type: ingestr
connection: postgres

parameters:
  source_connection: my-fluxx
  source_table: 'grant_request'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Fluxx connection defined in .bruin.yml.
- `source_table`: The name of the data table in Fluxx you want to ingest. For example, "grant_request" would ingest data related to grant requests. You can find the available source tables below.

## Available Tables

Fluxx source currently supports the following 50 tables:

### Core Resources
- `claim_expense_datum`: Individual data entries within claim expense forms with budget category details
- `claim_expense_row`: Specific line items or rows within claim expense forms
- `claim_expense`: Claim expense forms and templates for financial tracking
- `claim`: Grant claims and payment requests
- `concept_initiative`: Concept initiatives linking programs, initiatives, and sub-programs/sub-initiatives
- `dashboard_theme`: Dashboard theme configurations for UI customization
- `etl_claim_expense_datum`: ETL data for claim expense items with comprehensive budget tracking details
- `etl_grantee_budget_tracker_actual`: ETL data for actual grantee budget tracker amounts and expenses
- `etl_grantee_budget_tracker_period_datum`: ETL data for grantee budget tracker period information with detailed financial tracking
- `etl_relationship`: ETL data for entity relationships tracking connections between users, organizations, requests, and other entities
- `etl_request_budget`: ETL budget data for request funding sources with comprehensive financial details
- `etl_request_transaction_budget`: ETL budget data for request transaction funding sources including payment tracking
- `exempt_organization`: Tax-exempt organization data including EIN, classification, and financial information
- `geo_city`: City geographic data with coordinates and postal codes
- `geo_county`: County geographic data with FIPS codes
- `geo_place`: Geographic places with ancestry and location data
- `geo_region`: Geographic regions
- `geo_state`: State geographic data with abbreviations and FIPS codes
- `grant_request`: Grant applications and requests (300+ fields)
- `grantee_budget_category`: Budget category definitions used by grantees for expense tracking
- `grantee_budget_tracker_period_datum_actual`: Actual expenses and amounts recorded for budget tracking periods
- `grantee_budget_tracker_period_datum`: Budget data entries for specific tracking periods
- `grantee_budget_tracker_period`: Time periods for budget tracking with start and end dates
- `grantee_budget_tracker_row`: Individual budget line items and categories within budget trackers
- `grantee_budget_tracker`: Budget tracking documents for grantee financial management
- `integration_log`: Integration and system logs for tracking data processing and errors
- `mac_model_type_dyn_financial_audit`: Dynamic financial audit models with audit tracking, compliance status, and financial variance analysis
- `mac_model_type_dyn_mel`: Dynamic Monitoring, Evaluation & Learning (MEL) models with performance indicators, baseline tracking, and evaluation metrics
- `mac_model_type_dyn_tool`: Dynamic tool management models for tracking deployment status, usage metrics, and tool effectiveness
- `machine_category`: Machine category definitions for workflow state management
- `model_attribute_value`: Model attribute values with hierarchical data and dependencies
- `model_document_sub_type`: Document sub-type definitions and categories
- `model_document_type`: Document type configurations including DocuSign integration and permissions
- `model_document`: Document metadata including file information, storage details, and document relationships
- `model_theme`: Model themes for categorization and program hierarchy organization
- `organization`: Organizations (grantees, fiscal sponsors, etc.)
- `population_estimate_year`: Yearly population estimates with income and demographic breakdowns
- `population_estimate`: Population estimates by geographic area with demographic data
- `program`: Funding programs and initiatives
- `request_report`: Reports submitted for grants
- `request_transaction_funding_source`: Funding source details for specific request transactions
- `request_transaction`: Financial transactions and payments
- `request_user`: Relationships between requests and users with roles and descriptions
- `salesforce_authentication`: Salesforce authentication configurations with OAuth tokens, connection management, and API usage tracking
- `sub_initiative`: Sub-initiatives for detailed planning
- `sub_program`: Sub-programs under main programs
- `ui_version`: User interface version information and system configuration
- `user_organization`: Relationships between users and organizations with roles, departments, and contact details
- `user`: User accounts and profiles

### Field Selection

Each resource contains numerous fields. You can:
1. **Ingest all fields**: Use the resource name directly (e.g., `grant_request`)
2. **Select specific fields**: Use colon syntax (e.g., `grant_request:id,name,amount_requested`)

The field selection feature is particularly useful for large resources like `grant_request` which has over 300 fields.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/fluxx_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Fluxx table into your Postgres database.

## Authentication

Fluxx uses OAuth 2.0 with client credentials flow. To obtain credentials:

1. Contact your Fluxx administrator to create an API client
2. You'll receive a `client_id` and `client_secret`
3. Note your Fluxx instance subdomain (the part before `.fluxxlabs.com`)