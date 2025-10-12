# Mailchimp
[Mailchimp](https://mailchimp.com/) is an all-in-one marketing platform that helps businesses manage and talk to their clients, customers, and other interested parties through email marketing campaigns, automated messages, and targeted ads.

Bruin supports Mailchimp as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Mailchimp into your data warehouse.

In order to set up Mailchimp connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key` and `server`. For details on how to obtain these credentials, please refer [here](https://mailchimp.com/developer/marketing/guides/quick-start/).

Follow the steps below to correctly set up Mailchimp as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Mailchimp, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      mailchimp:
        - name: "connection_name"
          api_key: "your_api_key"
          server: "us10"
```

- `api_key`: The API key used for authentication with the Mailchimp API.
- `server`: The server prefix for your Mailchimp account (e.g., `us10`, `us19`).

### Step 2: Create an asset file for data ingestion

To ingest data from Mailchimp, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., mailchimp_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.mailchimp
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the mailchimp connection defined in .bruin.yml.
- `source_table`: The name of the data table in mailchimp that you want to ingest. For example, `campaigns` is the table of mailchimp that you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| account | - | - | replace | Retrieves account information including company details, account tier, and contact information. |
| account_exports | - | - | replace | Retrieves account export information. |
| audiences | id | date_created | merge | Retrieves audience (list) information including subscriber counts and list settings. |
| authorized_apps | id | - | replace | Retrieves third-party applications authorized to access your account. |
| automations | id | create_time | merge | Retrieves automated email workflows and their configurations. |
| batches | - | - | replace | Retrieves batch operation status and results. |
| campaign_folders | id | - | replace | Retrieves folders used to organize campaigns. |
| campaigns | id | create_time | merge | Retrieves email campaigns including their content, settings, and metadata. |
| chimp_chatter | - | - | replace | Retrieves recent activity feed from your Mailchimp account. |
| connected_sites | id | updated_at | merge | Retrieves websites connected to your Mailchimp account. |
| conversations | id | last_message.timestamp | merge | Retrieves conversation threads from connected channels. |
| ecommerce_stores | id | updated_at | merge | Retrieves e-commerce store information including products and orders. |
| facebook_ads | id | updated_at | merge | Retrieves Facebook ad campaigns managed through Mailchimp. |
| landing_pages | id | updated_at | merge | Retrieves landing pages created in Mailchimp. |
| lists_activity | - | - | replace | Retrieves recent activity for list members. Includes `audiences_id` reference. |
| lists_clients | - | - | replace | Retrieves email clients used by list members. Includes `audiences_id` reference. |
| lists_growth_history | - | - | replace | Retrieves historical growth data for the list. Includes `audiences_id` reference. |
| lists_interest_categories | - | - | replace | Retrieves interest categories (groups) for the list. Includes `audiences_id` reference. |
| lists_locations | - | - | replace | Retrieves geographic locations of list members. Includes `audiences_id` reference. |
| lists_merge_fields | - | - | replace | Retrieves custom merge fields defined for the list. Includes `audiences_id` reference. |
| lists_segments | - | - | replace | Retrieves segments (filtered subsets) of the list. Includes `audiences_id` reference. |
| reports | id | send_time | merge | Retrieves campaign performance reports and analytics. |
| reports_advice | - | - | replace | Retrieves feedback and suggestions for improving campaign performance. Includes `reports_id` reference. |
| reports_domain_performance | - | - | replace | Retrieves email performance broken down by email domain. Includes `reports_id` reference. |
| reports_locations | - | - | replace | Retrieves geographic location data for campaign opens. Includes `reports_id` reference. |
| reports_sent_to | - | - | replace | Retrieves list of recipients who were sent the campaign. Includes `reports_id` reference. |
| reports_sub_reports | - | - | replace | Retrieves sub-reports for A/B test campaigns. Includes `reports_id` reference. |
| reports_unsubscribed | - | - | replace | Retrieves list of members who unsubscribed from the campaign. Includes `reports_id` reference. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/mailchimp_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Mailchimp table into your Postgres database.
