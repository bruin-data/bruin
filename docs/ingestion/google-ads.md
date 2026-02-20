# Google Ads

[Google Ads](https://ads.google.com/), formerly known as Google Adwords, is an online advertising platform developed by Google, where advertisers bid to display brief advertisements, service offerings, product listings, and videos to web users. It can place ads in the results of search engines like Google Search (the Google Search Network), mobile apps, videos, and on non-search websites.

Bruin supports Google Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Ads into your data warehouse.

In order to set up Google Ads connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Google Ads as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Google Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      googleads:
        - name: "my-googleads"
          customer_id: "YOUR_GOOGLE_ADS_CUSTOMER_ID"
          dev_token: "YOUR_DEVELOPER_TOKEN"
          service_account_file: "/path/to/service-account.json"
          login_customer_id: "YOUR_MCC_CUSTOMER_ID"  # optional, only needed for MCC accounts

          # alternatively, you can specify the service account json directly
          service_account_json: |
          {
            "type": "service_account",
            ...
          }

```

- `customer_id`: The account ID of your google ads account. You can specify multiple customer IDs separated by commas (e.g., `1234567890,0987654321`).
- `dev_token`: [Developer Token](https://developers.google.com/google-ads/api/docs/get-started/dev-token) for your application.
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself
- `login_customer_id` (optional): The Manager Account (MCC) ID to use when accessing client accounts. Required when your service account has access to an MCC and you want to pull data from a client account under that MCC. See [Google Ads API docs](https://developers.google.com/google-ads/api/docs/concepts/call-structure#cid) for more details.

For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/google-ads.html#setting-up-a-google-ads-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from Google Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., google_ads_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'campaign_report_daily'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Google Ads.
- connection: This is the destination connection.
- source_connection: The name of the Google Ads connection defined in .bruin.yml.
- source_table: The name of the resource in Google Ads you want to ingest. You can also request a custom report by specifying the source table as `daily:{resource}:{dimensions}:{metrics}`.

### Overriding Customer IDs per Table

You can override the customer ID from the connection by appending `:customer_ids` to the table name. This is useful when you want to ingest data from multiple customer accounts:

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'campaign_report_daily:1234567890,0987654321'
  destination: postgres
```

For custom reports, you can also specify customer IDs at the end of the report specification:

```yaml
name: public.custom_report
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'daily:campaign:campaign.id,customer.id:clicks,impressions:1234567890,0987654321'
  destination: postgres
```

## Available Source Tables

Google Ads source allows ingesting the following sources into separate tables:

| Table | Description |
|-------|-------------|
| `account_report_daily` | Provides daily metrics aggregated at the account level |
| `campaign_report_daily` | Provides daily metrics aggregated at the campaign level |
| `ad_group_report_daily` | Provides daily metrics aggregated at the ad group level |
| `ad_report_daily` | Provides daily metrics aggregated at the ad level |
| `audience_report_daily` | Provides daily metrics aggregated at the audience level |
| `keyword_report_daily` | Provides daily metrics aggregated at the keyword level |
| `click_report_daily` | Provides daily metrics on clicks |
| `landing_page_report_daily` | Provides daily metrics on landing page performance |
| `search_keyword_report_daily` | Provides daily metrics on search keywords |
| `search_term_report_daily` | Provides daily metrics on search terms |
| `lead_form_submission_data_report_daily` | Provides daily metrics on lead form submissions |
| `local_services_lead_report_daily` | Provides daily metrics on local services leads |
| `local_services_lead_conversations_report_daily` | Provides daily metrics on local services lead conversations |
| `daily:{resource_name}:{dimensions}:{metrics}` | Custom reports with specified resource, dimensions, and metrics |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/google_ads_integration.asset.yml
```
As a result of this command, Bruin will ingest data for the given Google Ads resource into your Postgres database.

## GAQL Queries

In addition to predefined tables and custom reports, you can execute raw [Google Ads Query Language (GAQL)](https://developers.google.com/google-ads/api/docs/query/overview) queries directly. This gives you full flexibility to query any resource with any combination of fields, segments, and metrics.

### GAQL Query Format

To run a GAQL query, use the `gaql_query:` prefix followed by your query:

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'gaql_query:SELECT campaign.id, campaign.name, campaign.status FROM campaign LIMIT 10'
  destination: postgres
```

### Date Filtering with Placeholders

GAQL queries support special placeholders for date filtering that integrate with Bruin's incremental strategy:

- `:interval_start` - Replaced with the start date of the run interval (defaults to `1970-01-01` if not provided)
- `:interval_end` - Replaced with the end date of the run interval (defaults to today's date if not provided)

**Example with date range:**

```yaml
name: public.campaign_metrics
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'gaql_query:SELECT campaign.id, campaign.name, metrics.impressions, metrics.clicks, segments.date FROM campaign WHERE segments.date BETWEEN :interval_start AND :interval_end'
  destination: postgres
```

### GAQL Query Examples

**Get all campaigns with their status:**

```yaml
source_table: 'gaql_query:SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type FROM campaign'
```

**Get ad group performance metrics:**

```yaml
source_table: 'gaql_query:SELECT ad_group.id, ad_group.name, campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros, segments.date FROM ad_group WHERE segments.date DURING LAST_30_DAYS'
```

**Get keyword performance:**

```yaml
source_table: 'gaql_query:SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, metrics.impressions, metrics.clicks, metrics.conversions, segments.date FROM keyword_view WHERE segments.date DURING LAST_7_DAYS'
```

**Get search terms report:**

```yaml
source_table: 'gaql_query:SELECT search_term_view.search_term, campaign.name, ad_group.name, metrics.impressions, metrics.clicks, metrics.cost_micros FROM search_term_view WHERE segments.date DURING LAST_30_DAYS ORDER BY metrics.impressions DESC LIMIT 100'
```

> [!NOTE]
> GAQL queries use `append` write disposition by default. Each row includes a `customer_id` field to identify which account the data came from.

> [!TIP]
> Use the [Google Ads Query Builder](https://developers.google.com/google-ads/api/fields/v18/overview_query_builder) to construct and validate your GAQL queries before using them with ingestr.
