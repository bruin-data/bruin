# LinkedIn Ads

LinkedIn Ads is an advertising platform that allows businesses and marketers to create, manage, and analyze advertising campaigns.

Bruin supports LinkedIn Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from LinkedIn Ads into your data platform.

To set up a LinkedIn Ads connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `access_token` and `account_ids`. For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/linkedin_ads.html#create-a-linkedin-developer-application-to-obtain-an-access-token)

Follow the steps below to set up LinkedIn Ads correctly as a data source and run ingestion.

## Step 1: Add a connection to the .bruin.yml file

In order to set up LinkedIn Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      linkedinads:
        - name: "my-linkedinads"
          access_token: "token_123"
          account_ids: "id_123,id_456"  # Required only for custom tables
```

- `access_token` (required): The access token is used for authentication and allows your app to access data based on the permissions configured in the Developer App for your LinkedIn account.
- `account_ids` (optional): A comma-separated list of LinkedIn Ad Account IDs that identifies the accounts from which you want to retrieve data. This is only required for custom tables.

## Step 2: Create an asset file for data ingestion

To ingest data from LinkedIn Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., linkedin_ads.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.linkedinads
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'ad_accounts'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: The name of the destination connection and it must match the name of the connection defined in the .`bruin.yml` file.
- `source_connection`: The name of the LinkedIn Ads connection defined in `.bruin.yml`.
- `source_table`: The name of the table in LinkedIn Ads to ingest. See [Available Source Tables](#available-source-tables) for options.

## Available Source Tables

LinkedIn Ads source allows ingesting the following sources:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `ad_accounts` | id | – | replace | Retrieves all ad accounts accessible by the authenticated user. |
| `ad_account_users` | user, account | – | replace | Retrieves users associated with each ad account. |
| `campaign_groups` | id | – | replace | Retrieves campaign groups for each ad account. |
| `campaigns` | id | – | replace | Retrieves campaigns for each ad account. |
| `creatives` | id | – | replace | Retrieves creatives for each ad account. |
| `conversions` | id | – | replace | Retrieves conversion rules for each ad account. |
| `lead_forms` | id | – | replace | Retrieves lead generation forms for each ad account. |
| `lead_form_responses` | id | date (interval) | merge | Retrieves lead form responses for each ad account. |
| `dmp_segments` | id | – | replace | Retrieves matched/retargeting audience segments (sizes, match rates, rules) for each ad account. |
| `insight_tags` | id | – | replace | Retrieves Insight Tag configuration and installation status for each ad account. |
| `insight_tag_domains` | domainName, account_id | – | replace | Retrieves domains associated with Insight Tags for each ad account. |
| `custom` | [dimension, date] or [dimension, start_date, end_date] | date (daily) or start_date (monthly) | merge | Custom reports allow you to retrieve data based on specific dimensions and metrics. |

### Example

#### Retrieve all campaigns

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'campaigns'

  destination: postgres
```

## Custom Reports

The `custom` table uses LinkedIn's [Analytics Finder API](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2024-11&tabs=http#analytics-finder) to pull advertising performance reports. This allows you to retrieve metrics like impressions, clicks, and conversions broken down by dimensions such as campaign, account, or creative.

> [!IMPORTANT]
> When using custom tables, you must include `account_ids` in your `.bruin.yml` connection configuration.

**Format:**

```html
custom:<dimensions>:<metrics>
```

**Parameters:**

- `dimensions` (required): A comma-separated list of dimensions is required. It must include one pivot dimension and one time-based dimension (`date` or `month`).
  - Pivot dimensions (pick one):
    - Entity: `campaign`, `creative`, `account`
    - Placement & device: `impression_device`
    - Demographic: `member_job_title`, `member_job_function`, `member_seniority`, `member_industry`, `member_company_size`, `member_company`, `member_country`, `member_region`
  - Time dimensions:
    - `date`: group the data in your report by day
    - `month`: group the data in your report by month
- `metrics` (required): A comma-separated list of [metrics](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2024-11&tabs=http#metrics-available) to retrieve.

> [!NOTE]
> By default, the asset fetches data from January 1, 2018 to today's date. You can specify a custom date range using the **Start Date** and **End Date** fields in the Bruin UI when running the asset.

### Custom Reports Examples

#### Retrieve campaign data with daily metrics

```yaml
name: public.campaign_daily
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'custom:campaign,date:impressions,clicks'

  destination: postgres
```

The applied parameters for the report are:

- dimensions: `campaign`, `date`
- metrics: `impressions`, `clicks`

#### Retrieve creative data with monthly metrics

```yaml
name: public.creative_monthly
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'custom:creative,month:impressions,shares,videoCompletions'

  destination: postgres
```

The applied parameters for the report are:

- dimensions: `creative`, `month`
- metrics: `shares`, `impressions`, `videoCompletions`

#### Retrieve account data with monthly metrics

```yaml
name: public.account_monthly
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'custom:account,month:totalEngagements,impressions'

  destination: postgres
```

The applied parameters for the report are:

- dimensions: `account`, `month`
- metrics: `totalEngagements`, `impressions`

### Common Ad Analytics Reports

The following common analytics breakdowns are all available through the `custom` table:

| Report | `source_table` |
| ------ | -------------- |
| Ad Analytics by Campaign | `custom:campaign,date:<metrics>` |
| Ad Analytics by Creative | `custom:creative,date:<metrics>` |
| Ad Analytics by Impression Device | `custom:impression_device,date:<metrics>` |
| Ad Analytics by Member Company Size | `custom:member_company_size,date:<metrics>` |
| Ad Analytics by Member Country | `custom:member_country,date:<metrics>` |
| Ad Analytics by Member Job Function | `custom:member_job_function,date:<metrics>` |
| Ad Analytics by Member Job Title | `custom:member_job_title,date:<metrics>` |
| Ad Analytics by Member Industry | `custom:member_industry,date:<metrics>` |
| Ad Analytics by Member Region | `custom:member_region,date:<metrics>` |
| Ad Analytics by Member Company | `custom:member_company,date:<metrics>` |

Replace `<metrics>` with the LinkedIn metrics you want (e.g. `impressions,clicks,costInLocalCurrency`). Swap `date` for `month` to switch from daily to monthly granularity.

Any other pivot dimension from the list above — including `account` and `member_seniority` — is reachable with the same pattern, e.g. `custom:account,date:<metrics>` or `custom:member_seniority,date:<metrics>`.

## Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/linkedin_ads.asset.yml
```

As a result of this command, Bruin will ingest data from the given LinkedIn Ads table into your Postgres database.
