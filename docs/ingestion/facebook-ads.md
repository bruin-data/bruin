# Facebook Ads
Facebook Ads is the advertising platform that helps users to create targeted ads on Facebook, Instagram and Messenger.

Bruin supports Facebook Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Facebook Ads into your data warehouse.

In order to set up Facebook Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `access_token` and `account_id`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads#grab-credentials)

Follow the steps below to correctly set up Facebook Ads as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Facebook Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  facebookads:
    - name: "my_facebookads"
      access_token: "YOUR_FACEBOOK_ACCESS_TOKEN"
      account_id: "YOUR_ACCOUNT_ID"
```
- `access_token` is associated with Business Facebook App.
- `account_id`  is associated with Ad manager.

### Step 2: Create an asset file for data ingestion
To ingest data from Facebook Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., facebook_ads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.facebookads
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'ads'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the  Facebook Ads connection defined in .bruin.yml.
- `source_table`: The name of the data table in Facebook Ads you want to ingest. For example, `ads` would ingest data related to ads.

## Available Source Tables

Facebook Ads source allows ingesting the following sources into separate tables:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| campaigns | id | – | replace | Retrieves campaign data with fields: id, updated_time, created_time, name, status, effective_status, objective, start_time, stop_time, daily_budget, lifetime_budget |
| ad_sets | id | – | replace | Retrieves ad set data with fields: id, updated_time, created_time, name, status, effective_status, campaign_id, start_time, end_time, daily_budget, lifetime_budget, optimization_goal, promoted_object, billing_event, bid_amount, bid_strategy, targeting |
| ads | id | - | replace | Retrieves ad data with fields: id, updated_time, created_time, name, status, effective_status, adset_id, campaign_id, creative, targeting, tracking_specs, conversion_specs |
| ad_creatives | id | - | replace | Retrieves ad creative data with fields: id, name, status, thumbnail_url, object_story_spec, effective_object_story_id, call_to_action_type, object_type, template_url, url_tags, instagram_actor_id, product_set_id |
| leads | id | - | replace | Retrieves lead data with fields: id, created_time, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, form_id, field_data |
| facebook_insights | date_start | date_start | merge | Retrieves insights data. Supports custom configuration for level, fields, breakdowns, and action breakdowns. See [Facebook Insights Custom Configuration](#facebook-insights-custom-configuration) below. |

---

## Facebook Insights Custom Configuration

The `facebook_insights` table provides powerful customization options to retrieve performance metrics at different levels with various breakdowns. This allows you to build custom reports tailored to your analytics needs.

### Custom Table Format

The insights source table follows this format:

```plaintext
facebook_insights[:<level>][:<fields>][:<breakdowns>][:<action_breakdowns>]
```

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `level` | No | The aggregation level for insights data. Default: `ad` |
| `fields` | No | Comma-separated list of metrics to retrieve. Uses default fields if not specified |
| `breakdowns` | No | Comma-separated list of breakdowns to segment data |
| `action_breakdowns` | No | Comma-separated list of action breakdowns for action metrics |

### Available Levels

The `level` parameter determines at what granularity the insights data is aggregated:

| Level | Description |
|-------|-------------|
| `account` | Account-level aggregated metrics |
| `campaign` | Campaign-level metrics |
| `adset` | Ad set-level metrics |
| `ad` | Ad-level metrics (default) |

### Available Fields

You can request specific fields/metrics to retrieve. Common fields include:

| Field Category | Fields |
|----------------|--------|
| **Identifiers** | `campaign_id`, `adset_id`, `ad_id`, `campaign_name`, `adset_name`, `ad_name`, `account_id`, `account_name` |
| **Date** | `date_start`, `date_stop` |
| **Reach & Impressions** | `reach`, `impressions`, `frequency`, `full_view_impressions`, `full_view_reach` |
| **Clicks** | `clicks`, `unique_clicks`, `inline_link_clicks`, `unique_inline_link_clicks`, `outbound_clicks` |
| **Rates** | `ctr`, `unique_ctr`, `inline_link_click_ctr`, `unique_inline_link_click_ctr`, `website_ctr` |
| **Costs** | `spend`, `cpc`, `cpm`, `cpp`, `cost_per_inline_link_click`, `cost_per_unique_click` |
| **Actions** | `actions`, `action_values`, `conversions`, `cost_per_action_type`, `ad_click_actions` |
| **Video** | `video_thruplay_watched_actions`, `video_p25_watched_actions`, `video_p50_watched_actions`, `video_p75_watched_actions`, `video_p100_watched_actions` |
| **Other** | `account_currency`, `social_spend`, `objective`, `buying_type` |

### Available Breakdowns

Breakdowns allow you to segment your insights data by different dimensions:

| Breakdown | Description |
|-----------|-------------|
| `age` | Age ranges (e.g., 18-24, 25-34) |
| `gender` | Gender (male, female, unknown) |
| `country` | Country code |
| `region` | Region/state within a country |
| `dma` | Designated Market Area (US only) |
| `impression_device` | Device where the ad was shown |
| `platform_position` | Placement position (feed, stories, etc.) |
| `publisher_platform` | Platform (Facebook, Instagram, Messenger, Audience Network) |
| `device_platform` | Device operating system |
| `product_id` | Product ID for dynamic ads |
| `hourly_stats_aggregated_by_advertiser_time_zone` | Hourly breakdown in advertiser's timezone |
| `hourly_stats_aggregated_by_audience_time_zone` | Hourly breakdown in audience's timezone |

> [!NOTE]
> Some breakdowns cannot be combined. Refer to the [Facebook Marketing API documentation](https://developers.facebook.com/docs/marketing-api/insights/breakdowns) for valid combinations.

### Available Action Breakdowns

Action breakdowns provide additional segmentation for action-related metrics:

| Action Breakdown | Description |
|------------------|-------------|
| `action_type` | Type of action (e.g., link_click, purchase, lead) |
| `action_target_id` | Target of the action (page ID, app ID, etc.) |
| `action_destination` | Where the action was completed |
| `action_device` | Device where the action occurred |
| `action_reaction` | Reaction type (like, love, etc.) |
| `action_video_sound` | Whether video was played with sound |
| `action_video_type` | Type of video action |
| `action_carousel_card_id` | Carousel card that triggered the action |
| `action_carousel_card_name` | Name of the carousel card |

### Examples

#### Basic Usage (Default Configuration)

Retrieve insights with default settings at the ad level:

```yaml
name: public.facebook_insights
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights'
  destination: postgres
```

#### Campaign-Level Insights

Retrieve insights aggregated at the campaign level:

```yaml
name: public.facebook_campaign_insights
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:campaign'
  destination: postgres
```

#### Custom Fields

Retrieve specific fields only:

```yaml
name: public.facebook_insights_custom
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:ad:impressions,clicks,spend,ctr,cpc'
  destination: postgres
```

#### With Breakdowns

Retrieve insights broken down by age and gender:

```yaml
name: public.facebook_insights_demographics
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:ad:impressions,clicks,spend:age,gender'
  destination: postgres
```

#### With Country Breakdown

Retrieve insights broken down by country:

```yaml
name: public.facebook_insights_geo
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:campaign:impressions,clicks,spend,reach:country'
  destination: postgres
```

#### With Action Breakdowns

Retrieve insights with action type breakdown for conversion analysis:

```yaml
name: public.facebook_insights_actions
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:ad:actions,action_values,cost_per_action_type::action_type'
  destination: postgres
```

> [!NOTE]
> When specifying action breakdowns without regular breakdowns, use `::` (double colon) to skip the breakdowns parameter.

#### Full Custom Configuration

Retrieve insights with level, custom fields, breakdowns, and action breakdowns:

```yaml
name: public.facebook_insights_full
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'facebook_insights:adset:impressions,clicks,spend,actions:age,gender:action_type'
  destination: postgres
```

---

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/facebook_ads_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Facebook Ads table into your Postgres database.


<img width="962" alt="facebooksads" src="https://github.com/user-attachments/assets/7476fb59-8885-4c76-95d4-f150cac2d423">

