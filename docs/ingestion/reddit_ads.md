# Reddit Ads

Reddit Ads is an advertising platform for creating, managing, and analyzing advertising campaigns on Reddit.

Bruin supports Reddit Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Reddit Ads into your data warehouse.

To set up a Reddit Ads connection, add a configuration item to `.bruin.yml` and reference it from an ingestr asset. You need an OAuth `access_token` and a comma-separated list of `account_ids`. See the [official ingestr Reddit Ads documentation](https://getbruin.com/docs/ingestr/supported-sources/reddit_ads.html) for the OAuth flow and account ID discovery steps.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Reddit Ads as a source, add a `reddit_ads` connection to the connections section of `.bruin.yml`:

```yaml
connections:
  reddit_ads:
    - name: my-reddit-ads
      access_token: "token_123"
      account_ids: "id_123,id_456"
```

- `access_token` (required): OAuth2 access token used to authenticate with the Reddit Ads API.
- `account_ids` (required): Comma-separated Reddit Ads account IDs to retrieve data for.

Bruin uses the `reddit_ads` connection key in `.bruin.yml`. The underlying ingestr source URI scheme is `redditads://`, as required by ingestr.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file to define the data flow:

```yaml
name: public.reddit_ads_campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-reddit-ads
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset type. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: The destination connection, which defines where the data should be stored.
- `source_connection`: The name of the Reddit Ads connection defined in `.bruin.yml`.
- `source_table`: The Reddit Ads table to ingest. See [Available Source Tables](#available-source-tables) for options.
- `destination`: The destination connection name.

## Available Source Tables

Reddit Ads source allows ingesting the following tables:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `accounts` | id | - | replace | Retrieves all ad accounts accessible by the authenticated user. |
| `campaigns` | id | - | replace | Retrieves campaigns for each ad account. |
| `ad_groups` | id | - | replace | Retrieves ad groups for each ad account. |
| `ads` | id | - | replace | Retrieves ads for each ad account. |
| `posts` | id | - | replace | Retrieves ad posts and creatives for each ad account. |
| `custom_audiences` | id | - | replace | Retrieves custom audiences for targeting. |
| `saved_audiences` | id | - | replace | Retrieves saved audience configurations. |
| `pixels` | id | - | replace | Retrieves conversion tracking pixels. |
| `funding_instruments` | id | - | replace | Retrieves funding instruments for each ad account. |
| `custom` | level_id, breakdowns | date | merge | Retrieves advertising performance reports for selected levels, breakdowns, and metrics. |

### Example

To ingest campaigns:

```yaml
name: public.reddit_ads_campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-reddit-ads
  source_table: 'campaigns'

  destination: postgres
```

## Custom Reports

The `custom` table uses the Reddit Ads Reports API to retrieve performance data such as impressions, clicks, reach, and spend, broken down by dimensions such as date, country, community, or device.

**Format:**

```plaintext
custom:<level>,<breakdowns>:<metrics>
```

**Parameters:**

- `level` (required): Reporting level. Must be one of `account`, `campaign`, `ad_group`, or `ad`.
- `breakdowns` (optional): Comma-separated breakdown list after the level. Valid breakdowns include `date`, `country`, `region`, `community`, `placement`, `device_os`, `gender`, `interest`, `keyword`, and `carousel_card`. Reddit Ads supports up to two breakdowns per report.
- `metrics` (required): Comma-separated metrics to retrieve, such as `impressions`, `reach`, `clicks`, `spend`, `ecpm`, `ctr`, and `cpc`.

By default, ingestr fetches custom report data from January 1, 2020 to the current date. You can provide a custom range with the asset start and end interval settings.

### Custom Reports Examples

#### Retrieve daily campaign performance data

```yaml
name: public.reddit_ads_campaign_daily
type: ingestr
connection: postgres

parameters:
  source_connection: my-reddit-ads
  source_table: 'custom:campaign,date:impressions,clicks,spend'

  destination: postgres
```

The applied parameters for the report are:

- level: `campaign`
- breakdowns: `date`
- metrics: `impressions`, `clicks`, `spend`

#### Retrieve ad group performance by country

```yaml
name: public.reddit_ads_ad_group_country
type: ingestr
connection: postgres

parameters:
  source_connection: my-reddit-ads
  source_table: 'custom:ad_group,date,country:impressions,reach,ctr'

  destination: postgres
```

The applied parameters for the report are:

- level: `ad_group`
- breakdowns: `date`, `country`
- metrics: `impressions`, `reach`, `ctr`

## Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/reddit_ads.asset.yml
```

As a result of this command, Bruin will ingest data from the given Reddit Ads table into your destination database.
