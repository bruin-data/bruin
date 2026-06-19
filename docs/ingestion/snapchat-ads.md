# Snapchat Ads

Snapchat Ads is an advertising platform that enables businesses to create, manage, and analyze ad campaigns targeting Snapchat's user base.

Bruin supports Snapchat Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Snapchat Ads into your data warehouse.

In order to set up Snapchat Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `refresh_token`, `client_id`, `client_secret`, and optionally `organization_id`. For details on how to obtain these credentials, please refer [here](https://developers.snap.com/api/marketing-api/Ads-API/authentication)

Follow the steps below to correctly set up Snapchat Ads as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Snapchat Ads as a source, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  snapchatads:
     - name: my-snapchatads
       refresh_token: "your_refresh_token"
       client_id: "your_client_id"
       client_secret: "your_client_secret"
       organization_id: "your_organization_id"
```

- `refresh_token` (required): OAuth refresh token for Snapchat Ads API authentication.
- `client_id` (required): OAuth client ID for your Snapchat Ads API app.
- `client_secret` (required): OAuth client secret for your Snapchat Ads API app.
- `organization_id` (optional): Organization ID. Required for most resources except `organizations`.

### Step 2: Create an asset file for data ingestion

To ingest data from Snapchat Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., snapchat_ads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.snapchatads
type: ingestr
connection: postgres

parameters:
  source_connection: my-snapchatads
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Snapchat Ads connection defined in .bruin.yml.
- `source_table`: The table in Snapchat Ads you want to ingest.

## Available Source Tables

### Organization-level Resources

These resources require only authentication credentials:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `organizations` | id | updated_at | merge | Retrieves all organizations for the authenticated user |
| `fundingsources` | id | updated_at | merge | Retrieves all funding sources for the organization (requires `organization_id`) |
| `billingcenters` | id | updated_at | merge | Retrieves all billing centers for the organization (requires `organization_id`) |
| `adaccounts` | id | updated_at | merge | Retrieves all ad accounts for the organization (requires `organization_id`) |
| `transactions` | - | - | replace | Retrieves all transactions for the organization (requires `organization_id`) |
| `members` | - | - | replace | Retrieves all members of the organization (requires `organization_id`) |
| `roles` | - | - | replace | Retrieves all roles for the organization (requires `organization_id`) |

### Ad Account-level Resources

These resources can fetch data for a specific ad account, multiple ad accounts, or all ad accounts in the organization. All of these resources support the following formats:

- `table:ad_account_id` - fetch data for a specific ad account
- `table:id1,id2,id3` - fetch data for multiple ad accounts

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `campaigns` | id | updated_at | merge | Retrieves all campaigns for ad account(s). Supports `campaigns:ad_account_id` or `campaigns:id1,id2,id3` |
| `adsquads` | id | updated_at | merge | Retrieves all ad squads for ad account(s). Supports `adsquads:ad_account_id` or `adsquads:id1,id2,id3` |
| `ads` | id | updated_at | merge | Retrieves all ads for ad account(s). Supports `ads:ad_account_id` or `ads:id1,id2,id3` |
| `invoices` | id | updated_at | merge | Retrieves all invoices for ad account(s). Supports `invoices:ad_account_id` or `invoices:id1,id2,id3` |
| `event_details` | id | updated_at | merge | Retrieves all event details (pixel events) for ad account(s). Supports `event_details:ad_account_id` or `event_details:id1,id2,id3` |
| `creatives` | id | updated_at | merge | Retrieves all creatives for ad account(s). Supports `creatives:ad_account_id` or `creatives:id1,id2,id3` |
| `segments` | id | updated_at | merge | Retrieves all audience segments for ad account(s). Supports `segments:ad_account_id` or `segments:id1,id2,id3` |

### Stats / Measurement Data

Snapchat Ads source supports fetching stats/measurement data for campaigns, ad squads, ads, and ad accounts through dedicated stats resources.

#### Stats Resources

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `campaigns_stats` | campaign_id, adsquad_id, ad_id, start_time, end_time | - | merge | Retrieves stats for all campaigns. Supports breakdowns by `ad` or `adsquad` |
| `ads_stats` | campaign_id, adsquad_id, ad_id, start_time, end_time | - | merge | Retrieves stats for all ads. No breakdown supported (already lowest level) |
| `ad_squads_stats` | campaign_id, adsquad_id, ad_id, start_time, end_time | - | merge | Retrieves stats for all ad squads. Supports breakdown by `ad` |
| `ad_accounts_stats` | campaign_id, adsquad_id, ad_id, start_time, end_time | - | merge | Retrieves stats for all ad accounts. Supports breakdowns by `ad`, `adsquad`, or `campaign` |

#### Stats Table Format

```plaintext
<resource_name>:<granularity>:<fields>
<resource_name>:<breakdown>,<granularity>:<fields>
```

**Parameters:**

- `resource_name` (required): One of `campaigns_stats`, `ads_stats`, `ad_squads_stats`, `ad_accounts_stats`
- `breakdown` (optional): Object-level breakdown. Valid values depend on the resource:
  - `campaigns_stats`: `ad`, `adsquad`
  - `ad_squads_stats`: `ad`
  - `ad_accounts_stats`: `ad`, `adsquad`, `campaign`
  - `ads_stats`: No breakdown supported
- `granularity` (required): Time granularity - `TOTAL`, `DAY`, `HOUR`, or `LIFETIME`
- `fields` (required): Metrics to retrieve (comma-separated). Examples: `impressions`, `spend`, `swipes`, `conversion_purchases`, etc.

**Format Examples:**
- Without breakdown: `campaigns_stats:DAY:impressions,spend,swipes`
- With ad breakdown: `campaigns_stats:ad,HOUR:impressions,spend`
- With adsquad breakdown: `campaigns_stats:adsquad,DAY:impressions,swipes`
- Ad account stats with campaign breakdown: `ad_accounts_stats:campaign,DAY:spend`

**Note:** When breakdown is not specified, `adsquad_id` and `ad_id` will be `NULL` in the results.

#### Stats Asset Example

To ingest campaign stats with daily granularity, create an asset file:

```yaml
name: public.snapchat_campaigns_stats
type: ingestr
connection: postgres

parameters:
  source_connection: my-snapchatads
  source_table: 'campaigns_stats:DAY:impressions,spend,swipes'

  destination: postgres
```

For stats with ad-level breakdown:

```yaml
name: public.snapchat_campaigns_stats_detailed
type: ingestr
connection: postgres

parameters:
  source_connection: my-snapchatads
  source_table: 'campaigns_stats:ad,DAY:impressions,spend'

  destination: postgres
```

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/snapchat_ads_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Snapchat Ads table into your Postgres database.
