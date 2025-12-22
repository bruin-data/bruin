# Snapchat Ads
Snapchat Ads is an advertising platform that enables businesses to create, manage, and analyze ad campaigns targeting Snapchat's user base.

Bruin supports Snapchat Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Snapchat Ads into your data warehouse.

In order to set up Snapchat Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `refresh_token`, `client_id`, `client_secret`, and optionally `organization_id`. For details on how to obtain these credentials, please refer [here](https://developers.snap.com/api/marketing-api/Ads-API/authentication)

Follow the steps below to correctly set up Snapchat Ads as a data source and run ingestion.

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
- `table:ad_account_id` - fetch data for a single ad account
- `table:ad_account_id1,ad_account_id2` - fetch data for multiple ad accounts

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `campaigns` | id | updated_at | merge | Retrieves all campaigns for ad account(s). Supports `campaigns:ad_account_id` or `campaigns:id1,id2` |
| `adsquads` | id | updated_at | merge | Retrieves all ad squads for ad account(s). Supports `adsquads:ad_account_id` or `adsquads:id1,id2` |
| `ads` | id | updated_at | merge | Retrieves all ads for ad account(s). Supports `ads:ad_account_id` or `ads:id1,id2` |
| `invoices` | id | updated_at | merge | Retrieves all invoices for ad account(s). Supports `invoices:ad_account_id` or `invoices:id1,id2` |
| `event_details` | id | updated_at | merge | Retrieves all event details (pixel events) for ad account(s). Supports `event_details:ad_account_id` or `event_details:id1,id2` |
| `creatives` | id | updated_at | merge | Retrieves all creatives for ad account(s). Supports `creatives:ad_account_id` or `creatives:id1,id2` |
| `segments` | id | updated_at | merge | Retrieves all audience segments for ad account(s). Supports `segments:ad_account_id` or `segments:id1,id2` |

### Stats / Measurement Data

Snapchat Ads source supports fetching stats/measurement data for campaigns, ad squads, ads, and ad accounts through dedicated stats resources.

#### Stats Resources

| Table | Inc Strategy | Details |
|-------|--------------|---------|
| `campaigns_stats` | replace | Retrieves stats for all campaigns in the organization or specific ad account |
| `ads_stats` | replace | Retrieves stats for all ads in the organization or specific ad account |
| `ad_squads_stats` | replace | Retrieves stats for all ad squads in the organization or specific ad account |
| `ad_accounts_stats` | replace | Retrieves stats for all ad accounts in the organization (Note: only `spend` field is supported) |

#### Stats Table Format

The stats source table follows this format:

```plaintext
<resource_name>:<granularity>[:<fields>][:<options>]
```

Or with a specific ad account:

```plaintext
<resource_name>:<ad_account_id>:<granularity>[:<fields>][:<options>]
```

**Parameters:**

- `resource_name`: One of `campaigns_stats`, `ads_stats`, `ad_squads_stats`, `ad_accounts_stats`
- `ad_account_id` (optional): Specific ad account ID to fetch stats for
- `granularity`: Time granularity - `TOTAL`, `DAY`, `HOUR`, or `LIFETIME`
- `fields` (optional): Metrics to retrieve (comma-separated). Default: `impressions,spend`
- `options` (optional): Additional parameters in `key=value,key=value` format

**Available Options:**

| Option | Description | Values |
|--------|-------------|--------|
| `breakdown` | Object-level breakdown | `ad`, `adsquad` (Campaign only), `campaign` (Ad Account only) |
| `dimension` | Insight-level breakdown | `GEO`, `DEMO`, `INTEREST`, `DEVICE` |
| `pivot` | Pivot for insights breakdown | `country`, `region`, `dma`, `gender`, `age_bucket`, `interest_category_id`, `interest_category_name`, `operating_system`, `make`, `model` |
| `swipe_up_attribution_window` | Attribution window for swipe ups | `1_DAY`, `7_DAY`, `28_DAY` (default) |
| `view_attribution_window` | Attribution window for views | `none`, `1_HOUR`, `3_HOUR`, `6_HOUR`, `1_DAY` (default), `7_DAY`, `28_DAY` |
| `omit_empty` | Omit records with zero data | `false` (default), `true` |

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

For stats with additional options like breakdown and attribution window:

```yaml
name: public.snapchat_campaigns_stats_detailed
type: ingestr
connection: postgres

parameters:
  source_connection: my-snapchatads
  source_table: 'campaigns_stats:DAY:impressions,spend,swipes:breakdown=ad,swipe_up_attribution_window=7_DAY'

  destination: postgres
```

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/snapchat_ads_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Snapchat Ads table into your Postgres database.
