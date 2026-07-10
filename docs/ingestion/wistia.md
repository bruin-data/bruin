# Wistia

[Wistia](https://wistia.com/) is a video hosting and analytics platform for businesses.

Bruin supports Wistia as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest account, media, channel, webinar, and stats data from Wistia into your data platform.

To set up a Wistia connection, add a configuration entry to `.bruin.yml` and reference it from an asset file. You need an `access_token` to authenticate with Wistia.

## Configuration

### Step 1: Add a connection to the `.bruin.yml` file

```yaml
connections:
  wistia:
    - name: "my_wistia"
      access_token: "your-wistia-api-token"
```

- `access_token` (required): Wistia API token. ingestr also accepts `api_key` or `token` as aliases for this credential.
- `api_version` (optional): Value sent as the `X-Wistia-API-Version` header. ingestr defaults to `2026-03`.
- `base_url` (optional): Override for the Wistia API base URL. This is mostly useful for tests; ingestr defaults to `https://api.wistia.com/modern`.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `wistia.asset.yml`) inside the assets folder:

```yaml
name: public.wistia_medias
type: ingestr

parameters:
  source_connection: my_wistia
  source_table: 'medias'

  destination: postgres
```

- `source_connection`: The name of the Wistia connection defined in `.bruin.yml`.
- `source_table`: The Wistia table to ingest. See available tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/wistia.asset.yml
```

Running this command ingests data from Wistia into your destination.

## Available Source Tables

### Data API Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `account` | - | - | replace | Current account summary. |
| `token` | - | - | replace | Current token summary. |
| `allowed_domains` | - | - | replace | Allowed domains. |
| `folders` | - | - | replace | Folders. |
| `folder:<folder_id>` | - | - | replace | A single folder. |
| `folder_sharings:<folder_id>` | - | - | replace | Sharing records for a folder. |
| `subfolders:<folder_id>` | - | - | replace | Subfolders for a folder. |
| `medias` | - | - | replace | Media records. |
| `media:<media_id>` | - | - | replace | A single media record. |
| `captions` | - | - | replace | Captions across the account. |
| `captions:<media_id>` | - | - | replace | Captions filtered by media. |
| `media_captions:<media_id>` | - | - | replace | Captions for a media. |
| `media_localizations:<media_id>` | - | - | replace | Localizations for a media. |
| `media_customizations:<media_id>` | - | - | replace | Customizations for a media. |
| `media_stats:<media_id>` | - | - | replace | Aggregated media stats from the Data API. |
| `channels` | - | - | replace | Channels. |
| `channel:<channel_id>` | - | - | replace | A single channel. |
| `channel_episodes` | - | - | replace | Channel episodes. |
| `channel_episodes_by_channel:<channel_id>` | - | - | replace | Episodes in a channel. |
| `tags` | - | - | replace | Tags. |
| `webinars` | - | - | replace | Webinars. |
| `webinar:<webinar_id>` | - | - | replace | A single webinar. |

### Stats API Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `stats_account` | - | - | replace | Current account stats. |
| `stats_account_by_date` | - | date | merge | Account stats by date. |
| `stats_events` | - | received_at | merge | Event records. |
| `stats_events:<media_id>` | - | received_at | merge | Event records filtered by media. |
| `stats_events_by_visitor:<visitor_key>` | - | received_at | merge | Event records filtered by visitor. |
| `stats_visitors` | - | - | replace | Visitors. |
| `stats_event:<event_key>` | - | - | replace | A single event. |
| `stats_visitor:<visitor_key>` | - | - | replace | A single visitor. |
| `stats_media:<media_id>` | - | - | replace | Stats for a media. |
| `stats_media_by_date:<media_id>` | - | date | merge | Stats for a media by date. |
| `stats_media_engagement:<media_id>` | - | - | replace | Engagement stats for a media. |
| `stats_project:<project_id>` | - | - | replace | Stats for a project/folder. |

Date-filtered Stats API tables use Bruin's run interval as Wistia `start_date` and `end_date` query parameters. Bruin passes `--interval-start` and `--interval-end` to ingestr when a run interval is defined.
