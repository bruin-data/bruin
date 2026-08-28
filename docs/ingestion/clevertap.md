# CleverTap

[CleverTap](https://clevertap.com/) is a customer engagement and retention platform that combines analytics, segmentation, and cross-channel campaigns for mobile and web apps.

Bruin supports CleverTap as both a source and a destination for [Ingestr assets](/assets/ingestr): you can ingest data from CleverTap into your data warehouse, and you can load user profiles and events from your warehouse back into CleverTap through its Upload API.

To set up a CleverTap connection, you need the Account ID and Passcode from your CleverTap project (**Settings → Project**). Your region is the subdomain of your dashboard URL, so `in1.dashboard.clevertap.com` means `region: in1`; European projects have no subdomain and appear as `global`. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/clevertap.html).

Follow the steps below to correctly set up CleverTap as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to CleverTap, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      clevertap:
        - name: "my_clevertap"
          account_id: "YOUR_CLEVERTAP_ACCOUNT_ID"
          passcode: "YOUR_CLEVERTAP_PASSCODE"
          region: "eu1"
          timezone: "Asia/Kolkata"
```

- `account_id`: The CleverTap Account ID for your project.
- `passcode`: The Account Passcode, or your user passcode if your admin has enabled user-level passcodes.
- `region` (optional): The data centre your account lives in. One of `eu1`, `in1`, `us1`, `sg1`, `aps3`, `mec1`. Defaults to `eu1`. European projects appear as `global` in the dashboard, and that value works too.
- `timezone` (optional): The timezone your CleverTap project is set to, as an IANA name such as `Asia/Kolkata`. Defaults to `UTC`. Set this to match your project, otherwise every event time is shifted by the difference.

### Step 2: Create an asset file for data ingestion

To ingest data from CleverTap, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., clevertap_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.clevertap
type: ingestr
connection: postgres

parameters:
  source_connection: my_clevertap
  source_table: 'profiles'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for CleverTap.
- `connection`: This is the destination connection.
- `source_connection`: The name of the CleverTap connection defined in .bruin.yml.
- `source_table`: The name of the data table in CleverTap you want to ingest. For example, `profiles` would ingest your user profiles.

The `events` and `profiles` tables accept an `event_name` parameter that narrows them to the events you name. Each name must match your CleverTap dashboard exactly, and several comma-separated names can share one destination table:

```yaml
  source_table: 'events?event_name=Charged,App Launched'
```

Leave the parameter out and `events` loads every event, while `profiles` covers everyone who has raised at least one of them.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `events` | – | ts | delete+insert | Individual event occurrences, with who raised the event and its properties. |
| `profiles` | object_id | – | replace | Your users, with their custom properties, activity summaries, and devices. |
| `campaigns` | id | – | replace | Campaigns created through the API, with their name, schedule, and status. |
| `campaign_reports` | id | – | replace | Delivery and engagement metrics for each completed API-created campaign. |
| `content_blocks` | id | updatedAt | merge | Reusable content blocks, with their type, content, and authorship. |
| `message_reports` | message_id | – | replace | Per-message delivery and engagement counts. |
| `event_schema` | name | – | replace | Every event defined in your project, with its properties. |
| `user_properties` | name | – | replace | Every custom profile property defined in your project. |
| `category_groups` | key | – | replace | Messaging subscription groups, with the channels each one covers. |

The `events` table is loaded incrementally with a delete+insert strategy keyed on `ts`, and `content_blocks` with a merge strategy keyed on `updatedAt`. Both respect `--interval-start`/`--interval-end`, where the end bound is exclusive of that day's activity, so use the following day to capture a full day. With no interval, everything is loaded. All other tables are loaded in full on every run.

> [!NOTE]
> `campaigns` and `campaign_reports` only ever contain campaigns created through the CleverTap API. Campaigns built in the dashboard are not included, because CleverTap offers no way to list them.

A few things to know: a campaign only gets a report once it has delivered, so `campaign_reports` usually holds fewer rows than `campaigns`; notification events such as push impressions cannot be exported and are skipped; and the `profile` column on `events` holds the user's details as they stand today, not as they were when the event happened.

> [!WARNING]
> Join `events` to `profiles` on `identity`, not `object_id`. `object_id` identifies one device, so joining on it silently drops the events a user raised on their other devices. `identity` is only set for users who have logged in.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.clevertap.asset.yml
```

As a result of this command, Bruin will ingest data from the given CleverTap table into your Postgres database.

## CleverTap as a destination

Bruin can also write user profiles and events **into** CleverTap through its [Upload API](https://developer.clevertap.com/docs/upload-user-profiles-api). Each source row is sent as one profile or event record, uploaded in bulk (up to 1000 records per request).

Reuse the same connection as the source — the destination uses `account_id`, `passcode`, and `region`. `timezone` is not used when writing, because CleverTap timestamps are absolute.

The `destination_table` parameter selects the record type — `profiles` or `events` — and the parameters after the `?` tell Bruin which columns carry the special fields. Every other column is uploaded as an attribute under its own name. (The record type and its parameters go in `destination_table` rather than the asset `name`, because an asset name may not contain `?`, `=`, or `&`.)

### Profiles

```yaml
name: clevertap_profiles
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.marketing_users'

  destination: clevertap
  destination_connection: my_clevertap
  destination_table: 'profiles?identity_column=email'
```

| Parameter | Required? | Description |
|-----------|-----------|-------------|
| `identity_column` | **Required** | The source column holding each row's identifier. For example, `identity_column=email` takes each row's identifier from the `email` column. |
| `id_type` | Optional | How CleverTap resolves the identifier: `identity` (default), `objectId`, `FBID`, or `GPID`. For example, `identity_column=device_id&id_type=objectId` sends each `device_id` value as an `objectId`. |
| `on_error` | Optional | `fail` (default) fails the run if CleverTap rejects any record; `skip` warns and continues. Either way each rejected record is printed as it happens and listed with its error at the end. |

Profiles are always upserted by identity on CleverTap's side, so re-sending a user updates their attributes instead of creating a duplicate. With no incremental key the whole table is re-sent each run; set an incremental key (such as `updated_at`) with an interval to send only the rows in that window.

### Events

```yaml
name: clevertap_events
type: ingestr

parameters:
  source_connection: my-bigquery
  source_table: 'analytics.purchases'

  destination: clevertap
  destination_connection: my_clevertap
  destination_table: 'events?identity_column=user_id&ts=purchased_at&event_name=Charged'

  incremental_key: purchased_at
```

| Parameter | Required? | Description |
|-----------|-----------|-------------|
| `event_name` **or** `event_name_column` | **Required** | A fixed event name applied to every row (`event_name`), or a column whose value is the event name per row (`event_name_column`) for tables that mix event types. |
| `identity_column` | **Required** | The source column holding each row's identifier. For example, `identity_column=email` takes each row's identifier from the `email` column. |
| `id_type` | Optional | How CleverTap resolves the identifier: `identity` (default), `objectId`, `FBID`, or `GPID`. For example, `identity_column=device_id&id_type=objectId` sends each `device_id` value as an `objectId`. |
| `ts` | Optional | The source column holding the event timestamp. If omitted, CleverTap stamps the upload time. |
| `on_error` | Optional | `fail` (default) fails the run if CleverTap rejects any record; `skip` warns and continues. Either way each rejected record is printed as it happens and listed with its error at the end. |

Events are always appended on CleverTap's side — each uploaded event is added to the user's timeline; CleverTap never replaces or de-duplicates events, so re-sending a row creates a duplicate. Use an incremental key (usually the same column as `ts`) with an interval to control exactly which events are sent each run.

> [!NOTE]
> Every record must carry an identifier; rows with an empty identity value are skipped. CleverTap accepts up to 1000 records per request and limits uploads to 3 concurrent requests per account; Bruin batches and rate-limits accordingly. For the full destination reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/clevertap.html).
