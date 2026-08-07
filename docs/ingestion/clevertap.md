# CleverTap

[CleverTap](https://clevertap.com/) is a customer engagement and retention platform that combines analytics, segmentation, and cross-channel campaigns for mobile and web apps.

Bruin supports CleverTap as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from CleverTap into your data warehouse.

To set up a CleverTap connection, you need the Account ID and Passcode from your CleverTap project (**Settings → Project**). For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/clevertap.html).

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

The `events` and `profiles` tables accept an `event_name` parameter that narrows them to the events you name. The name must match your CleverTap dashboard exactly, and several comma-separated names can share one destination table:

```yaml
  source_table: 'events?event_name=Charged,App Launched'
```

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

The `events` table is loaded incrementally with a delete+insert strategy keyed on `ts`, and respects `--interval-start`/`--interval-end`. The `content_blocks` table is loaded incrementally with a merge strategy keyed on `updatedAt`. All other tables are loaded in full on every run.

> [!NOTE]
> `campaigns` and `campaign_reports` only ever contain campaigns created through the CleverTap API. Campaigns built in the dashboard are not included, because CleverTap offers no way to list them.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.clevertap.asset.yml
```

As a result of this command, Bruin will ingest data from the given CleverTap table into your Postgres database.
