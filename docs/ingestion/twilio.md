# Twilio

[Twilio](https://www.twilio.com/) is a cloud communications platform for messaging, voice, and phone numbers.

Bruin supports Twilio as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Twilio into your data warehouse.

To set up a Twilio connection, you need your Account SID and either an Auth Token or an API Key SID + Secret. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/twilio.html)

Follow the steps below to correctly set up Twilio as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Twilio, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      twilio:
        - name: "my_twilio"
          account_sid: "YOUR_TWILIO_ACCOUNT_SID"
          auth_token: "YOUR_TWILIO_AUTH_TOKEN"
```

- `account_sid`: Your Twilio Account SID (starts with `AC`). Required.
- `auth_token`: Your Twilio Auth Token. Required unless `api_key`/`api_secret` are provided.
- `api_key` (optional): An API Key SID, used instead of the Auth Token. Requires `api_secret`.
- `api_secret` (optional): The secret for the API Key. Required when `api_key` is set.

Authentication uses HTTP Basic Auth; the API Key + Secret pair takes precedence over the Auth Token when both are provided.

### Step 2: Create an asset file for data ingestion

To ingest data from Twilio, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., twilio_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.twilio
type: ingestr
connection: postgres

parameters:
  source_connection: my_twilio
  source_table: 'messages'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Twilio.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Twilio connection defined in .bruin.yml.
- `source_table`: The name of the data table in Twilio you want to ingest. For example, `messages` would ingest message records.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `messages` | sid | - | replace | SMS and MMS messages. Fully replaced each run because redactions and deletions can't be detected incrementally. |
| `calls` | sid | date_updated | merge | Voice calls. Lifecycle changes (status/duration/end_time) bump `date_updated`. |
| `recordings` | sid | date_updated | merge | Call recordings, including deleted ones (soft-deletes surface as `status=deleted`). |
| `incoming_phone_numbers` | sid | - | replace | Phone numbers owned by the account. |
| `usage_records` | - | - | replace | Usage totals per category over the account lifetime. Accepts an optional `:daily`, `:monthly`, or `:yearly` granularity suffix (e.g. `usage_records:daily`) that loads incrementally by period. |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.twilio.asset.yml
```

As a result of this command, Bruin will ingest data from the given Twilio table into your Postgres database.
