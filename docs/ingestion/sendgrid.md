# SendGrid

[SendGrid](https://sendgrid.com/) is Twilio's email delivery and marketing platform.

Bruin supports SendGrid as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SendGrid into your data warehouse.

To set up a SendGrid connection, you need to have a SendGrid API key. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/sendgrid.html)

Follow the steps below to correctly set up SendGrid as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to SendGrid, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      sendgrid:
        - name: "my_sendgrid"
          api_key: "YOUR_SENDGRID_API_KEY"
```

- `api_key`: The API key used for authentication with the SendGrid v3 API.
- `on_behalf_of` (optional): The subuser username to query on behalf of, when using a parent account to access a subuser's data.

### Step 2: Create an asset file for data ingestion

To ingest data from SendGrid, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sendgrid_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sendgrid
type: ingestr
connection: postgres

parameters:
  source_connection: my_sendgrid
  source_table: 'bounces'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for SendGrid.
- `connection`: This is the destination connection.
- `source_connection`: The name of the SendGrid connection defined in .bruin.yml.
- `source_table`: The name of the data table in SendGrid you want to ingest. For example, `bounces` would ingest bounced email records.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| messages | msg_id | last_event_time | merge | Email Activity feed. Requires the Email Activity add-on. |
| global_stats | date | date | merge | Global email statistics. Requires `--interval-start`. Use a `global_stats:week` or `global_stats:month` suffix for weekly/monthly grain. |
| bounces | email, created | created | merge | Bounced addresses. |
| blocks | email, created | created | merge | Blocked addresses. |
| invalid_emails | email, created | created | merge | Invalid addresses. |
| spam_reports | email, created | created | merge | Spam-report suppressions. |
| unsubscribes | email, created | created | merge | Global unsubscribe list. |
| suppression_groups | id | – | replace | Unsubscribe (suppression) groups. |
| suppression_group_members | group_id, email | – | replace | Suppressed addresses per group. |
| templates | id | updated_at | merge | Transactional templates (legacy + dynamic). |
| lists | id | – | replace | Marketing contact lists. |
| single_sends | id | updated_at | merge | Marketing single sends. |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.sendgrid.asset.yml
```

As a result of this command, Bruin will ingest data from the given SendGrid table into your Postgres database.
