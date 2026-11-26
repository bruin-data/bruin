# Brevo

[Brevo](https://www.brevo.com/) (formerly Sendinblue) is a marketing platform for email, SMS, and WhatsApp campaigns, transactional messaging, CRM, and ecommerce.

Bruin supports Brevo as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Brevo into your data warehouse.

In order to set up a Brevo connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Brevo as a data source and run ingestion.

## Configuration

### Step 1: Get an API key

1. Log in to your Brevo account.
2. Open **SMTP & API** → **API Keys** in your account settings.
3. Click **Generate a new API key** and copy it. Brevo v3 API keys start with `xkeysib-`.

### Step 2: Add a connection to .bruin.yml file

To connect to Brevo, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      brevo:
        - name: "brevo"
          api_key: "xkeysib-..."
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | - | The name to identify this Brevo connection. |
| `api_key` | Yes | - | A Brevo API key (v3). |

Bruin URL-encodes the key, so keys containing special characters can be used as-is.

### Step 3: Create an asset file for data ingestion

To ingest data from Brevo, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., brevo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.brevo_contacts
type: ingestr
connection: postgres

parameters:
  source_connection: brevo
  source_table: 'contacts'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Brevo connection defined in .bruin.yml.
- `source_table`: The name of the table to read. See [Source tables](#source-tables).

Event tables replace whole days, so an asset that loads transactional email events looks like this:

```yaml
name: public.brevo_transactional_email_events
type: ingestr
connection: postgres

parameters:
  source_connection: brevo
  source_table: 'transactional_email_events'

  destination: postgres
```

### Step 4: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/brevo_ingestion.yml
```

As a result of this command, Bruin will ingest the contacts from Brevo into your Postgres database.

## Source tables

| Source table | Primary key | Incremental key | Strategy | Details |
|--------------|-------------|-----------------|----------|---------|
| `contacts` | `id` | `modifiedAt` | merge | All contacts with their attributes, list memberships, and blocklist flags. Incremental runs only fetch contacts modified since the interval start. |
| `lists` | `id` | - | replace | Contact lists. |
| `segments` | `id` | - | replace | Contact segments. |
| `folders` | `id` | - | replace | Folders that group contact lists. |
| `contact_attributes` | `category`, `name` | - | replace | Contact attribute definitions, including the values of category attributes. |
| `email_campaigns` | `id` | - | replace | Email campaigns of every status, including archived ones (`status` is `archive`), with their global statistics. HTML content is excluded. |
| `sms_campaigns` | `id` | - | replace | SMS campaigns of every status, including archived ones. |
| `email_templates` | `id` | - | replace | Transactional email templates. |
| `transactional_email_events` | - | `event_date` | delete+insert | One row per transactional email event (request, delivered, opened, click, bounce, and so on). `event_date` is the day of the event's `date`. |
| `transactional_email_reports` | - | `date` | delete+insert | Daily totals for transactional email. |
| `blocked_contacts` | `email` | - | replace | Contacts blocked or unsubscribed from transactional email, with the reason. |
| `companies` | `id` | `modifiedAt` | merge | CRM companies with their attributes and linked contacts and deals. `modifiedAt` is copied from the `last_updated_at` attribute. |
| `deals` | `id` | `modifiedAt` | merge | CRM deals with their attributes and links. `modifiedAt` is copied from the `last_updated_date` attribute. |
| `crm_tasks` | `id` | - | replace | CRM tasks. |
| `crm_notes` | `id` | - | replace | CRM notes. |
| `orders` | `id` | `updatedAt` | merge | Ecommerce orders. Brevo keeps every update of an order; the table keeps the latest one. |
| `products` | `id` | `modifiedAt` | merge | Ecommerce products, including deleted ones (`isDeleted` is true). |
| `categories` | `id` | `modifiedAt` | merge | Ecommerce categories, including deleted ones (`isDeleted` is true). |
| `sms_events` | - | `event_date` | delete+insert | One row per transactional SMS event. `event_date` is the day of the event's `date`. |
| `whatsapp_events` | - | `event_date` | delete+insert | One row per WhatsApp event. `event_date` is the day of the event's `date`. |
| `whatsapp_campaigns` | `id` | - | replace | WhatsApp campaigns. |
| `custom_events` | - | `event_date` | delete+insert | Custom events tracked for contacts, with their properties. |

## Incremental loading

- `contacts`, `companies`, `deals`, `orders`, `products`, and `categories` merge on `id` and only fetch records modified since the start of the run's date range.
- `transactional_email_events`, `transactional_email_reports`, `sms_events`, and `whatsapp_events` are loaded in whole days: each run replaces the days from the start date through the end date, inclusive. Without a date range, they load the last 90 days.
- `custom_events` replaces the events in the run's date range by `event_date`.
- A [`--full-refresh`](/commands/run) without a date range keeps only the last 90 days of the event and report tables, and only the last 6 months of `custom_events`.
- All other tables are fully replaced on every run and ignore the date range.

## Notes

- `event_date` is the event's day in the timezone set in your Brevo account.
- Brevo returns `createdAt` and `modifiedAt` of email templates in Central European Time but marks them as UTC, so they can be an hour or two ahead.
- The ecommerce tables need ecommerce enabled in Brevo, and the SMS and WhatsApp tables need those channels on your plan.
- Deleted contacts are not returned by Brevo, so they stay in the destination `contacts` table.
