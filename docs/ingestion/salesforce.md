# Salesforce

[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform.

To set up a Salesforce connection, you must add a configuration item in the `.bruin.yml` and `asset` file. Salesforce supports two authentication methods:

- **Username, password, and security token**: classic SOAP-style login. You can obtain your security token by logging into your Salesforce account and navigating to the user settings under "Reset My Security Token."
- **OAuth access token**: an access token minted by the Salesforce CLI (`sf org auth show-access-token`). Use this when you want to authenticate interactively in a browser.

Follow the steps below to set up Salesforce correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

Use either the username/password/token combination:

```yaml
connections:
      salesforce:
            - name: "salesforce"
              username: "user_123"
              password: "pass_123"
              token: "token_123"
              domain: "your-domain.my.salesforce.com"
```

- `username` is your Salesforce account username.
- `password` is your Salesforce account password.
- `token` is your Salesforce security token.
- `domain` is your Salesforce domain (e.g., "your-company.my.salesforce.com").

Or an OAuth access token:

```yaml
connections:
      salesforce:
            - name: "salesforce"
              access_token: "00D...!AQ...your_oauth_access_token"
              domain: "your-domain.my.salesforce.com"
```

- `access_token` is an OAuth access token for your Salesforce org. You can obtain one with `sf org auth show-access-token --target-org <salesforce-username>` after logging in via `sf org login web`.
- `domain` is your Salesforce instance domain — same field as above. You can pass it either as a host (e.g. `your-domain.my.salesforce.com`) or as a full URL (e.g. `https://your-domain.my.salesforce.com`); both are accepted. For sandboxes, use the sandbox My Domain URL.

When `access_token` is set it takes precedence over `username`/`password`/`token`.

### Step 2: Create an asset file for data ingestion

To ingest data from Salesforce, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., salesforce_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.salesforce
type: ingestr
connection: postgres

parameters:
  source_connection: salesforce
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Salesforce, it will be always `ingestr`.
- `source_connection`: The name of the Salesforce connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Salesforce to ingest.
- `destination`: The name of the destination connection.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `user` | - | - | replace | Refers to an individual who has access to a Salesforce org or instance. |
| `user_role` | - | - | replace | A standard object that represents a role within the organization's hierarchy. |
| `opportunity` | id | last_timestamp | merge | Represents a sales opportunity for a specific account or contact. |
| `opportunity_line_item` | id | last_timestamp | merge | Represents individual line items or products associated with an Opportunity. |
| `opportunity_contact_role` | id | last_timestamp | merge | Represents the association between an Opportunity and a Contact. |
| `account` | id | last_timestamp | merge | Individual or organization that interacts with your business. |
| `contact` | id | - | replace | An individual person associated with an account or organization. |
| `lead` | id | - | replace | Prospective customer/individual/org. that has shown interest in a company's products/services. |
| `campaign` | id | - | replace | Marketing initiative or project designed to achieve specific goals, such as generating leads. |
| `campaign_member` | id | last_timestamp | merge | Association between a Contact or Lead and a Campaign. |
| `product` | id | - | replace | For managing and organizing your product-related data within the Salesforce ecosystem. |
| `pricebook` | id | - | replace | Used to manage product pricing and create price books. |
| `pricebook_entry` | id | - | replace | Represents a specific price for a product in a price book. |
| `task` | id | last_timestamp | merge | Used to track and manage various activities and tasks within the Salesforce platform. |
| `event` | id | last_timestamp | merge | Used to track and manage calendar-based events, such as meetings, appointments, or calls. |
| `custom:<custom_object_name>` | - | - | replace | Track and store data that's unique to your organization. For more information about custom objects in Salesforce, read `here`. |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/salesforce_asset.yml
```

As a result of this command, Bruin will ingest data from the given salesforce table into your Postgres database.
