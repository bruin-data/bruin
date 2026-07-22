# Salesforce

[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform.

Bruin's Salesforce ingestr connection supports two credential forms:

- **Username, password, and security token**: recommended for scheduled runs when Salesforce SOAP API login is enabled.
- **Static access token**: useful when SOAP API login is unavailable or username/password auth is blocked. Bruin does not run a Salesforce OAuth flow or refresh expired Salesforce access tokens for this source.

Follow the steps below to set up Salesforce correctly as a data source and run ingestion.

## Configuration

### Step 1: Confirm Salesforce user access

Use a Salesforce user that has:

- API access
- Access to the Salesforce objects you want to ingest
- Permission to log in through API/SOAP if using username/password/security-token auth
- **Read** field-level security (FLS) on every field you want to ingest

> [!WARNING]
> ingestr only ingests the fields the connecting user is permitted to read. If you want a field to be ingested, make sure the user has Read access to it, through its profile or a permission set. If the user does not have permission to a field, Salesforce does not return it and ingestr does not fetch it.

Common Salesforce objects include:

- Account
- Contact
- Opportunity
- Task

### Step 2: Enable SOAP API login

This is required for username/password/security-token auth.

In Salesforce:

1. Open **Setup**.
2. Search for **User Interface**.
3. Open **User Interface**.
4. Scroll to **API Settings**.
5. Enable **Enable SOAP API Login**.
6. Save.

If this option is unavailable, use static access-token auth instead.

### Step 3: Get the Salesforce security token

Log in as the Salesforce user Bruin should use.

1. Click the user avatar.
2. Open **Settings**.
3. Search for **Reset My Security Token**.
4. Click **Reset My Security Token**.
5. Check the Salesforce user's email for the new token.

The security token is case-sensitive.

### Step 4: Add a connection to the .bruin.yml file

Use the username/password/security-token combination when SOAP API login is enabled:

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
- `token` is your Salesforce security token. Do not append the security token to the password.
- `domain` is your Salesforce domain. You can pass either the host, such as `your-domain.my.salesforce.com`, or the full URL, such as `https://your-domain.my.salesforce.com`. For sandboxes, use the sandbox My Domain URL.

If SOAP API login is not enabled or username/password auth is blocked, use a static access token:

```yaml
connections:
  salesforce:
    - name: "salesforce"
      access_token: "00D...!AQ...your_salesforce_access_token"
      domain: "your-domain.my.salesforce.com"
```

- `access_token` must be valid for the Salesforce org and user.
- Bruin does not refresh this token when it expires.
- `domain` is the Salesforce instance domain for the same org as the token. You can pass either the host or the full URL.

When `access_token` is set it takes precedence over `username`/`password`/`token`.

Do not commit `.bruin.yml` if it contains Salesforce credentials.

### Step 5: Create a Bruin Cloud connection

In Bruin Cloud:

1. Open the team or project.
2. Go to **Connections**.
3. Click **New connection**.
4. Choose **Salesforce**.
5. Set the connection name to match the pipeline asset config.

For username/password/security-token auth:

```text
Access Token (OAuth): leave blank
Username: <salesforce-username>
Password: <salesforce-password>
Security Token: <salesforce-security-token>
Domain: https://your-domain.my.salesforce.com
```

Do not append the security token to the password if Bruin Cloud has a separate **Security Token** field.

For static access-token auth:

```text
Access Token (OAuth): <salesforce-access-token>
Username: leave blank
Password: leave blank
Security Token: leave blank
Domain: https://your-domain.my.salesforce.com
```

The Bruin Cloud connection name must exactly match the asset's `source_connection` value. For example, if an asset uses:

```yaml
parameters:
  source_connection: salesforce
```

then the Bruin Cloud connection name must be:

```text
salesforce
```

### Step 6: Create an asset file for data ingestion

To ingest data from Salesforce, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file, such as `salesforce_ingestion.yml`, inside the assets folder and add the following content:

```yaml
name: public.salesforce
type: ingestr
connection: postgres

parameters:
  source_connection: salesforce
  source_table: "account"

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For Salesforce, it will be always `ingestr`.
- `source_connection`: The name of the Salesforce connection defined in `.bruin.yml` or Bruin Cloud.
- `source_table`: The name of the table in Salesforce to ingest.
- `destination`: The destination platform/type, for example `postgres`.

Before running a full pipeline, test with one Salesforce ingestion asset.

Expected logs include:

```text
Source: salesforce / account
Fetched Account using Bulk API
Ingestion completed successfully
```

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
| `custom:<custom_object_name>` | - | - | replace | Track and store data that's unique to your organization. |

### Step 7: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/salesforce_asset.yml
```

As a result of this command, Bruin will ingest data from the given Salesforce table into your destination database.

## Troubleshooting

### `SOAP API login() is disabled by default in this org`

The Salesforce org blocks username/password/security-token auth.

Fix one of the following:

- Enable **Enable SOAP API Login** in Salesforce **User Interface** settings.
- Or use static access-token auth instead.

### `INVALID_LOGIN`

Salesforce rejected the login.

Check:

- Username is correct.
- Password is correct.
- Security token is correct and current.
- User is not locked out.
- User has API access.
- SOAP API login is enabled.
- Domain points to the correct Salesforce org.

### `INVALID_SESSION_ID`

The static access token expired or is invalid.

Fix one of the following:

- Generate a fresh Salesforce access token.
- Update the Bruin Salesforce connection.
- For scheduled runs, prefer username/password/security-token auth if SOAP API login is enabled.

### Connection name mismatch

If the asset uses:

```yaml
source_connection: salesforce
```

but the Bruin Cloud connection is named something else, the run will fail.

Fix one of the following:

- Rename the Bruin Cloud connection to match the asset.
- Or update the asset to reference the Bruin Cloud connection name.

### Some fields are not ingested

A field is not ingested even though it has data in Salesforce, and the run reports no error.

ingestr only fetches the fields the connecting user is permitted to read (its query is built from what Salesforce returns for that user). A common reason a field is skipped is that the user lacks Read access to it, for example due to field-level security (FLS).

To ingest a field that is missing:

- Make sure the Salesforce user has **Read** access to it, through its profile or an assigned permission set.
- You can check which fields the user can see by describing the object as that user (for example `GET /services/data/v59.0/sobjects/<object>/describe`). Only the fields listed in the response are ingested.
- Run the asset again.
