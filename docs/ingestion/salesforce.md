# Salesforce

[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform. Salesforce can also be a [reverse-ETL destination](#salesforce-as-a-destination), to write data back into Salesforce.

Bruin's Salesforce ingestr connection supports three credential forms:

- **Username, password, and security token**: recommended for scheduled runs when Salesforce SOAP API login is enabled.
- **OAuth 2.0 client credentials**: use a Connected App that allows the client credentials flow. ingestr exchanges `client_id` and `client_secret` for an access token.
- **Static access token**: useful when SOAP API login is unavailable or username/password auth is blocked. Bruin does not refresh expired static Salesforce access tokens.

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

If this option is unavailable, use OAuth 2.0 client credentials or a static access token instead.

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

For OAuth 2.0 client credentials:

```yaml
connections:
  salesforce:
    - name: "salesforce"
      client_id: "your_connected_app_consumer_key"
      client_secret: "your_connected_app_consumer_secret"
      domain: "your-domain.my.salesforce.com"
```

- `client_id` is the Connected App consumer key.
- `client_secret` is the Connected App consumer secret.
- `domain` is your Salesforce domain. You can pass either the host or the full URL.

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

When multiple credential forms are set, `access_token` takes precedence, then `client_id`/`client_secret`, then `username`/`password`/`token`.

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

For OAuth 2.0 client credentials, use `client_id`, `client_secret`, and `domain` in `.bruin.yml`.

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

## Salesforce as a destination

Bruin can also write records **into** Salesforce ([reverse ETL](/ingestion/reverse-etl)). Each source row creates, updates or deletes one Salesforce record. Reuse the same `salesforce` connection as the source. The user it logs in with needs **Create**, **Edit** and, for `delete`/`replace`, **Delete** permission on the object, plus **Edit** access to every field you write.

Set the destination with three parameters:

- `destination: salesforce`
- `destination_connection`: your Salesforce connection name
- `destination_table: '<object>?external_id=<field>&load_method=<load_method>'`
  - `<object>`: the Salesforce object to write to, e.g. `Contact`. See [Objects and fields](#objects-and-fields).
  - `external_id`: the field used to find existing records. Needed for `merge` and `replace`. See [Matching records](#matching-records).
  - `load_method` *(optional)*: `bulk` (default) or `rest`. See [Load method](#load-method).

`incremental_strategy` is **required**. Salesforce has no default.

### Example: upsert contacts by an External ID

```yaml
name: sync_contacts_to_salesforce
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.customers'

  destination: salesforce
  destination_connection: my-salesforce
  destination_table: 'Contact?external_id=External_Id__c'
  incremental_strategy: merge

columns:
  - name: External_Id__c
    primary_key: true
```

For every row, this finds the Contact whose `External_Id__c` equals the row's `External_Id__c` column: if it exists it is updated, otherwise it is created. The other source columns (`FirstName`, `Email`, …) are written to the Contact fields of the same name.

### Objects and fields

- The object is its **API name**: `Contact`, `Account`, `Opportunity`, or a custom object such as `Invoice__c`.
- Each source column is written to the field with the same **API name**, e.g. `FirstName` or `Amount__c`. Names are not case-sensitive.
- Find API names in Setup → **Object Manager**. Labels don't work:

| | Label | API name to use |
|---|---|---|
| Custom object | Invoice | `Invoice__c` |
| Custom field | Amount | `Amount__c` |
| Object from a managed package | Invoice (package `acme`) | `acme__Invoice__c` |

- Bruin doesn't create objects or fields. A source column that isn't a field, or is read-only (a formula, or a system field like `CreatedDate`), stops the run **before anything is written**. Drop such columns, for example with `sql_exclude_columns`.
- A source column named `Id` is only used to find records, never written. The `_ingestr_loaded_at` and `_ingestr_run_id` columns are never sent.

### Strategies

| Strategy | What it does | What it needs |
|---|---|---|
| `merge` | Updates the matching record, or creates it if there is none | `external_id=` and a `primary_key` column |
| `update` | Updates matching records only; a row with no match is rejected | nothing when matching by record `Id` |
| `append` | Always creates new records | nothing |
| `delete` | Deletes matching records | nothing when matching by record `Id` |
| `replace` | Like `merge`, then deletes every record that isn't in the source | `external_id=` and a `primary_key` column |

- **`append`** never matches, so re-running it creates duplicates, unless a unique field on the object refuses them (`DUPLICATE_VALUE`). `external_id=` isn't allowed with it; a `primary_key` column is accepted and only names rejected rows in the report.
- **`delete`** moves records to the Recycle Bin, where they can be restored for 15 days.

> [!WARNING]
> `replace` deletes **every** record of the object that isn't in your source, including ones created in Salesforce or by other tools. Use it only when your source is the complete list. As a safety net, a run with **0 source rows** deletes nothing, and with the default `reject_mode: fail` a run with any rejected row deletes nothing either.

### Matching records

Two settings decide which record a row updates or deletes:

| Setting | Set with | Example |
|---|---|---|
| The **Salesforce field** to match on | `external_id=` in `destination_table` | `Contact?external_id=External_Id__c` |
| The **source column** that holds the value | `primary_key: true` on the column | `- name: External_Id__c` |

**`merge` and `replace`** need both. The field must be marked **External ID** in Salesforce, or be a standard lookup field such as a Contact's or Lead's `Email`. Standard objects have no External ID field by default, so create one first: Setup → Object Manager → *object* → Fields & Relationships → New, and tick **External ID**. The record `Id` can't be used, because Salesforce can't create a record with an id you choose.

**`update` and `delete`** match by the Salesforce record id by default, using a source column named `Id`:

```yaml
parameters:
  source_connection: my-postgres
  source_table: 'public.churned_contacts'   # has an Id column
  destination: salesforce
  destination_connection: my-salesforce
  destination_table: 'Contact'
  incremental_strategy: delete
```

They can also match on any other text, number or id field with `external_id=`, even one that isn't unique. Then **every** matching record is updated or deleted.

### Linking records

A record points to its parent through a **lookup field**. A Contact's Account, for example, is stored in the Contact's `AccountId` field. Write it like any other column, in one of two ways:

| You have | Column name | Example value |
|---|---|---|
| The parent's Salesforce record id | The lookup field, e.g. `AccountId` | `001WU00002EGrguYAD` |
| Your own key for the parent | `<Relationship>.<Field>`, e.g. `Account.Ext_Id__c` | `ACC-1` |

- The **relationship** is usually the lookup field without `Id`: `AccountId` → `Account`, `OwnerId` → `Owner`. For a custom lookup, replace `__c` with `__r`: `Parent__c` → `Parent__r`.
- The **field** must identify one parent: a field marked External ID, or a unique field such as a user's email (`Owner.Email`).
- An empty (null) value removes the link. Set `write_nulls: false` to leave existing links alone.
- Lookups that can point to more than one object, like a Task's `Who` (a Contact or a Lead), need the object in the middle: `Who.Contact.Ext_Id__c`. `Owner.Email` always means a User.
- Many-to-many links, such as `OpportunityContactRole` or `CampaignMember`, are records of their own. Write them as their own object, with one lookup column per side.

### Load method

- **`bulk`** *(default)*: uses Salesforce's Bulk API. It uses far fewer API calls than `rest` on large tables, and reports rejected rows when the load finishes.
- **`rest`**: writes records in small batches and reports results right away, but uses more API calls on large tables.

```yaml
destination_table: 'Contact?external_id=External_Id__c&load_method=rest'
```

Use `rest` if you need `reject_mode: fail_fast`, upload files (such as `ContentVersion.VersionData`), or write text that is exactly `#N/A`, which bulk reads as an empty value. Every strategy works the same with both.

### Run options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reject_mode` | `fail` | How to handle a row Salesforce refuses, or that matches no record. `fail`: write the valid rows, then error with the reject list. `fail_fast`: stop at the first bad row; needs `load_method=rest`. `skip`: write the valid rows, report the rejects, and still succeed. |
| `write_nulls` | `true` | Whether a source NULL clears the Salesforce field. `false` leaves the existing value untouched. No effect on `delete`. |

Problems that affect the whole run, such as an expired login, always stop it, whatever the `reject_mode`. Salesforce writes can't be rolled back, so with `fail` or `fail_fast` some records may already be written when the run stops.

### Column name mapping

When a source column name differs from the Salesforce field, set `source_column` on the column entry: `name` is the field's **API name**, `source_column` is the source column. This requires `enforce_schema: "true"`, and you must **omit `type`**, since Salesforce owns the field types:

```yaml
parameters:
  # ...
  enforce_schema: "true"

columns:
  - name: FirstName
    source_column: first_name
  - name: Account.Ext_Id__c
    source_column: account_code
```

A `primary_key` column uses its Salesforce name (`name`), not the source name.

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
