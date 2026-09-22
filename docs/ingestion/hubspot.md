# HubSpot

[HubSpot](https://www.hubspot.com/) is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as both a source and a destination for [Ingestr assets](/assets/ingestr): you can ingest data from HubSpot into your data warehouse, and you can write CRM records and record-to-record associations from your warehouse back into HubSpot (reverse ETL).

In order to set up HubSpot connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You will need an `api_key`, which can be either a **private app access token** or a HubSpot **service key** — both are sent to HubSpot as a bearer token, so either works.

- For a **service key**, go to Settings → Integrations → Service Keys in your HubSpot account, create a key, grant it the required scopes, and copy the generated key. HubSpot has moved private apps to "legacy apps" and now recommends service keys for system-to-system data integrations.
- For a **private app access token**, go to Settings → Integrations → Private Apps, create a private app, grant it the required scopes, and copy the generated access token.

> **Note:** HubSpot service keys are currently in **public beta**. The feature may change before general availability, so keep that in mind before relying on it for production workloads.

Follow the steps below to correctly set up HubSpot as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to HubSpot, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      hubspot:
        - name: "my-hubspot"
          api_key: "pat-123"
```

- `name`: The name of the connection
- `api_key`: The credential used for authentication with the HubSpot API. Accepts either a private app access token or a HubSpot service key.

### Step 2: Create an asset file for data ingestion

To ingest data from HubSpot, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubspot_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hubspot
type: ingestr
connection: postgres

parameters:
  source_connection: my-hubspot
  source_table: 'companies'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the hubspot connection defined in .bruin.yml.
- `source_table`: The name of the data table in hubspot that you want to ingest. For example, `companies` is a data table in hubspot that you may want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| --------------- | ----------- | --------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `companies` | hs_object_id | hs_lastmodifieddate | merge | Retrieves information about organizations. |
| `contacts` | hs_object_id | lastmodifieddate | merge | Retrieves information about visitors, potential customers, and leads. |
| `deals` | hs_object_id | hs_lastmodifieddate | merge | Retrieves deal records and tracks deal progress. |
| `tickets` | hs_object_id | hs_lastmodifieddate | merge | Handles requests for help from customers or users. |
| `products` | hs_object_id | hs_lastmodifieddate | merge | Retrieves pricing information of products. |
| `quotes` | hs_object_id | hs_lastmodifieddate | merge | Retrieves price proposals that salespeople can create and send to their contacts. |
| `calls` | hs_object_id | hs_lastmodifieddate | merge | Retrieves call engagement records. |
| `emails` | hs_object_id | hs_lastmodifieddate | merge | Retrieves email engagement records. |
| `feedback_submissions` | hs_object_id | hs_lastmodifieddate | merge | Retrieves customer feedback survey responses. |
| `line_items` | hs_object_id | hs_lastmodifieddate | merge | Retrieves individual products or services associated with deals. |
| `meetings` | hs_object_id | hs_lastmodifieddate | merge | Retrieves meeting engagement records. |
| `notes` | hs_object_id | hs_lastmodifieddate | merge | Retrieves note engagement records. |
| `tasks` | hs_object_id | hs_lastmodifieddate | merge | Retrieves task engagement records. |
| `carts` | hs_object_id | hs_lastmodifieddate | merge | Retrieves shopping cart records. |
| `discounts` | hs_object_id | hs_lastmodifieddate | merge | Retrieves discount records. |
| `fees` | hs_object_id | hs_lastmodifieddate | merge | Retrieves fee records. |
| `invoices` | hs_object_id | hs_lastmodifieddate | merge | Retrieves invoice records. |
| `commerce_payments` | hs_object_id | hs_lastmodifieddate | merge | Retrieves commerce payment records. |
| `taxes` | hs_object_id | hs_lastmodifieddate | merge | Retrieves tax records. |
| `owners` | id | – | merge | Retrieves HubSpot users who can be assigned to CRM records. |
| `schemas` | id | – | merge | Returns all object schemas that have been defined for your account. |
| `pipelines` | object_type, pipeline_id | – | replace | Pipeline definitions across all pipelined object types. One row per pipeline. |
| `pipeline_stages` | object_type, pipeline_id, stage_id | – | replace | Stage definitions for each pipeline. One row per stage. |

### Property History Tables

For every CRM object table listed above, a corresponding **property history** table is available. These tables return one row per property change, enabling you to track how properties changed over time.

Use the format `property_history:<table>` as the `source_table` value. An optional comma-separated list of property names can be appended (`property_history:<table>:<prop1>,<prop2>,...`) to restrict the history to just those properties.

| Table | PK | Inc Key | Inc Strategy | Details |
| ------------------------------------- | ---------------------------------------- | --------- | ------------ | ------------------------------------------------------- |
| `property_history:contacts` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for contacts. |
| `property_history:companies` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for companies. |
| `property_history:deals` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for deals. |
| `property_history:tickets` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for tickets. |
| `property_history:products` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for products. |
| `property_history:quotes` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for quotes. |
| `property_history:calls` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for calls. |
| `property_history:emails` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for emails. |
| `property_history:feedback_submissions` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for feedback submissions. |
| `property_history:line_items` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for line items. |
| `property_history:meetings` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for meetings. |
| `property_history:notes` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for notes. |
| `property_history:tasks` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for tasks. |
| `property_history:carts` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for carts. |
| `property_history:discounts` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for discounts. |
| `property_history:fees` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for fees. |
| `property_history:invoices` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for invoices. |
| `property_history:commerce_payments` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for commerce payments. |
| `property_history:taxes` | hs_object_id, property_name, timestamp | timestamp | merge | Property change history for taxes. |

> **Note:** The `owners` and `schemas` tables do not have history variants. Custom objects also support history via `property_history:custom:<objectType>` (e.g., `property_history:custom:myObject`).

## Overriding associations

Each built-in table fetches a default set of associations. You can override that list by appending `:<assoc1>,<assoc2>` to the table name. The suffix **replaces** the default list, so you can narrow it down to just what you need, or include custom object names. Use `<table>:` (colon with empty list) to skip associations entirely.

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'contacts:companies,deals'   # only fetch companies and deals
  destination: postgres
```

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'contacts:'                  # fetch contacts with no associations
  destination: postgres
```

## Custom Objects

HubSpot allows you to create custom objects to store unique business data that's not covered by the standard objects. ingestr supports ingesting data from custom objects using the following format:

```plaintext
custom:<custom_object_name>
```

or with associations to other objects:

```plaintext
custom:<custom_object_name>:<associations>
```

### Parameters

- `custom_object_name`: The name of your custom object in HubSpot (can be either singular or plural form)
- `associations` (optional): Comma-separated list of object types to include as associations (e.g., `companies,deals,tickets,contacts`)

### Examples

Ingesting a custom object called "licenses":

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'custom:licenses'
  destination: postgres
```

Ingesting a custom object with associations to companies, deals, and contacts:

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'custom:licenses:companies,deals,contacts'
  destination: postgres
```

When you include associations, the response will contain information about the related objects, allowing you to track relationships between your custom objects and standard HubSpot objects.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/hubspot_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given HubSpot table into your Postgres database.

<img width="1124" alt="hubspot" src="https://github.com/user-attachments/assets/c88f2781-1e78-4d5b-8cb1-60b7993ea674">

## HubSpot as a destination

Bruin can also write CRM records and record-to-record associations **into** HubSpot (reverse ETL). Each source row becomes one HubSpot record (or one association link), sent in bulk through HubSpot's batch APIs. Reuse the same connection as the source (the `api_key`).

The `destination_table` parameter selects the object type and, after the `?`, the property to match on; the `incremental_strategy` parameter selects the write behaviour. (These go in `destination_table` rather than the asset `name`, because an asset name may not contain `?`, `=`, or `&`.)

### Strategies

| Strategy | Behaviour |
| -------- | --------- |
| `merge` | Upsert — update the matching record, or create it if none matches. The usual choice. |
| `update` | Update matching records only; rows with no match are rejected, never created. |
| `append` | Always create a new record, never match. Re-running the same rows creates duplicates. |
| `delete` | Archive (soft-delete) the matching records. |
| `replace` | Mirror — upsert every source row, then archive any record of that object type whose match value is **not** in the source. Scans the whole object each run, so it is slow and expensive on large objects. |

### Matching records

Upsert/update/delete match on two things: **which HubSpot property** to match on, and **which source column** carries the value.

- **Match property** — set with `id_property=<property>` on `destination_table`. Use the property's internal name (e.g. `numberofemployees`, not "Number of Employees"). `update` and `delete` default it to `hs_object_id` (the record id); `merge` and `replace` require it explicitly and it must be a **unique property** — not `hs_object_id`, since HubSpot has no upsert-by-record-id (use `update` to match existing records by id).
- **Source column** — the column supplying the match value, marked with `primary_key: true` in the asset's `columns`. `update` and `delete` default to the `hs_object_id` column when none is marked.

### Example: upsert contacts by email

```yaml
name: sync_contacts_to_hubspot
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.marketing_contacts'

  destination: hubspot
  destination_connection: my-hubspot
  destination_table: 'contacts?id_property=email'
  incremental_strategy: merge

columns:
  - name: email
    type: string
    primary_key: true
```

Every other column in the source is written to the HubSpot property of the same internal name. HubSpot does not create properties on the fly — each column must map to a property that already exists on the object.

### Associations

An `obj1+obj2` destination table links records of one object to another (e.g. contacts to companies). Provide two match properties and two source key columns (one per side); `merge` adds links, `delete` unlinks, and `replace` mirrors each From record's links to exactly what the source lists.

```yaml
name: link_contacts_to_companies
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.contact_company_map'

  destination: hubspot
  destination_connection: my-hubspot
  destination_table: 'contacts+companies?id_property=email,domain'
  incremental_strategy: merge

columns:
  - name: email
    type: string
    primary_key: true
  - name: domain
    type: string
    primary_key: true
```

Optional `destination_table` parameters for associations: `label` (a custom association label to add/remove), `association_type`, and `association_category`. An empty side of `id_property` (e.g. `id_property=email,`) means that side's value is already a HubSpot record id.

Custom objects work as a destination too — use the object's internal name (e.g. `license?id_property=sku`).

### Run options

Two optional parameters tune how a reverse-ETL run behaves:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reject_mode` | `fail` | How to handle a row HubSpot can't apply: `fail` writes the valid rows then errors with the reject list, `fail_fast` stops at the first bad row, `skip` writes the valid rows and succeeds while reporting the rejects. |
| `write_nulls` | `true` | Whether a source NULL is written through to **clear** the HubSpot field. `write_nulls: false` omits null columns instead, leaving the existing value untouched (a partial update). |

```yaml
parameters:
  source_connection: my-postgres
  source_table: 'public.marketing_contacts'

  destination: hubspot
  destination_connection: my-hubspot
  destination_table: 'contacts?id_property=email'
  incremental_strategy: merge
  reject_mode: skip
  write_nulls: false
```

> [!NOTE]
> For the full destination reference — every strategy, matching detail, association options, and null-handling — see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/hubspot.html).
