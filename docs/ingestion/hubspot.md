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

Bruin can also write CRM records and record-to-record associations **into** HubSpot (reverse ETL). Each source row becomes one HubSpot record (or one association link), sent in bulk through HubSpot's batch APIs. Reuse the same `hubspot` connection as the source; the token needs **write** scopes on the objects you target (e.g. `crm.objects.contacts.write`), plus the schema read scope for custom objects.

Set the destination with three parameters:

- `destination: hubspot`
- `destination_connection`: your HubSpot connection name
- `destination_table: '<object>?<params>'` — the object type before the `?`, and matching parameters after it.

The write behaviour is chosen with `incremental_strategy`, which is **required** — HubSpot has no default. Every source column is written to the HubSpot property whose **internal name** matches the column name (e.g. `numberofemployees`, never the display label "Number of Employees"); rename a column with [`source_column`](#column-name-mapping). The metadata columns ingestr adds to every load (`_ingestr_loaded_at`, `_ingestr_run_id`) are never sent as properties.

### Strategies

| Strategy | Behaviour |
| -------- | --------- |
| `merge` | Upsert — update the matching record, or create it if none matches. The usual choice. |
| `update` | Update matching records only; rows with no match are rejected, never created (honoring `reject_mode`). |
| `append` | Always create a new record, never match. Re-running the same rows creates duplicates — scope each run with an incremental key, or use `merge` to stay idempotent. |
| `delete` | Archive (soft-delete) the matching records. Rows with no match honor `reject_mode`. |
| `replace` | Mirror — upsert every source row, then archive any record of that object type whose match value is **not** in the source. |

> [!WARNING]
> `replace` is a full object-wide mirror: it archives **every** record of the object type that is not in your source — including records created in the HubSpot UI or by other integrations. Use it only when the source is the complete, sole system of record for that object. It also scans every record of the object on each run, so it is slow and costly on large objects. A run with **0 source rows** archives nothing; to remove records intentionally, use `delete`.

### Matching records

Upsert/update/delete match on two independent things — **which HubSpot property** to match on, and **which source column** carries the value:

- **Match property** — `id_property=<property>` on `destination_table`, always the property's **internal name**. `merge` and `replace` require it explicitly, and it must be a **unique property** — not `hs_object_id` (HubSpot has no upsert-by-record-id; use `update` to match by id). `update` and `delete` default it to `hs_object_id`.
- **Source column** — the column carrying the match value, marked `primary_key: true` in the asset's `columns`. `update` and `delete` default to the `hs_object_id` column when none is marked.

For `update` and `delete`, the match property need not be unique: if it is non-unique (e.g. `company_name`), every record with that value is updated/archived. (`merge`/`replace` still require a unique property, since upsert/mirror target a single record.)

> [!WARNING]
> Matching on a non-unique property is much slower: each value is resolved through HubSpot's Search API (which is rate-limited) instead of a direct batch lookup, so large runs take considerably longer. Prefer a unique property, or `hs_object_id`, when you can.

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
    primary_key: true
```

### Run options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reject_mode` | `fail` | How to handle a row HubSpot can't apply. `fail`: write the valid rows, then error with the reject list. `fail_fast`: stop at the first bad row (rows after it aren't written). `skip`: write the valid rows, report the rejects, and still succeed. |
| `write_nulls` | `true` | Whether a source NULL clears the HubSpot field. `true` writes it through as empty (clearing the field); `false` omits the column, leaving the existing value untouched. Only affects property-writing strategies (`merge`/`update`/`replace`/`append`), not `delete` or associations. |

Only **per-record** problems count as rejects that `skip` tolerates (no matching record, or a value HubSpot rejects such as a validation error or unique-property conflict); **systemic failures always abort the run** regardless of `reject_mode`. HubSpot's batch writes are not transactional, so under `fail`/`fail_fast` some records may already be written before the run stops.

### Column name mapping

When a source column name differs from the HubSpot property name, set `source_column` on the column entry: `name` is the destination property's **internal name**, `source_column` is the source column. This requires `enforce_schema: "true"`, and — because HubSpot fixes property types on its side — you must **omit `type`** on renamed columns (an entry carrying a type is rejected):

```yaml
parameters:
  # ...
  enforce_schema: "true"

columns:
  - name: firstname          # HubSpot property internal name
    source_column: first_name
  - name: hs_lead_status
    source_column: status
```

The property must already exist in HubSpot — an override can't create one, and naming a source column that doesn't exist fails fast.

### Properties must already exist

HubSpot does **not** create properties on the fly. Every column you write must map to a property that already exists on the object (built-in, or one you created in HubSpot beforehand). A row referencing an unknown property is rejected by HubSpot and handled per `reject_mode`.

### Multi-select properties

For multi-select (checkbox) properties HubSpot uses a semicolon-separated list of option values, sent through verbatim:

- `"CHAMPION;DECISION_MAKER"` sets exactly those two options (replacing any current selection).
- A **leading** semicolon appends instead of replacing: `";BLOCKER"` adds `BLOCKER` to whatever is already selected.

### Associations

Link two objects by naming both sides as `obj1+obj2` and marking two `primary_key: true` columns — **the first primary-key column is the `obj1` (from) key, the second is the `obj2` (to) key**. The order follows the column declaration order in the asset, so declare the from-side key first; a wrong order silently swaps the sides.

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
  - name: email          # obj1 (contacts) key — declared first
    primary_key: true
  - name: domain         # obj2 (companies) key
    primary_key: true
```

Optional `destination_table` parameters:

- `id_property=fromProp,toProp` — the property each side matches on, positionally (empty = the value is already a HubSpot record id, e.g. `id_property=email,`).
- `label=<name>` — apply a named association label, resolved to its type id.
- `association_type=<id>` — the numeric association type id, as an alternative to `label`.
- `association_category=<cat>` — category for a numeric `association_type` (e.g. `HUBSPOT_DEFINED`, `USER_DEFINED`); resolved automatically when omitted.

A plain association (no `label`/`association_type`) uses HubSpot's **default** association — you don't need to provide a type.

Behaviour:

- Match values are resolved to record ids before linking; a non-empty key matching no record is a not-found reject (per `reject_mode`). An empty cell is skipped for `merge`/`delete`; under `replace`, an empty `obj2` cell for a present `obj1` clears that record's links.
- A key column holding a list/array links the row to every element (one contact to many companies in a single row); pairs repeated within a row are de-duplicated, and re-linking the same pair is harmless (idempotent).
- For `delete`/`replace`, a `label`/`association_type` **scopes the unlink to that one type** — other labels between the pair survive. Without one, the unlink removes **every** type between the two records.

Association strategies:

| Strategy | Behaviour |
| -------- | --------- |
| `merge` | Add the links in the source. |
| `delete` | Remove (unlink) the links in the source. |
| `replace` | Mirror — make each `obj1` record's links exactly match the source; an `obj1` row with an empty `obj2` cell removes all of that record's links. |

Only `merge`, `delete`, and `replace` are supported for associations; `update` and `append` fail fast.

### Custom objects

Use a custom object anywhere a built-in object is accepted (records and associations). Address it by its **internal name** (`license`), **fully-qualified name** (`p123_license`), or **objectTypeId** (`2-123`) — not its display label (labels are not guaranteed unique). Everything else — strategies, matching, `reject_mode`, associations — works exactly as for built-in objects:

```yaml
parameters:
  source_connection: my-postgres
  source_table: 'public.licenses'

  destination: hubspot
  destination_connection: my-hubspot
  destination_table: 'license?id_property=sku'
  incremental_strategy: merge

columns:
  - name: sku
    primary_key: true
```

> [!NOTE]
> For the full destination reference, see the [ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/hubspot.html).
