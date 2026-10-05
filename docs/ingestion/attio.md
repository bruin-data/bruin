# Attio

[Attio](https://attio.com/) is an AI-native CRM platform that helps companies build, scale, and grow their business.

Bruin supports Attio as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Attio into your data warehouse. You can also write data back into Attio: see [Attio as a destination](#attio-as-a-destination).

In order to set up Attio connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Attio as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Attio, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  Attio:
    - name: "attio"
      api_key: "key_123"
```

- `api_key`: the API key used for authentication with the Attio API

### Step 2: Create an asset file for data ingestion

To ingest data from Attio, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., Attio_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.attio
type: ingestr
connection: postgres

parameters:
  source_connection: attio
  source_table: 'objects'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Attio connection defined in .bruin.yml.
- `source_table`: The name of the data table in Attio that you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `objects` | - | - | replace | Objects are the data types used to store facts about your customers. Fetches all objects. Full reload on each run. |
| `records:{object_api_slug}` | - | - | replace | Fetches all records of an object. For example: records:companies. Full reload on each run. |
| `lists` | - | - | replace | Fetches all lists. Full reload on each run. |
| `list_entries:{list_id}` | - | - | replace | Lists all items in a specific list. For example: list_entries:8abc-123-456-789d-123. Full reload on each run. |
| `all_list_entries:{object_api_slug}` | - | - | replace | Fetches all the lists for an object, and then fetches all the entries from that list. For example: all_list_entries:companies. Full reload on each run. |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/attio_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Attio table into your Postgres database.

<img alt="Pipedrive" src="./media/attio_ingestion.png">

## Attio as a destination

Bruin can also write records **into** Attio ([reverse ETL](/ingestion/reverse-etl)). Each source row creates, updates or deletes one Attio record. Reuse the same `attio` connection as the source. Its API key needs **Records: Read-write** and **Object Configuration: Read**.

Set the destination with three parameters:

- `destination: attio`
- `destination_connection`: your Attio connection name
- `destination_table: '<object>?matching_attribute=<attribute>'`
  - `<object>`: the object to write to, by its API slug: `people`, `companies`, `deals`, `users`, `workspaces`, or a custom object's slug.
  - `matching_attribute`: the attribute used to find existing records. Needed for `merge` and `replace`. See [Matching records](#matching-records).

`incremental_strategy` is **required**. Attio has no default.

### Example: upsert people by email

```yaml
name: sync_people_to_attio
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.customers'

  destination: attio
  destination_connection: my-attio
  destination_table: 'people?matching_attribute=email_addresses'
  incremental_strategy: merge

columns:
  - name: email_addresses
    primary_key: true
```

For every row, this finds the person whose email address equals the row's `email_addresses` column: if one exists it is updated, otherwise it is created. The other source columns (`job_title`, `description`, …) are written to the attributes with the same slug.

### Objects and attributes

- Each source column is written to the attribute with the same **API slug**, e.g. `job_title` or `email_addresses`. Slugs are not case-sensitive. Find them in Attio under Settings → Objects → *object* → Attributes.
- Bruin doesn't create objects or attributes. A source column that isn't an attribute, or is read-only (such as an enriched system attribute), stops the run **before anything is written**. Drop such columns, for example with `sql_exclude_columns`.
- A source column named `record_id` is only used to find records, never written. The `_ingestr_loaded_at` and `_ingestr_run_id` columns are never sent.

### Strategies

| Strategy | What it does | What it needs |
|---|---|---|
| `merge` | Updates the matching record, or creates it if there is none | `matching_attribute=` and a `primary_key` column |
| `update` | Updates matching records only; a row with no match is rejected | nothing when matching by `record_id` |
| `append` | Always creates new records | nothing |
| `delete` | Deletes matching records | nothing when matching by `record_id` |
| `replace` | Like `merge`, then deletes every record that isn't in the source | `matching_attribute=` and a `primary_key` column |

- **`append`** never matches, so re-running it creates duplicates, unless a unique attribute refuses them. `matching_attribute=` isn't allowed with it; a `primary_key` column is accepted and only names rejected rows in the report.
- **`delete`** deletes records permanently. Attio has no recycle bin for records deleted through the API.
- In `merge`, a row with an empty match value creates a new record.

> [!WARNING]
> `replace` permanently deletes **every** record of the object that isn't in your source, including ones created in Attio or by other tools. Use it only when your source is the complete list. As a safety net, a run with **0 source rows** deletes nothing, and with the default `reject_mode: fail` a run with any rejected row deletes nothing either.

### Matching records

Two settings decide which record a row updates or deletes:

| Setting | Set with | Example |
|---|---|---|
| The **Attio attribute** to match on | `matching_attribute=` in `destination_table` | `people?matching_attribute=email_addresses` |
| The **source column** that holds the value | `primary_key: true` on the column | `- name: email_addresses` |

**`merge` and `replace`** need both, and the attribute must be **unique** in Attio, e.g. `email_addresses` on people or `domains` on companies. To match on your own id, create a text attribute, tick **Unique**, and fill it from the source. `record_id` can't be used, because Attio can't create a record with an id you choose.

Email addresses and domains match regardless of case. A unique text attribute you create is case-sensitive: `A-1` and `a-1` are two different records.

**`update` and `delete`** match by Attio's record id by default, using a source column named `record_id`:

```yaml
parameters:
  source_connection: my-postgres
  source_table: 'public.churned_people'   # has a record_id column
  destination: attio
  destination_connection: my-attio
  destination_table: 'people'
  incremental_strategy: delete
```

They can also match on any text, number, email, domain or select attribute with `matching_attribute=`, even one that isn't unique. Then **every** matching record is updated or deleted.

### Linking records

Attio links records through **record reference** attributes, such as a person's `company` or a deal's `associated_company`. Write them like any other column, in one of these ways:

| You have | Column name | Example value |
|---|---|---|
| A company's domain or a person's email | The attribute, e.g. `company` | `acme.com` |
| A unique value on the linked record | `<attribute>.<unique attribute>`, e.g. `company.domains` | `acme.com` |
| The linked record's Attio id | `<attribute>.record_id`, e.g. `company.record_id` | `99a03ff3-0435-47da-95cc-76b2caeb4dab` |

- Name the column this way in your source query (`SELECT company_domain AS "company.domains"`), or map it with `source_column` (see [Column name mapping](#column-name-mapping)).
- If the attribute can point to more than one object, put the object in the middle: `<attribute>.<object>.<unique attribute>`, e.g. `related.people.email_addresses`.
- A list value links to several records at once, for attributes that allow many.
- The linked record must already exist, so load companies before the people that point to them.
- An empty (null) value removes the link. Set `write_nulls: false` to leave existing links alone.

### Names

A person's `name` can be written in two ways:
- As one text column in the form `Last, First`, e.g. `Lovelace, Ada`. Text without a comma is taken as the first name only.
- As separate columns `name.first_name`, `name.last_name` and, optionally, `name.full_name`. Without a full name, the first and last names are joined.

Attio stores a name as one value, so every write replaces the whole name: a row with only `name.first_name` clears the last name. When every name column is empty, the name is cleared, or left as it is with `write_nulls: false`.

### Values

- Select and status attributes take the option's title, e.g. `Lead`.
- A list value writes every item to an attribute that allows several values (tags, email addresses, domains). On `merge` and `update`, the list replaces the record's current values.
- Timestamps are written in UTC, and Attio keeps them to the millisecond.

### Run options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reject_mode` | `fail` | How to handle a row Attio refuses, or that matches no record. `fail`: write the valid rows, then error with the reject list. `fail_fast`: stop at the first bad row. `skip`: write the valid rows, report the rejects, and still succeed. |
| `write_nulls` | `true` | Whether a source NULL clears the Attio attribute. `false` leaves the existing value untouched. No effect on `delete`. |

Problems that affect the whole run, such as an invalid API key or a missing scope, always stop it, whatever the `reject_mode`. Attio writes can't be rolled back, so with `fail` or `fail_fast` some records may already be written when the run stops.

Attio accepts about 25 writes per second, so a run writes roughly 1,000 records a minute.

### Column name mapping

When a source column name differs from the Attio attribute, set `source_column` on the column entry: `name` is the attribute's slug, `source_column` is the source column. This requires `enforce_schema: "true"`, and you must **omit `type`**, since Attio owns the attribute types:

```yaml
parameters:
  # ...
  enforce_schema: "true"

columns:
  - name: email_addresses
    source_column: email
    primary_key: true
  - name: company.domains
    source_column: company_domain
```
