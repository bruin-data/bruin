# Reverse ETL

Reverse ETL is ingestion in the other direction: instead of loading data **into** your warehouse, Bruin pushes rows **out** of the warehouse and into an operational tool — a CRM, a marketing app — through that tool's API.

- The warehouse stays the source of truth.
- Any Bruin source can feed a reverse-ETL destination.
- The asset is a regular `ingestr` asset — you just point `destination` at a reverse-ETL platform. Rows are read the same way as any other ingestion (full table, or a window via an incremental key). Only the destination changes: each row becomes a remote record operation, not a row in a table.

## How it's different from a warehouse destination

A warehouse destination stages data, runs SQL, and swaps tables. A reverse-ETL destination does none of that:

- No staging table, no SQL, no atomic swap.
- Each source row is written to the target API as one record (or one link). It usually affects a single remote record, but not always — matching on a non-unique field can update or delete every record that matches (see [Matching records](#matching-records)).
- Rows are sent in bulk through the API's batch endpoints, within its rate limits.

Two things follow from this:

- **The API decides what's possible.** Which strategies work and what you can match on depend entirely on the target API. Bruin only exposes what the API actually supports.
- **Writes aren't transactional.** A run that fails partway may already have written some records, and Bruin can't roll them back. It reports the rows the API rejected (see [Run options](#run-options)).

## Supported destinations

| Destination | What you can write |
| ----------- | ------------------ |
| [HubSpot](/ingestion/hubspot#hubspot-as-a-destination) | CRM records (contacts, companies, deals, custom objects) and associations |
| [CleverTap](/ingestion/clevertap#clevertap-as-a-destination) | User profiles and events |

Each destination's page has its own connection setup, object types, and quirks. This page covers what they share.

## Strategies

Pick the write behaviour with `incremental_strategy` in the asset's `parameters`. There's often no sensible default, so some destinations require it explicitly. Where a destination honours the strategy, the names map to API operations like this:

- `merge` — upsert: update the match, or create it if there's none. The usual choice.
- `update` — update matches only; a row with no match is rejected, never created.
- `append` — always create. Re-running duplicates rows unless you scope each run to new rows.
- `delete` — remove the matches. Whether that's a soft-delete (archive, recoverable) or a hard-delete (permanent) depends on the destination — some APIs only offer one. Check its page.
- `replace` — mirror: upsert every source row, then remove anything **not** in the source, using the same delete semantics as `delete`.

Notes:

- Not every destination supports every strategy — check its page.
- Impossible combinations are rejected before the run starts, not silently mishandled.
- **Some destinations don't act on the strategy at all.** CleverTap, for example, always upserts profiles and always appends events regardless of the strategy you pass — so `merge`, `delete`, and `replace` don't perform the operations above (a `delete` or `replace` run can succeed while removing nothing). The strategy semantics here apply only where the destination's page says the strategy is honoured.

::: warning
On a destination that supports deletion, `replace` removes **every** record that isn't in your source — including ones created by hand or by other tools. Use it only when the source is the complete system of record. A run with 0 source rows removes nothing (a safety guard). To remove specific records, use `delete`.
:::

## Matching records

`merge`, `update`, and `delete` need to find the existing record. That's two separate things:

- **Remote field to match on** — a destination-side property, set on `destination_table` (e.g. `destination_table: 'contacts?id_property=email'`).
- **Source column with the value** — the column marked `primary_key: true` in the asset's `columns`.

They're independent — the `id_property` names the remote property, and the `primary_key` column supplies the value matched against it.

The exact identity rules differ per destination — what counts as a valid match field, and whether more than one key is allowed, depends on the API. See each page.

## Run options

Two parameters apply only to reverse-ETL destinations:

### `reject_mode`

What happens to a row the API can't apply (no match, or rejected on its value). All three modes write the valid rows they process — they differ in how far they get and in whether the run fails:

- `fail` *(default)* — send every valid row, then fail at the end listing the rejects.
- `fail_fast` — stop at the first bad row; valid rows before it are written, rows after it aren't sent.
- `skip` — send every valid row, report the rejects, and still succeed.

Only per-record problems are skippable this way. Systemic failures (auth, malformed request) always abort, whatever the mode.

### `write_nulls`

What a source `NULL` does to a field:

- `true` *(default)* — write it through, clearing the field.
- `false` — omit the cell, leaving the existing stored value untouched.

Only affects strategies that write field values (`merge`, `update`, `append`, `replace`). No effect on `delete`, or on writes that don't carry editable fields.

## Column name mapping

When a source column name differs from the destination property name, set `source_column` on the column entry — `name` is the destination property, `source_column` is the source column:

```yaml
parameters:
  # ...
  enforce_schema: "true"

columns:
  - name: firstname          # destination property
    source_column: first_name
```

- Rename only — reverse-ETL destinations own their property types. Some destinations reject a renamed column that carries a `type`; check its page.
- Whether an unknown property is accepted also depends on the destination: some create attributes on the fly, others require the property to already exist. Check its page.

## Example

Upsert warehouse contacts into HubSpot, matching on email:

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

The same overall asset shape works for CleverTap, but you must also adapt the destination table and matching parameters (CleverTap writes `profiles`/`events` and takes its identity from a `primary_key: true` column, not `contacts?id_property=email`) — see each destination's page for its object types and required parameters.
