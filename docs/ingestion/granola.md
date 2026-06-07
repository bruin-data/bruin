# Granola

[Granola](https://www.granola.ai/) is an AI meeting-notes product. Bruin supports Granola as a source for [Ingestr assets](/assets/ingestr), including notes and folders from the Granola public API.

To set up a Granola connection, add a configuration item in the `.bruin.yml` file and reference it in your asset file. The configuration requires `api_key`.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  granola:
    - name: "granola"
      api_key: "your-api-key"
```

- `api_key`: Granola API token used as a bearer token.

You can also use environment variables in your `.bruin.yml` file:

```yaml
connections:
  granola:
    - name: "granola"
      api_key: ${GRANOLA_API_KEY}
```

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (for example, `granola_ingestion.yml`) inside the assets folder:

```yaml
name: public.granola_notes
type: ingestr

parameters:
  source_connection: granola
  source_table: "notes"

  destination: postgres
```

- `name`: The destination asset name.
- `type`: Always `ingestr` for Granola.
- `source_connection`: The Granola connection name defined in `.bruin.yml`.
- `source_table`: Name of the Granola table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| notes | id | updated_at | merge | Meeting notes listed from `/v1/notes` and hydrated from `/v1/notes/{note_id}`, including summary, transcript, attendees, folder membership, calendar event, and web URL. Incremental runs use Granola's `updated_after` parameter. |
| folders | id |  | replace | Accessible folders returned by the `/v1/folders` endpoint. |

## Incremental Loading

The `notes` table supports incremental loading by `updated_at`:

```yaml
name: public.granola_notes
type: ingestr

parameters:
  source_connection: granola
  source_table: "notes"
  destination: postgres
```

The `folders` table is loaded as a full refresh. The `notes` table fetches note details with `include=transcript`, so transcripts are available in the `transcript` JSON column when Granola returns them.

### Step 3: Run the asset to ingest data

```bash
bruin run assets/granola_ingestion.yml
```

Running this command ingests data from Granola into your configured destination.
