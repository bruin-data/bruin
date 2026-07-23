# Typeform

[Typeform](https://www.typeform.com/) is an online form and survey platform for building interactive forms and collecting responses.

Bruin supports Typeform as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Typeform into your data platform.

To set up a Typeform connection, you need to add a configuration entry to your `.bruin.yml` file and create an asset file for data ingestion. You'll need a `token` (personal access token) to authenticate.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  typeform:
    - name: "my_typeform"
      token: "your-personal-access-token"
```

- `token` (required): The personal access token used to authenticate with the Typeform API.
- `region` (optional): The account region. Must be `us` (default) or `eu`.

For EU accounts:

```yaml
connections:
  typeform:
    - name: "my_typeform_eu"
      token: "your-personal-access-token"
      region: "eu"
```

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `typeform.asset.yml`) inside the assets folder:

```yaml
name: public.typeform
type: ingestr

parameters:
  source_connection: my_typeform
  source_table: 'forms'

  destination: postgres
```

- `source_connection`: The name of the Typeform connection defined in `.bruin.yml`.
- `source_table`: The table to ingest from Typeform (see available tables below).
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/typeform.asset.yml
```

Running this command ingests data from Typeform into your destination.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `forms` | id | last_updated_at | merge | All forms in the account with metadata (title, settings, theme, timestamps) |
| `responses` | response_id | submitted_at | merge | Submitted responses with answers and metadata, collected per form |
| `workspaces` | id | - | replace | Workspaces in the account |
| `themes` | id | - | replace | Themes available in the account |

Use these as the `source_table` parameter in the asset configuration.

## Getting a Personal Access Token

1. Log in to your Typeform account.
2. Go to your [personal tokens settings](https://admin.typeform.com/user/tokens).
3. Click **Generate a new token**, give it a name, and grant at least read access to forms, responses, workspaces, and themes.
4. Copy the generated token.
