# Pinterest
[Pinterest](https://www.pinterest.com/) is a social media platform for discovering and sharing ideas using visual bookmarks.

Bruin supports Pinterest as a source for [Ingestr assets](/assets/ingestr), allowing you to ingest data from Pinterest into your data warehouse.

To connect to Pinterest you must add a configuration item to the `.bruin.yml` file and the asset file. You will need `access_token`.

Follow the steps below to correctly set up Pinterest as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
Add the connection configuration to the connections section of `.bruin.yml`:

```yaml
connections:
  pinterest:
    - name: "pinterest"
      access_token: "your-token"
```

- `access_token`: The token used for authentication with the Pinterest API. You can obtain an access token from the [official Pinterest documentation](https://developers.pinterest.com/docs/getting-started/connect-app/).

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file to define the data flow:

```yaml
name: public.pinterest_pins
type: ingestr

parameters:
  source_connection: pinterest
  source_table: 'pins'

  destination: postgres
```

- `source_connection`: name of the Pinterest connection defined in `.bruin.yml`.
- `source_table`: Pinterest table to ingest. Available tables are listed in the [Ingestr documentation](https://github.com/bruin-data/ingestr/blob/main/docs/supported-sources/pinterest.md#tables).
- `destination`: name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/pinterest_asset.yml
```

Executing this command ingests data from Pinterest into your Postgres database.
