# Zoom
[Zoom](https://zoom.us/) is a video conferencing platform used for online meetings and webinars.

Bruin supports Zoom as a source for [ingestr assets](/assets/ingestr), so you can ingest data from Zoom into your data warehouse.

### Prerequisites
- A Zoom Server-to-Server OAuth App
- Appropriate permissions related to meetings and users must be added in the app's scopes
- Obtain the client_id, client_secret and account_id credentials from the app

To connect to Zoom you must add a configuration item to the `.bruin.yml` file and the asset file.
Follow the steps below to correctly set up Zoom as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
Add the connection configuration to the connections section of `.bruin.yml`:

```yaml
connections:
  zoom:
    - name: "zoom"
      client_id: "cid"
      client_secret: "csecret"
      account_id: "accid"
```

- `client_id`: OAuth client id from your Zoom application.
- `client_secret`: OAuth client secret.
- `account_id`: Zoom account id.

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file to define the data flow:

```yaml
name: public.zoom_meetings
type: ingestr

parameters:
  source_connection: zoom
  source_table: 'meetings'

  destination: duckdb
```

- `source_connection`: name of the Zoom connection defined in `.bruin.yml`.
- `source_table`: Zoom table to ingest. For more information, [read here](https://bruin-data.github.io/ingestr/supported-sources/zoom.html)
- `destination`: name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/zoom_asset.yml
```
<img alt="zoom" src="./media/zoom_ingestion.png">

