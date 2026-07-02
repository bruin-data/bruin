# Google Sheets

[Google Sheets](https://www.google.com/sheets/about/) is a web-based spreadsheet program that is part of Google's free, web-based Google Docs Editors suite.

Bruin supports Google Sheets as both a source and a destination for [Ingestr assets](/assets/ingestr): you can ingest data from Google Sheets into your data warehouse, or write data from any supported source into a Google Sheets spreadsheet.

To set up a Google Sheets connection, you need to add a configuration item in the `.bruin.yml` file and the `asset` file. You can provide either the `service_account_file` or the `service_account_json`; if you omit both, Application Default Credentials are used (e.g. after running `gcloud auth application-default login`). For more information, please follow the [guide](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets#google-service-account-credentials). Once you complete the guide, you should have a `service account JSON` file.

Follow the steps below to correctly set up Google Sheets as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

```yaml
    connections:
      google_sheets:
        - name: "my-gsheets"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"

          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```

- `service_account_file` (optional): The path to the service account JSON file. If omitted, Application Default Credentials are used (e.g. the `GOOGLE_APPLICATION_CREDENTIALS` env var, or the `gcloud auth application-default login` token on your machine).
- `service_account_json` (optional): The service account JSON content itself (alternative to `service_account_file`).

### Step 2: Create an asset file for data ingestion

To ingest data from Google Sheets, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gsheets_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gsheets
type: ingestr
connection: postgres

parameters:
  source_connection: my-gsheets
  source_table: '16UY6EQ_6jkdUdasdNfUq2CA.Sheet1'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Google Sheets connection defined in .bruin.yml.
- `source_table`: The name of the data table in Google Sheets to ingest. For example, if the `spreadsheet URL` is https\://docs.google.com/spreadsheets/d/1VTtCiw7UM1sadasdfas/edit?usp=sharing, the `spreadsheet ID` is 1VTtCiw7UM1sadasdfas. If the `sheet name` is Sheet1, the `source_table` will be `1VTtCiw7UM1sadasdfas.Sheet1`

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/gsheets_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Google Sheets table into your Postgres database.

<img width="1140" alt="google_sheets" src="https://github.com/user-attachments/assets/8ee4e055-15e8-4439-a94c-26e124bfd5a7">

## Google Sheets as a destination

You can also write data into a Google Sheets spreadsheet by using a Google Sheets connection as the ingestr destination. The connection is configured exactly as above (Step 1), but the credentials must grant **write** access — share the target spreadsheet with the service account's `client_email` as an **Editor**.

Set `destination: gsheets` and point the asset `connection` at your Google Sheets connection. The asset `name` is used as the target and must follow the `spreadsheet_id.sheet_name` format; the sheet (tab) is created automatically if it does not exist.

```yaml
name: '16UY6EQ_6jkdUdasdNfUq2CA.Sheet1'
type: ingestr
connection: my-gsheets

parameters:
  source_connection: my-postgres
  source_table: 'public.users'

  destination: gsheets
```

- `name`: The target spreadsheet and sheet, in `spreadsheet_id.sheet_name` format.
- `connection`: The name of the Google Sheets connection (Editor access) defined in `.bruin.yml`.
- `destination`: Set to `gsheets` to write into Google Sheets.

Only the `replace` (default) and `append` incremental strategies are supported for the Google Sheets destination.
