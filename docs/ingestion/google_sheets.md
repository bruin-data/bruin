# Google Sheets
[Google Sheets](https://www.google.com/sheets/about/) is a web-based spreadsheet program that is part of Google's free, web-based Google Docs Editors suite.

Bruin supports Google Sheets as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Sheets into your data warehouse.

To set up a Google Sheets connection, you need to add a configuration item in the `.bruin.yml` file and the asset file. You will need either the `service_account_file` or the `service_account_json`. For more information, please follow the [guide](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets#google-service-account-credentials). Once you complete the guide, you should have a `service account JSON` file.

Follow the steps below to correctly set up Google Sheets as a data source and run ingestion.

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
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself


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
```     
bruin run assets/gsheets_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Google Sheets table into your Postgres database.

<img width="1140" alt="google_sheets" src="https://github.com/user-attachments/assets/8ee4e055-15e8-4439-a94c-26e124bfd5a7">
