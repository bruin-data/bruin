# Google Sheets

[Google Sheets](https://workspace.google.com/products/sheets/)  is a web-based spreadsheet program that is part of Google's free, web-based Google Docs Editors suite.

Bruin supports Google Sheets as a source, and you can use it to ingest data from Google Sheets into your data warehouse.

In order to have set up a Google Sheets connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.
For more information on how to get these credentials check the Google Sheets section in [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/gsheets.html).

```yaml
    connections:
      google_sheets:
        - name: "connection_name"
          credentials_path: "/path/to/service/account.json"
```