# Google Sheets

[Google Sheets](https://workspace.google.com/products/sheets/)  is a web-based spreadsheet program that is part of Google's free, web-based Google Docs Editors suite.

Bruin supports Google Sheets as a source, and you can use it to ingest data from Google Sheets into your data warehouse.

In order to have set up a Google Sheets connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.


```yaml
    connections:
      google_sheets:
        - name: "connection_name"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```