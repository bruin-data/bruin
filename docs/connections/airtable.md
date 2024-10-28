# Airtable
[Airtable ](https://www.airtable.com/) is a cloud-based platform that combines spreadsheet and database functionalities, designed for data management and collaboration.

Bruin supports Airtable as a source for [Ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), and you can use it to ingest data from Airtable into your data warehouse.

In order to have set up Airtable connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Airtable section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

```yaml
    connections:
      airtable:
        - name: "connection_name"
          base_id: "id123",
          access_token: "key123",
```
