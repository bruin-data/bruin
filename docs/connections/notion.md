# Notion

In order to have set up a Notion connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.
For more information on how to get these credentials check the Notion Section in [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/notion.html).


```yaml
    connections:
      notion:
        - name: "connection_name"
          api_key: "XXXXXXXX"
```
