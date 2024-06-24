# Gorgias

[Gorgias](https://gorgias.com) is a helpdesk for e-commerce merchants, providing customer service via email, social media, SMS, and live chat.

Bruin supports Gorgias as a source, and you can use it to ingest data from Gorgias into your data warehouse.

In order to have set up a Gorgias connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.
For more information on how to get these credentials check the Gorgias section in [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/gorgias.html).

```yaml
    connections:
      gorgias:
        - name: "connection_name"
          domain: "my-shop"
          email: "myemail@domain.com"
          api_key: "abc123"
```
