# Zendesk
[Zendesk](https://www.hubspot.com/)  is a cloud-based customer service and support platform. It offers a range of features including ticket management, self-service options, knowledgebase management, live chat, customer analytics, and conversations.

ingestr supports Zendesk as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from zendesk into your data warehouse.

In order to have set up Zendesk connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.
Depending on the data you are ingesting (source table), you will need to use either API Token authentication or OAuth Token authentication. Choose the appropriate method based on your source table. [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/zendesk.html)


API Token Authentication:
```yaml
      connections:
          zendesk:
            - name: "connection_name",
              api_key: "xyzKey",
              email: "example.zendesk@gmail.com",
              subdomain: "myCompany",
```

OAuth Token Authentication:
```yaml
  connections:
        zendesk:
          - name: "connection_name",
            oauth_token: "abcToken",
            subdomain: "myCompany",
```