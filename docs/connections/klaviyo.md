# Klaviyo
[Klaviyo](https://www.Klaviyo.com/) is a marketing automation platform that helps businesses build and manage smarter digital relationships with their customers by connecting through personalized email and enhancing customer loyality.

ingestr supports Klaviyo as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Klaviyo into your data warehouse.

In order to have set up Klaviyo connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Klaviyo section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

```yaml
    connections:
      Klaviyo:
        - name: "connection_name"
          api_key: "key123"
