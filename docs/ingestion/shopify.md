# Shopify

In order to have set up a Shopify connection, you need to add a configuration item to connections in the .bruin.yml file complying with the following schema.

```yaml
connections:
    shopify:
        - name: "shopify-default"
          api_key: "********"
          url: "<YOUR STORE URL>"
```

The following fields are required:
- `url`: Your Shopify store's URL
- `api_key`: A private app access token or admin API access token