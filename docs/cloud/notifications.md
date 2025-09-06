# Notifications

Bruin Cloud supports various types of notifications, starting with Slack & Microsoft Teams. These notifications allow you to receive updates on your data pipelines, such as when a pipeline has completed successfully, as well as when a pipeline has failed.

Notifications are always defined on a pipeline level, inside the `pipeline.yml` file.

## Slack

> [!INFO]
> You need to create a Slack connection in Bruin Cloud before you can use Slack notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

Adding Slack notifications is just a few lines of code:
```yaml
notifications:
  slack:
    # the only required field is `channel`. By default, this will send both success and failure notifications to this channel.   
    - channel: "#channel1"
    
    # you can have multiple channels, all of them will be notified.
    - channel: "#channel2"
    
    # you can also specify different channels for success and failure notifications
    - channel: "#channel-for-only-success"
      failure: false

    - channel: "#channel-for-only-failure"
      success: false

```

The full spec for Slack notifications is like this:
```yaml
notifications:
  slack:
    - channel: "#your-channel-name"
      success: true
      failure: true
```

## Microsoft Teams

> [!INFO]
> You need to create a Microsoft Teams connection in Bruin Cloud before you can use Teams notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Microsoft Teams webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Microsoft Teams notifications is like this:
```yaml
notifications:
  ms_teams:
    - connection: "the-name-of-the-ms-teams-connection"
      success: true
      failure: true
```

## Discord

> [!INFO]
> You need to create a Discord connection in Bruin Cloud before you can use Discord notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Discord webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Discord notifications is like this:
```yaml
notifications:
  discord:
    - connection: "the-name-of-the-discord-connection"
      success: true
      failure: true
```

## Webhook

> [!INFO]
> You need to create a Webhook connection in Bruin Cloud before you can use webhook notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI and adding a Webhook connection pointing to your endpoint.

Webhook notifications are generic and can target any HTTP endpoint you configure via a connection.

The full spec for Webhook notifications is like this:
```yaml
notifications:
  webhook:
    - connection: "the-name-of-the-webhook-connection"
      success: true
      failure: true
```

Details:
- Method: POST
- Auth: Basic Auth (configure username/password in the Webhook connection)
- Body: JSON
- Headers: `Content-Type: application/json`

Example payloads

Success
```json
{
  "pipeline": "daily_orders",
  "asset": null,
  "column": null,
  "check": null,
  "run_id": "2025-09-03T12:34:56Z-8f3a2c",
  "status": "success"
}
```

Column check failure
```json
{
  "pipeline": "daily_orders",
  "asset": "orders_curated",
  "column": "order_id",
  "check": "not_null",
  "run_id": "2025-09-03T00:00:00Z-42b1de",
  "status": "failure"
}
```
