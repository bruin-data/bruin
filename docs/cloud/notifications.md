# Notifications

Bruin Cloud supports notifications across Slack, Microsoft Teams, Discord, and generic webhooks. Notifications can be configured at three scopes:

| Scope | When it fires | Where it's configured |
|-------|---------------|-----------------------|
| **Pipeline** | When an entire pipeline run succeeds or fails | `pipeline.yml` |
| **Asset** | When a single asset succeeds or fails | Asset definition file |
| **Check** | When a quality check with its own `notifications` block succeeds or fails | Asset definition file |

All three scopes share the same `notifications` block structure and the same `success`/`failure` flags. The `success` and `failure` flags default to `true`, so omitting them means notifications are sent for both outcomes.

---

## Pipeline-level notifications

Pipeline notifications fire once per run, when all assets have finished.

```yaml
# pipeline.yml
notifications:
  slack:
    - channel: "#channel1"

  ms_teams:
    - connection: "the-name-of-the-ms-teams-connection"

  discord:
    - connection: "the-name-of-the-discord-connection"

  webhook:
    - connection: "the-name-of-the-webhook-connection"
```

---

## Asset-level notifications

Asset notifications fire when the individual asset finishes, independently of the pipeline run. This is useful when you want targeted alerts for critical assets without waiting for the full pipeline to complete.

Add a `notifications` block directly inside the asset definition:

```bruin-sql
/* @bruin

name: finance.revenue
type: bq.sql

notifications:
  slack:
    # send to this channel on both success and failure (default)
    - channel: "#channel1"

    # send to this channel on failure only
    - channel: "#channel2"
      success: false

  ms_teams:
    - connection: "the-name-of-the-ms-teams-connection"
      success: false   # failure only

  discord:
    - connection: "the-name-of-the-discord-connection"

  webhook:
    - connection: "the-name-of-the-webhook-connection"

@bruin */

SELECT ...
```

Asset success notifications fire after the asset task **and all its quality checks** have passed. Asset failure notifications fire as soon as the asset task itself fails (before checks run). Check notifications (below) are separate and fire for each individual quality check.

---

## Check-level notifications

Check notifications fire for each individual quality check that runs against an asset — both **column-level checks** (defined under `columns[].checks`) and **custom checks** (defined under `custom_checks`).

Check notifications are **opt-in per check** — asset-level notifications do not apply to individual checks. To receive a notification for a specific check, add a `notifications` block directly on that check.

```bruin-sql
/* @bruin

name: orders.curated
type: bq.sql

notifications:
  slack:
    # Notified when the asset succeeds or fails (does not cover individual checks).
    - channel: "#data-quality"

    # Notified only when the asset fails.
    - channel: "#data-alerts"
      success: false

columns:
  - name: order_id
    checks:
      - name: not_null
      - name: unique
      - name: accepted_values
        value: ["pending", "shipped", "delivered"]
        # This check has its own notifications — sent to #data-alerts on failure only
        notifications:
          slack:
            - channel: "#data-alerts"
              success: false

custom_checks:
  - name: order count is positive
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM orders.curated
    value: 1
    # This check has its own notifications configured
    notifications:
      slack:
        - channel: "#data-quality"

@bruin */

SELECT ...
```

Checks without their own `notifications` block send no notification — asset-level notifications only cover the asset success/failure event, not individual checks.

### Check notification timing

- **Column check** — fires immediately when that column's check task completes (pass or fail).
- **Custom check** — fires immediately when that custom check task completes (pass or fail).
- **Asset success** — fires after all checks have passed (i.e., the asset is fully complete including all checks).

These are distinct events. Only checks with their own `notifications` block produce check notifications. An asset success notification fires once regardless of how many checks pass.

---

## Platform configuration

### Slack

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

---

### Microsoft Teams

> [!INFO]
> You need to create a Microsoft Teams connection in Bruin Cloud before you can use Teams notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Microsoft Teams webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Microsoft Teams notifications is like this:

```yaml
notifications:
  ms_teams:
    - connection: "the-name-of-the-ms-teams-connection"
      success: true   # default: true
      failure: true   # default: true
```

---

### Discord

> [!INFO]
> You need to create a Discord connection in Bruin Cloud before you can use Discord notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Discord webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Discord notifications is like this:

```yaml
notifications:
  discord:
    - connection: "the-name-of-the-discord-connection"
      success: true   # default: true
      failure: true   # default: true
```

---

### Webhook

> [!INFO]
> You need to create a Webhook connection in Bruin Cloud before you can use webhook notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI and adding a Webhook connection pointing to your endpoint.

Webhook notifications are generic and can target any HTTP endpoint you configure via a connection.

The full spec for Webhook notifications is like this:

```yaml
notifications:
  webhook:
    - connection: "the-name-of-the-webhook-connection"
      success: true   # default: true
      failure: true   # default: true
```

Details:

- Method: POST
- Auth: Basic Auth (configure username/password in the Webhook connection)
- Body: JSON
- Headers: `Content-Type: application/json`

#### Payload shape

The payload is the same for all events. Fields that don't apply to the event type are `null`.

| Field | Type | Description |
|-------|------|-------------|
| `pipeline` | string | Pipeline name |
| `asset` | string \| null | Asset name — `null` for pipeline-level events |
| `column` | string \| null | Column name — non-null for column check events only |
| `check` | string \| null | Check name — non-null for check events only |
| `run_id` | string | Unique run identifier |
| `status` | string | `"success"` or `"failure"` |
