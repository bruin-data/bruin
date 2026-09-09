# Notifications

Notification rules are managed under **Team Settings → Notifications**. A rule defines which events to watch, optional filters, and where Bruin sends the notification.

Notification rules are separate from notification settings in pipeline files. If your team still manages notifications in `pipeline.yml`, see [Legacy notifications and migration](/cloud/legacy-notifications).

## How rules work

Each rule describes what you want to be notified about and who should receive the notification. For example, you can notify the data team in Slack when a production pipeline fails. Use filters to focus a rule on particular projects, pipelines, assets, or checks, and disable it whenever you want to pause notifications.

A rule is built from:

- **Trigger:** Selects the events to watch, such as a failed pipeline run or a successful column check. A rule can have several triggers and matches when any trigger matches.
- **Condition:** Filters events by one field, such as `Pipeline name equals daily-orders`.
- **Condition group:** Combines related conditions or other condition groups. Choose **all** when every item in the group must match, or **any** when at least one item must match.
- **Destination:** Defines who receives the notification, such as an email address or Slack channel.

## Create a rule

Go to **Team Settings → Notifications** and click **New rule**.

1. Enter a name that describes the notification, such as `Production failures`.
2. Select the events the rule should watch.
3. Add filters if the rule should apply only to specific projects, pipelines, assets, or checks.
4. Add one or more destinations.
5. Choose whether the rule should start enabled, then click **Save rule**.

### Triggers

A trigger can watch successful or failed events for:

| Event group | When it occurs |
| --- | --- |
| Pipeline runs | A pipeline run succeeds or fails. |
| Assets | An asset execution succeeds or fails. |
| Column checks | A column check succeeds or fails. |
| Custom checks | A custom check succeeds or fails. |

You can select several events in one trigger. You can also add another trigger when different groups of events need different filters. A rule matches when any of its triggers matches.

### Filters

Filters are optional. Without filters, a trigger matches every selected event for the team.

You can filter by:

- **Pipeline:** project ID or pipeline name.
- **Run:** run ID.
- **Asset:** name or type.
- **Check:** name or column.

Choose **all** to require every condition in a group, or **any** to require at least one condition. In the **Order Fails** rule below, **all** requires the pipeline name to equal `pipeline-2`, while the nested **any** group accepts either `a_asset` or `b_asset`.

<a href="notifications/notification-rule-conditions.png" target="_blank">
  <img class="docs-screenshot" src="/cloud/notifications/notification-rule-conditions.png" alt="A notification trigger with nested all and any condition groups">
</a>

### Destinations

A rule can send to multiple destinations and can include multiple recipients or channels for a destination.

| Destination | Setup | Value |
| --- | --- | --- |
| Email | No connection is required. | One or more email addresses. |
| Slack | [Connect Slack to Bruin](/cloud/integrations/slack). | A channel name or ID. |
| Microsoft Teams | [Add the Bruin bot to Teams](/cloud/integrations/teams). | The full channel ID, starting with `19:`. Connection names are not supported. |
| Discord | [Connect the Bruin bot to Discord](/cloud/integrations/discord). | A numeric channel ID the bot can access. Connection names are not supported. |

Enter multiple values on separate lines or separate them with commas.

To find a Microsoft Teams channel ID, copy the channel link. The encoded value between `/channel/` and the next `/` is the channel ID. Replace `%3A` with `:` and `%40` with `@`. For example, `19%3Aabc%40thread.tacv2` becomes `19:abc@thread.tacv2`.

<a href="notifications/notification-rule-destinations.png" target="_blank">
  <img class="docs-screenshot" src="/cloud/notifications/notification-rule-destinations.png" alt="Email and Slack destinations in a notification rule">
</a>

## Manage rules

The notifications page shows each rule with its events, filters, destinations, and current status.

<a href="notifications/notification-rules-overview.png" target="_blank">
  <img class="docs-screenshot" src="/cloud/notifications/notification-rules-overview.png" alt="Notification rules with one rule expanded">
</a>

- Expand a rule to inspect the complete matching logic.
- Use the switch to enable or disable a rule without deleting it.
- Use **Edit** to change its triggers, filters, or destinations.
- Use **Delete** to remove it permanently.

Changes apply to events received after the rule is saved. Disabling or deleting a rule does not affect notifications that were already sent.

## Related

- [Legacy notifications and migration](/cloud/legacy-notifications)
- [Bruin Cloud integrations](/cloud/integrations/overview)
