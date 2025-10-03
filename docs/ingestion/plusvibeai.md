# Plus Vibe AI

[Plus Vibe AI](https://plusvibe.ai/) is an email marketing and outreach platform that helps businesses automate their email campaigns, manage leads, and track engagement metrics.

To set up a Plus Vibe AI connection, you need to have an API key and workspace ID that has the necessary permissions for the resources you want to access.

## Set up a connection

Plus Vibe AI connections are defined using the following properties:

- `name`: The name to identify this connection
- `api_key`: Your Plus Vibe AI API key (required)
- `workspace_id`: Your workspace ID (required)

:::code-group
```yaml [connections.yml]
connections:
  plusvibeai:
    - name: "my_plusvibeai"
      api_key: "your_api_key"
      workspace_id: "your_workspace_id"
```
:::

You can also use environment variables in your connections.yml by using the `&#123;&#123; env_var("ENV_VAR_NAME") &#125;&#125;` syntax.

For example:
```yaml
connections:
  plusvibeai:
    - name: "my_plusvibeai"
      api_key: "&#123;&#123; env_var('PLUSVIBEAI_API_KEY') &#125;&#125;"
      workspace_id: "&#123;&#123; env_var('PLUSVIBEAI_WORKSPACE_ID') &#125;&#125;"
```

## Supported Data Assets

Plus Vibe AI assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset                | Table Name        | Incremental Key | Description                                                                                                                                        |
|----------------------|-------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| Campaigns            | `campaigns`       | `modified_at`   | Campaign information including configuration, schedules, sequences, and performance metrics                                                       |
| Leads                | `leads`           | `modified_at`   | Lead information including contact details, campaign association, engagement metrics, and professional information                                 |
| Email Accounts       | `email_accounts`  | `timestamp_updated` | Email account configurations including SMTP/IMAP settings, warmup configurations, and analytics data                                              |
| Emails               | `emails`          | `timestamp_created` | Email data including message content, headers, thread information, and recipient details                                                          |
| Blocklist            | `blocklist`       | `created_at`    | Blocklist entries for email addresses or domains that should be excluded from campaigns                                                           |
| Webhooks             | `webhooks`        | `modified_at`   | Webhook configurations for receiving real-time notifications about campaign events and lead interactions                                          |
| Tags                 | `tags`            | `modified_at`   | Tag information used for organizing and categorizing campaigns, leads, and other resources                                                        |

## Asset-Specific Configuration

Plus Vibe AI assets support incremental loading based on modification timestamps. Each table uses its respective timestamp field to fetch only updated records since the last sync.

### Nested Data Handling

The source preserves nested objects as JSON columns to maintain data structure integrity:

- **Campaigns**: Schedule, sequences, and events are stored as JSON
- **Email Accounts**: All configuration data is stored in the `payload` JSON field
- **Emails**: Headers and address information are stored as JSON

## Notes

- **Authentication**: Get your API key from https://app.plusvibe.ai/v2/settings/api-access/
- **Incremental Loading**: Supported for all tables using their respective timestamp fields
- **Rate Limiting**: Plus Vibe AI API has a rate limit of 5 requests per second. The source automatically handles rate limiting with exponential backoff and retry logic
- **Pagination**: The emails endpoint uses cursor-based pagination with `page_trail` parameter, while other endpoints use standard offset-based pagination

## Example pipeline

Here's an example of an asset ingesting data from Plus Vibe AI to a Snowflake table:

```sql
/* @bruin

name: plusvibeai.campaigns
type: ingestr

@ingestr
source_connection: plusvibeai
source_table: campaigns

destination: snowflake

@end
*/

select * from {{ source() }}
```

The ingestr operator will automatically pull data from the `campaigns` endpoint of your Plus Vibe AI instance and load it into your target data warehouse.
