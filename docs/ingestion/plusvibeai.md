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

You can also use environment variables in your connections.yml by using the `${VAR_NAME}` syntax.

For example:

```yaml
connections:
  plusvibeai:
    - name: "my_plusvibeai"
      api_key: ${PLUSVIBEAI_API_KEY}
      workspace_id: ${PLUSVIBEAI_WORKSPACE_ID}
```

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `campaigns` | id | modified_at | merge | Contains campaign information including configuration, schedules, sequences, and performance metrics. Nested objects (schedule, sequences) are stored as JSON columns. |
| `leads` | _id | modified_at | merge | Contains lead information including contact details, campaign association, engagement metrics, and professional information. |
| `email_accounts` | _id | timestamp_updated | merge | Contains email account configurations including SMTP/IMAP settings, warmup configurations, and analytics data stored in payload JSON. |
| `emails` | id | timestamp_created | merge | Contains email data including message content, headers, thread information, and recipient details. Uses cursor-based pagination. |
| `blocklist` | _id | created_at | merge | Contains blocklist entries for email addresses or domains that should be excluded from campaigns. |
| `webhooks` | _id | modified_at | merge | Contains webhook configurations for receiving real-time notifications about campaign events and lead interactions. |
| `tags` | _id | modified_at | merge | Contains tag information used for organizing and categorizing campaigns, leads, and other resources. |

## Asset-Specific Configuration

Plus Vibe AI assets support incremental loading based on modification timestamps. Each table uses its respective timestamp field to fetch only updated records since the last sync.

### Nested Data Handling

The source preserves nested objects as JSON columns to maintain data structure integrity:

- **Campaigns**: Schedule, sequences, and events are stored as JSON
- **Email Accounts**: All configuration data is stored in the `payload` JSON field
- **Emails**: Headers and address information are stored as JSON

## Notes

- **Authentication**: Get your API key from <https://app.plusvibe.ai/v2/settings/api-access/>
- **Incremental Loading**: Supported for all tables using their respective timestamp fields
- **Rate Limiting**: Plus Vibe AI API has a rate limit of 5 requests per second. The source automatically handles rate limiting with exponential backoff and retry logic
- **Pagination**: The emails endpoint uses cursor-based pagination with `page_trail` parameter, while other endpoints use standard offset-based pagination
