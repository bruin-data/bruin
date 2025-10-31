# Cursor

[Cursor](https://cursor.com/) is an AI-powered code editor built for productivity. The Cursor API provides access to team usage data, spending information, and detailed usage events.

To set up a Cursor connection, you need to have an API key from your Cursor team settings.

## Set up a connection

Cursor connections are defined using the following properties:

- `name`: The name to identify this connection
- `api_key`: Your Cursor API key (required)

:::code-group
```yaml [connections.yml]
connections:
  cursor:
    - name: "my_cursor"
      api_key: "your_api_key_here"
```
:::

You can also use environment variables in your connections.yml by using the `&#123;&#123; env_var("ENV_VAR_NAME") &#125;&#125;` syntax.

For example:
```yaml
connections:
  cursor:
    - name: "my_cursor"
      api_key: "&#123;&#123; env_var('CURSOR_API_KEY') &#125;&#125;"
```

## Supported Data Assets

Cursor assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset                  | Table Name             | Incremental Key | Description                                                                  |
|------------------------|------------------------|-----------------|------------------------------------------------------------------------------|
| Team Members           | `team_members`         | replace         | Team member information including names, emails, and roles                   |
| Daily Usage Data       | `daily_usage_data`     | replace         | Daily usage statistics including lines added/deleted, AI requests, model usage |
| Team Spend             | `team_spend`           | replace         | Team spending data for the current billing cycle                             |
| Filtered Usage Events  | `filtered_usage_events`| replace         | Detailed usage events with timestamps, models, token usage, and costs        |

## Asset-Specific Configuration

### Full Refresh Assets
All Cursor tables use full refresh mode:
- `team_members`
- `daily_usage_data`
- `team_spend`
- `filtered_usage_events`

### Optional Date Filtering
`daily_usage_data` and `filtered_usage_events` tables support optional date filtering:
- When dates are provided, only data within that range is fetched
- When dates are omitted, the API returns default data (typically last 30 days)
- **Important:** Date range cannot exceed 30 days

## Notes

- **Authentication**: The Cursor API uses API key authentication.
- **Rate Limits**: The source handles rate limiting and server errors automatically.
- **Pagination**: The source handles pagination automatically (100 records per page by default).
- **Date Range Limit**: The `daily_usage_data` and `filtered_usage_events` endpoints have a 30-day limit per request. If you need more than 30 days of historical data, make multiple requests with different date ranges.
- **Endpoints**:
  - `team_members` uses a GET endpoint
  - All other endpoints use POST with JSON payloads
- **Data Format**: All data is returned with flat schema for easier querying.
