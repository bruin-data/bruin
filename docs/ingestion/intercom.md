# Intercom

[Intercom](https://www.intercom.com/) is a customer messaging platform that helps businesses engage with their customers through personalized, messenger-based experiences. This source enables you to extract comprehensive data from the Intercom API, including contacts, companies, conversations, tickets, and more.

To set up an Intercom connection, you need to have an Access Token that has the necessary permissions for the resources you want to access.

## Set up a connection

Intercom connections are defined using the following properties:

- `name`: The name to identify this connection
- `access_token`: Your Intercom Access Token (required)
- `region`: The Intercom region for your app (optional, defaults to "us"). Supported regions: "us", "eu", "au"

:::code-group
```yaml [connections.yml]
connections:
  intercom:
    - name: "my_intercom"
      access_token: "your_access_token_here"
      region: "us"
```
:::

You can also use environment variables in your connections.yml by using the `&#123;&#123; env_var("ENV_VAR_NAME") &#125;&#125;` syntax.

For example:
```yaml
connections:
  intercom:
    - name: "my_intercom"
      access_token: "&#123;&#123; env_var('INTERCOM_ACCESS_TOKEN') &#125;&#125;"
      region: "us"
```

## Supported Data Assets

Intercom assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset | Table Name | Incremental Key | Description |
|-------|------------|-----------------|-------------|
| Contacts | `contacts` | updated_at | Customer contacts with their attributes, tags, and company associations |
| Companies | `companies` | updated_at | Company information including custom attributes and tags |
| Conversations | `conversations` | updated_at | Customer conversations with messages, state, and metadata |
| Tickets | `tickets` | updated_at | Customer support tickets with attributes and resolution details |
| Articles | `articles` | updated_at | Help center articles and knowledge base content |
| Tags | `tags` | replace | All available tags in your Intercom workspace |
| Segments | `segments` | replace | Customer segments and their definitions |
| Teams | `teams` | replace | Support and sales teams configuration |
| Admins | `admins` | replace | Admin users and their permissions |
| Data Attributes | `data_attributes` | replace | Custom data attributes definitions for contacts and companies |

## Asset-Specific Configuration

### Incremental Assets
The following tables support incremental loading based on the `updated_at` field:
- `contacts`
- `companies`
- `conversations`
- `tickets`
- `articles`

### Full Refresh Assets
The following tables use full refresh mode as they represent current state data:
- `tags`
- `segments`
- `teams`
- `admins`
- `data_attributes`

## Notes

- **Authentication**: The Intercom API uses Bearer token authentication with Access Tokens.
- **Regional Endpoints**: Intercom supports regional data centers. Specify the correct region ("us", "eu", "au") for your Intercom app.
- **API Version**: This source uses Intercom API version 2.14.
- **Rate Limits**: The Intercom API has rate limits (166 requests per 10 seconds). The source handles these automatically.
- **Pagination**: The source handles pagination automatically using cursor-based pagination where supported.
- **Custom Attributes**: Custom attributes for contacts, companies, and conversations are included as JSON fields.
- **Search API**: For large datasets, the source uses Intercom's Search API for contacts, companies, and conversations to support efficient incremental loading.
- **Data Freshness**: Data is typically available in real-time through the Intercom API.

