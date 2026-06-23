# Intercom

[Intercom](https://www.intercom.com/) is a customer messaging platform that helps businesses engage with their customers through personalized, messenger-based experiences. This source enables you to extract comprehensive data from the Intercom API, including contacts, companies, conversations, articles, and more.

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

You can also use environment variables in your connections.yml by using the `${VAR_NAME}` syntax.

For example:

```yaml
connections:
  intercom:
    - name: "my_intercom"
      access_token: ${INTERCOM_ACCESS_TOKEN}
      region: "us"
```

## Available Source Tables

Intercom assets will be ingested to your data warehouse as defined in the `destination` table.


| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `contacts` | id | updated_at | merge | Retrieves information about contacts (visitors, users, and leads) |
| `companies` | id | updated_at | merge | Retrieves information about companies |
| `conversations` | id | updated_at | merge | Retrieves conversation data |
| `articles` | id | updated_at | merge | Retrieves help center articles |
| `tags` | id | - | replace | Retrieves tags used to organize contacts and companies |
| `segments` | id | - | replace | Retrieves segments for filtering contacts and companies |
| `admins` | id | - | replace | Retrieves admin user information |
| `teams` | id | - | replace | Retrieves team information |
| `data_attributes` | name | - | replace | Retrieves data attributes (both built-in and custom attributes) |

## Asset-Specific Configuration

### Incremental Assets

The following tables support incremental loading based on the `updated_at` field:

- `contacts`
- `companies`
- `conversations`
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
