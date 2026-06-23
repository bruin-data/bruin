# FundraiseUp

[FundraiseUp](https://fundraiseup.com/) is a modern donation platform that helps non-profits increase their online fundraising revenue.

To set up a FundraiseUp connection, you need to have an API key that has the necessary permissions for the resources you want to access.

## Set up a connection

FundraiseUp connections are defined using the following properties:

- `name`: The name to identify this connection
- `api_key`: Your FundraiseUp API key (required)

:::code-group

```yaml [connections.yml]
connections:
  fundraiseup:
    - name: "my_fundraiseup"
      api_key: "your_api_key"
```

:::

You can also use environment variables in your connections.yml by using the `${VAR_NAME}` syntax.

For example:

```yaml
connections:
  fundraiseup:
    - name: "my_fundraiseup"
      api_key: ${FUNDRAISEUP_API_KEY}
```

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `donations` | id | - | replace | All donation records including amounts, supporters, and payment details |
| `donations:incremental` | id | - | merge | All donation records including amounts, supporters, and payment details. Loads records incrementally. |
| `events` | id | - | replace | Audit log events for tracking changes and activities |
| `fundraisers` | id | - | replace | Fundraiser campaigns (requires appropriate API permissions) |
| `recurring_plans` | id | - | replace | Recurring donation plans and subscription details |
| `supporters` | id | - | replace | Donor/supporter information including contact details |

## Notes

- **Authentication**: The FundraiseUp API uses Bearer token authentication. Make sure your API key has the necessary permissions for the resources you want to access.
- **Incremental Loading**: The FundraiseUp source does not support incremental loading. All data is fetched with a full refresh (replace strategy).
- **Date Filtering**: The API does not support date filtering for any of the endpoints.
- **Permissions**: The `fundraisers` endpoint may return a 403 Forbidden error if your API key doesn't have the required permissions. Contact FundraiseUp support to enable access if needed.
