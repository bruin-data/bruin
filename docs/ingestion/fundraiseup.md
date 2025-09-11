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

You can also use environment variables in your connections.yml by using the `{{ env_var("ENV_VAR_NAME") }}` syntax.

For example:
```yaml
connections:
  fundraiseup:
    - name: "my_fundraiseup"
      api_key: "{{ env_var('FUNDRAISEUP_API_KEY') }}"
```

## Supported Data Assets

FundraiseUp assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset                | Table Name        | Incremental Key | Description                                                                                                                                        |
|----------------------|-------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| Donations            | `donations`       | replace         | All donation records including amounts, supporters, and payment details                                                                           |
| Events               | `events`          | replace         | Audit log events for tracking changes and activities                                                                                              |
| Fundraisers          | `fundraisers`     | replace         | Fundraiser campaigns (requires appropriate API permissions)                                                                                        |
| Recurring Plans      | `recurring_plans` | replace         | Recurring donation plans and subscription details                                                                                                  |
| Supporters           | `supporters`      | replace         | Donor/supporter information including contact details                                                                                              |

## Asset-Specific Configuration

All FundraiseUp assets use the full refresh strategy (replace) as incremental loading is not supported by the FundraiseUp API.

## Notes

- **Authentication**: The FundraiseUp API uses Bearer token authentication. Make sure your API key has the necessary permissions for the resources you want to access.
- **Incremental Loading**: The FundraiseUp source does not support incremental loading. All data is fetched with a full refresh (replace strategy).
- **Date Filtering**: The API does not support date filtering for any of the endpoints.
- **Permissions**: The `fundraisers` endpoint may return a 403 Forbidden error if your API key doesn't have the required permissions. Contact FundraiseUp support to enable access if needed.

## Example pipeline

Here's an example of an asset ingesting data from FundraiseUp to a Snowflake table:

```sql
/* @bruin

name: fundraiseup.donations
type: ingestr

@ingestr
source_connection: fundraiseup
source_table: donations

destination: snowflake

@end
*/

select * from {{ source() }}
```

The ingestr operator will automatically pull data from the `donations` endpoint of your FundraiseUp instance and load it into your target data warehouse.