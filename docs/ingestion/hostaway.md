# Hostaway

[Hostaway](https://www.hostaway.com/) is a property management system (PMS) designed for vacation rental managers and hosts. It provides tools for managing listings, reservations, channels, and guest communications across multiple booking platforms.

To set up a Hostaway connection, you need to have an API access token generated through OAuth 2.0 client credentials authentication.

## Set up a connection

Hostaway connections are defined using the following properties:

- `name`: The name to identify this connection
- `api_key`: Your Hostaway API access token (required)

:::code-group
```yaml [connections.yml]
connections:
  hostaway:
    - name: "my_hostaway"
      api_key: "your_access_token_here"
```
:::

You can also use environment variables in your connections.yml by using the `&#123;&#123; env_var("ENV_VAR_NAME") &#125;&#125;` syntax.

For example:
```yaml
connections:
  hostaway:
    - name: "my_hostaway"
      api_key: "&#123;&#123; env_var('HOSTAWAY_API_KEY') &#125;&#125;"
```

## Getting Your API Access Token

Hostaway uses OAuth 2.0 client credentials for authentication. Follow these steps to obtain an API access token:

### 1. Get Your Credentials

First, you need your Hostaway account credentials:
- `client_id`: Your Hostaway account ID
- `client_secret`: Your API client secret (available in Hostaway settings)

### 2. Generate an Access Token

Use the following curl command to generate an access token:

```bash
curl -X POST https://api.hostaway.com/v1/accessTokens \
  -H 'Cache-control: no-cache' \
  -H 'Content-type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=YOUR_ACCOUNT_ID&client_secret=YOUR_CLIENT_SECRET&scope=general'
```

The response will contain an access token (JWT) that you'll use as your `api_key` in the connection configuration.

### 3. Revoking Access Tokens

To revoke an access token when it's no longer needed:

```bash
curl -X DELETE 'https://api.hostaway.com/v1/accessTokens?token=YOUR_ACCESS_TOKEN' \
  -H 'Content-type: application/x-www-form-urlencoded'
```

## Supported Data Assets

Hostaway assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset                           | Table Name                      | Incremental Key  | Description                                                    |
|---------------------------------|---------------------------------|------------------|----------------------------------------------------------------|
| Listings                        | `listings`                      | latestActivityOn | Property listings managed in Hostaway                          |
| Listing Fee Settings            | `listing_fee_settings`          | updatedOn        | Fee settings configured for each listing                       |
| Listing Pricing Settings        | `listing_pricing_settings`      | replace          | Pricing rules and settings for listings                        |
| Listing Agreements              | `listing_agreements`            | replace          | Rental agreements associated with listings                     |
| Listing Calendars               | `listing_calendars`             | replace          | Calendar availability data for each listing                    |
| Cancellation Policies           | `cancellation_policies`         | replace          | General cancellation policies                                  |
| Cancellation Policies Airbnb    | `cancellation_policies_airbnb`  | replace          | Airbnb-specific cancellation policies                          |
| Cancellation Policies Marriott  | `cancellation_policies_marriott`| replace          | Marriott-specific cancellation policies                        |
| Cancellation Policies VRBO      | `cancellation_policies_vrbo`    | replace          | VRBO-specific cancellation policies                            |
| Reservations                    | `reservations`                  | replace          | Booking reservations across all channels                       |
| Finance Fields                  | `finance_fields`                | replace          | Financial data for each reservation                            |
| Reservation Payment Methods     | `reservation_payment_methods`   | replace          | Available payment methods for reservations                     |
| Reservation Rental Agreements   | `reservation_rental_agreements` | replace          | Rental agreements for specific reservations                    |
| Conversations                   | `conversations`                 | replace          | Guest communication threads                                    |
| Message Templates               | `message_templates`             | replace          | Pre-configured message templates                               |
| Bed Types                       | `bed_types`                     | replace          | Available bed type configurations                              |
| Property Types                  | `property_types`                | replace          | Property type classifications                                  |
| Countries                       | `countries`                     | replace          | Supported countries and their codes                            |
| Account Tax Settings            | `account_tax_settings`          | replace          | Tax configuration for the account                              |
| User Groups                     | `user_groups`                   | replace          | User groups and permissions                                    |
| Guest Payment Charges           | `guest_payment_charges`         | replace          | Guest payment transaction records                              |
| Coupons                         | `coupons`                       | replace          | Discount coupons and promotional codes                         |
| Webhook Reservations            | `webhook_reservations`          | replace          | Webhook configurations for reservation events                  |
| Tasks                           | `tasks`                         | replace          | Tasks and to-dos within the system                             |


## Notes

- **Authentication**: Hostaway uses OAuth 2.0 client credentials authentication. Access tokens are JWTs with configurable expiration times - manage them securely and rotate them as needed.
- **Incremental Loading**: Only `listings` and `listing_fee_settings` support incremental loading. Use `--interval-start` and `--interval-end` parameters for these tables.
- **API Documentation**: More details on the Hostaway API can be found in the [official API documentation](https://api-docs.hostaway.com/).
- **Rate Limits**: Be aware of Hostaway API rate limits when ingesting large amounts of data.

