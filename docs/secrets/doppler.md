# Using Doppler as a Secrets Backend

Bruin supports using [Doppler](https://www.doppler.com/) as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag on the `run` command.

## Enabling Doppler

To use Doppler as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend doppler
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=doppler
bruin run
```

## Configuring Doppler Connection

Bruin connects to Doppler using environment variables. The following are required:

- `BRUIN_DOPPLER_TOKEN`: Your Doppler service token (create one in your Doppler project settings)
- `BRUIN_DOPPLER_PROJECT`: The Doppler project name
- `BRUIN_DOPPLER_CONFIG`: The Doppler config name (e.g., `dev`, `prod`, `staging`)

### Example Setup

```bash
export BRUIN_DOPPLER_TOKEN=dp.st.your-token-here
export BRUIN_DOPPLER_PROJECT=my-data-project
export BRUIN_DOPPLER_CONFIG=dev

bruin run --secrets-backend doppler
```

## Storing Secrets in Doppler

Bruin expects connection credentials to be stored in Doppler as JSON strings. Each secret should be named after the connection name and contain the connection details in a specific format.

### Secret Format

The secret value must be a JSON string with two required fields:

- `type`: The connection type (must match a valid Bruin connection type)
- `details`: An object containing the connection-specific configuration

### Example: PostgreSQL Connection

In Doppler, create a secret named `my-postgres` with this value:

```json
{
  "type": "postgres",
  "details": {
    "host": "localhost",
    "port": 5432,
    "username": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
    "schema": "public"
  }
}
```

### Example: Snowflake Connection

In Doppler, create a secret named `my-snowflake` with this value:

```json
{
  "type": "snowflake",
  "details": {
    "account": "my-account",
    "username": "myuser",
    "password": "mypassword",
    "warehouse": "my-warehouse",
    "database": "my-database",
    "schema": "my-schema"
  }
}
```

### Example: Google BigQuery Connection

In Doppler, create a secret named `my-bigquery` with this value:

```json
{
  "type": "google_cloud_platform",
  "details": {
    "project_id": "my-gcp-project",
    "service_account_file": "/path/to/service-account.json"
  }
}
```

## Supported Connection Types

The `type` field must be one of the valid Bruin connection types. Common types include:

- `postgres` - PostgreSQL database
- `mysql` - MySQL database
- `snowflake` - Snowflake data warehouse
- `google_cloud_platform` - Google BigQuery
- `redshift` - AWS Redshift
- `databricks` - Databricks
- `generic` - Generic key-value secrets

For a complete list of supported connection types and their configuration schemas, see the [connections documentation](../getting-started/introduction/quickstart.md#bruin-yml).

## How It Works

When you run Bruin with `--secrets-backend doppler`:

1. Bruin connects to Doppler using your credentials
2. For each connection referenced in your pipeline, Bruin fetches the corresponding secret from Doppler
3. The secret is parsed and validated according to the connection type
4. The connection is established using the fetched credentials
5. Results are cached in memory for the duration of the run

## Troubleshooting

### Environment Variables Not Set

If you see an error like:
```
failed to initialize doppler client: BRUIN_DOPPLER_TOKEN env variable not set
```

Make sure all three required environment variables are set:
```bash
echo $BRUIN_DOPPLER_TOKEN
echo $BRUIN_DOPPLER_PROJECT
echo $BRUIN_DOPPLER_CONFIG
```

### Secret Not Found

If you see an error like:
```
secret 'my-connection' not found in Doppler
```

Verify that:
1. The secret exists in Doppler with the exact name used in your pipeline
2. The secret is in the correct project and config
3. Your Doppler token has access to read the secret

### Invalid Secret Format

If you see an error like:
```
failed to parse secret as JSON
```

Verify that:
1. The secret value in Doppler is valid JSON
2. The JSON includes both `type` and `details` fields
3. The `type` value matches a supported connection type
4. The `details` object contains all required fields for that connection type
