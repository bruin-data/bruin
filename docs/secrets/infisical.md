# Using Infisical as a Secrets Backend

Bruin supports using [Infisical](https://infisical.com/) as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag on the `run` command.

## Enabling Infisical

To use Infisical as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend infisical
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=infisical
bruin run
```

## Configuring Infisical Connection

Bruin connects to Infisical using Universal Auth (Machine Identities). You'll need to configure the following environment variables:

### Required Environment Variables

- `BRUIN_INFISICAL_CLIENT_ID`: Your Infisical Machine Identity Client ID
- `BRUIN_INFISICAL_CLIENT_SECRET`: Your Infisical Machine Identity Client Secret
- `BRUIN_INFISICAL_PROJECT_ID`: The Infisical project ID
- `BRUIN_INFISICAL_ENVIRONMENT`: The environment name (e.g., `dev`, `prod`, `staging`)

### Optional Environment Variables

- `BRUIN_INFISICAL_HOST`: Infisical API URL (default: `https://app.infisical.com`)
- `BRUIN_INFISICAL_SECRET_PATH`: Path to secrets (default: `/`)

### Example Setup

```bash
export BRUIN_INFISICAL_CLIENT_ID=your-client-id-here
export BRUIN_INFISICAL_CLIENT_SECRET=your-client-secret-here
export BRUIN_INFISICAL_PROJECT_ID=your-project-id
export BRUIN_INFISICAL_ENVIRONMENT=dev

bruin run --secrets-backend infisical
```

## Setting Up Machine Identity (Universal Auth)

1. Go to your Infisical project settings
2. Navigate to **Access Control** > **Machine Identities**
3. Click **Create Identity** and give it a name
4. Note the **Client ID** and **Client Secret** - these are your credentials
5. Configure the identity's access to specific environments and secret paths

For detailed instructions, see [Infisical's Machine Identity documentation](https://infisical.com/docs/documentation/platform/identities/universal-auth).

## Storing Secrets in Infisical

Bruin expects connection credentials to be stored in Infisical as JSON strings. Each secret should be named after the connection name and contain the connection details in a specific format.

### Secret Format

The secret value must be a JSON string with two required fields:

- `type`: The connection type (must match a valid Bruin connection type)
- `details`: An object containing the connection-specific configuration

### Example: PostgreSQL Connection

In Infisical, create a secret named `my-postgres` with this value:

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

In Infisical, create a secret named `my-snowflake` with this value:

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

In Infisical, create a secret named `my-bigquery` with this value:

```json
{
  "type": "google_cloud_platform",
  "details": {
    "project_id": "my-gcp-project",
    "service_account_file": "/path/to/service-account.json"
  }
}
```

### Example: Generic Secret

For simple key-value secrets:

```json
{
  "type": "generic",
  "details": {
    "value": "my-secret-value"
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
- `mongodb` - MongoDB
- `mssql` - Microsoft SQL Server
- `generic` - Generic key-value secrets

For a complete list of supported connection types and their configuration schemas, see the [connections documentation](../getting-started/introduction/quickstart.md#bruin-yml).

## How It Works

When you run Bruin with `--secrets-backend infisical`:

1. Bruin connects to Infisical using Universal Auth (Machine Identity credentials)
2. The SDK automatically handles token refresh for long-running operations
3. For each connection referenced in your pipeline, Bruin fetches the corresponding secret from Infisical
4. The secret is parsed as JSON and validated according to the connection type
5. The connection is established using the fetched credentials
6. Results are cached in memory for the duration of the run to minimize API calls

## Troubleshooting

### Environment Variables Not Set

If you see an error like:
```
failed to initialize infisical client: BRUIN_INFISICAL_CLIENT_ID env variable not set
```

Make sure all required environment variables are set:
```bash
echo $BRUIN_INFISICAL_CLIENT_ID
echo $BRUIN_INFISICAL_CLIENT_SECRET
echo $BRUIN_INFISICAL_PROJECT_ID
echo $BRUIN_INFISICAL_ENVIRONMENT
```

### Authentication Failed

If you see an error like:
```
failed to authenticate with Infisical
```

Verify that:
1. Your Client ID and Client Secret are correct
2. The Machine Identity has access to the specified project
3. The Machine Identity has the necessary permissions for the environment
4. Your Infisical instance is accessible (check `BRUIN_INFISICAL_HOST` if using self-hosted)

### Secret Not Found

If you see an error like:
```
failed to read secret from Infisical
```

Verify that:
1. The secret exists in Infisical with the exact name used in your pipeline
2. The secret is in the correct environment (matches `BRUIN_INFISICAL_ENVIRONMENT`)
3. The secret is in the correct path (matches `BRUIN_INFISICAL_SECRET_PATH`)
4. Your Machine Identity has read access to this secret

### Invalid Secret Format

If you see an error like:
```
failed to parse secret as JSON
```

Verify that:
1. The secret value in Infisical is valid JSON
2. The JSON includes both `type` and `details` fields
3. The `type` value matches a supported connection type
4. The `details` object contains all required fields for that connection type

## Security Best Practices

1. **Use Machine Identities** (Universal Auth) - never use deprecated Service Tokens
2. **Rotate credentials regularly** - update Client Secrets periodically
3. **Apply least privilege** - grant Machine Identities only the access they need
4. **Use environment-specific identities** - separate identities for dev, staging, prod
5. **Never commit credentials** - always use environment variables
6. **Enable audit logging** in Infisical to track secret access
7. **Use secret path scoping** - organize secrets by path and restrict access accordingly
