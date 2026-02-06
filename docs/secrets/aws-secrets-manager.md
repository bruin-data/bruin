# Using AWS Secrets Manager as a Secrets Backend

Bruin supports using [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag on the `run` command.

## Enabling AWS Secrets Manager

To use AWS Secrets Manager as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend aws
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=aws
bruin run
```

## Configuring AWS Connection

Bruin connects to AWS Secrets Manager using environment variables. The following are required:

- `BRUIN_AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `BRUIN_AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `BRUIN_AWS_REGION`: The AWS region where your secrets are stored (e.g., `us-east-1`, `eu-west-1`)

The following is optional:

- `BRUIN_AWS_SESSION_TOKEN`: A session token for temporary credentials (e.g., when using AWS STS)

### Example Setup

```bash
export BRUIN_AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export BRUIN_AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export BRUIN_AWS_REGION=us-east-1

bruin run --secrets-backend aws
```

### Using Temporary Credentials

If you are using temporary credentials (e.g., from AWS STS AssumeRole), you can also set the session token:

```bash
export BRUIN_AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export BRUIN_AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export BRUIN_AWS_REGION=us-east-1
export BRUIN_AWS_SESSION_TOKEN=FwoGZXIvYXdzE...

bruin run --secrets-backend aws
```

## Storing Secrets in AWS Secrets Manager

Bruin expects connection credentials to be stored in AWS Secrets Manager as JSON strings. Each secret should be named after the connection name and contain the connection details in a specific format.

### Secret Format

The secret value must be a JSON string with two required fields:

- `type`: The connection type (must match a valid Bruin connection type)
- `details`: An object containing the connection-specific configuration

### Example: PostgreSQL Connection

In AWS Secrets Manager, create a secret named `my-postgres` with this value:

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

In AWS Secrets Manager, create a secret named `my-snowflake` with this value:

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

In AWS Secrets Manager, create a secret named `my-bigquery` with this value:

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

For a complete list of supported connection types and their configuration schemas, see the [connections documentation](../getting-started/introduction/quickstart.md#setting-up-your-bruinyml-file).

## How It Works

When you run Bruin with `--secrets-backend aws`:

1. Bruin connects to AWS Secrets Manager using your credentials
2. For each connection referenced in your pipeline, Bruin fetches the corresponding secret by name
3. The secret is parsed and validated according to the connection type
4. The connection is established using the fetched credentials
5. Results are cached in memory for the duration of the run

## Troubleshooting

### Environment Variables Not Set

If you see an error like:
```
failed to initialize AWS Secrets Manager client: BRUIN_AWS_ACCESS_KEY_ID env variable not set
```

Make sure all required environment variables are set:
```bash
echo $BRUIN_AWS_ACCESS_KEY_ID
echo $BRUIN_AWS_SECRET_ACCESS_KEY
echo $BRUIN_AWS_REGION
```

### Secret Not Found

If you see an error like:
```
failed to read secret 'my-connection' from AWS Secrets Manager
```

Verify that:
1. The secret exists in AWS Secrets Manager with the exact name used in your pipeline
2. The secret is in the correct AWS region
3. Your AWS credentials have the `secretsmanager:GetSecretValue` permission for that secret

### Invalid Secret Format

If you see an error like:
```
failed to parse secret as JSON
```

Verify that:
1. The secret value in AWS Secrets Manager is valid JSON
2. The JSON includes both `type` and `details` fields
3. The `type` value matches a supported connection type
4. The `details` object contains all required fields for that connection type

### Secret Has No String Value

If you see an error like:
```
secret 'my-connection' has no string value
```

Make sure the secret is stored as a plaintext string (not binary) in AWS Secrets Manager.
