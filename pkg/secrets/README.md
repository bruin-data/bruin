# Secrets Package

This package provides a unified interface for retrieving secrets from different secret management providers.

## Supported Providers

1. **HashiCorp Vault** - Enterprise secret management
2. **Doppler** - Modern secrets management platform
3. **.bruin.yml** - Local configuration file

## Usage

### Doppler Client

The Doppler client retrieves secrets from Doppler's API. Each secret in Doppler should be stored as a JSON string with the following structure:

```json
{
  "type": "postgres",
  "details": {
    "username": "myuser",
    "password": "mypass",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "schema": "public"
  }
}
```

#### Environment Variables

To use the Doppler client, set the following environment variables:

- `BRUIN_DOPPLER_TOKEN` - Your Doppler service token
- `BRUIN_DOPPLER_PROJECT` - The Doppler project name
- `BRUIN_DOPPLER_CONFIG` - The Doppler config name (e.g., "dev", "prod")

#### Example

```go
import (
    "github.com/bruin-data/bruin/pkg/secrets"
    "github.com/bruin-data/bruin/pkg/logger"
)

// Create client from environment variables
client, err := secrets.NewDopplerClientFromEnv(logger)
if err != nil {
    log.Fatal(err)
}

// Get a connection
conn := client.GetConnection("my-postgres-connection")

// Get connection details
details := client.GetConnectionDetails("my-postgres-connection")
```

#### Programmatic Creation

```go
// Create client with explicit parameters
client, err := secrets.NewDopplerClient(
    logger,
    "dp.st.xxxxx", // token
    "my-project",  // project
    "dev",         // config
)
```

### Vault Client

The Vault client retrieves secrets from HashiCorp Vault using KV v2 secrets engine.

#### Environment Variables

- `BRUIN_VAULT_HOST` - Vault server URL
- `BRUIN_VAULT_TOKEN` - Vault authentication token (or use BRUIN_VAULT_ROLE)
- `BRUIN_VAULT_ROLE` - Kubernetes role for authentication
- `BRUIN_VAULT_PATH` - Base path for secrets in Vault
- `BRUIN_VAULT_MOUNT_PATH` - Vault mount path

#### Example

```go
// Create client from environment variables
client, err := secrets.NewVaultClientFromEnv(logger)
if err != nil {
    log.Fatal(err)
}

// Use the same GetConnection/GetConnectionDetails interface
conn := client.GetConnection("my-connection")
```

## Secret Format

All secrets should follow this structure:

```json
{
  "type": "<connection-type>",
  "details": {
    // Connection-specific fields
  }
}
```

### Supported Connection Types

- `postgres` - PostgreSQL database
- `mysql` - MySQL database
- `snowflake` - Snowflake data warehouse
- `bigquery` / `google_cloud_platform` - Google BigQuery
- `redshift` - AWS Redshift
- `generic` - Generic key-value connection
- ... and many more (see `pkg/config/connections.go`)

### Example Secret Configurations

#### PostgreSQL
```json
{
  "type": "postgres",
  "details": {
    "host": "localhost",
    "port": 5432,
    "username": "myuser",
    "password": "mypass",
    "database": "mydb",
    "schema": "public"
  }
}
```

#### Generic Secret
```json
{
  "type": "generic",
  "details": {
    "value": "my-secret-value"
  }
}
```

#### Snowflake
```json
{
  "type": "snowflake",
  "details": {
    "account": "my-account",
    "username": "myuser",
    "password": "mypass",
    "warehouse": "my-warehouse",
    "database": "my-database",
    "schema": "my-schema"
  }
}
```

## Client Interface

Both Vault and Doppler clients implement the same interface:

```go
type Client struct {
    // GetConnection retrieves a connection configuration
    GetConnection(name string) any

    // GetConnectionDetails retrieves detailed connection information
    GetConnectionDetails(name string) any
}
```

Results are cached in memory to avoid repeated API calls.

## Implementation Details

### Caching

Both clients implement connection caching to minimize API calls:
- Connections are cached after first retrieval
- Cache is per-client instance
- No automatic cache invalidation

### Error Handling

- Missing secrets return `nil`
- API errors are logged and return `nil`
- Invalid secret format errors are logged and return `nil`

### Doppler API

The Doppler client uses the Doppler REST API v3:
- Endpoint: `https://api.doppler.com/v3/configs/config/secrets/download`
- Authentication: Bearer token
- Fetches all secrets in a config, then extracts the requested secret

### Vault Integration

The Vault client uses the HashiCorp Vault Go client:
- Supports token-based authentication
- Supports Kubernetes-based authentication
- Uses KV v2 secrets engine
- Configurable mount path and base path
