# Environments

Environments are configuration contexts that define how Bruin connects to external systems. They enable you to run the same pipeline code against different targets—for example, a development database during testing and a production database in deployment.

All environment configuration is stored in the `.bruin.yml` file, which is automatically created when you first run Bruin and added to `.gitignore` to keep credentials secure.

## Overview

An environment consists of:

- **[Connections](/core-concepts/connections)**: Built-in connection configurations for data platforms and ingestion sources
- **[Secrets](/core-concepts/secrets)**: Custom credentials and API keys for injection into assets
- **Schema prefix**: Optional prefix for schema-based environment isolation

## File Structure

The `.bruin.yml` file is expected to be in the root of your Git repository. You can specify a different location using `--config-file /path/to/.bruin.yml`.

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
  
  staging:
    schema_prefix: "stg_"
    connections:
      postgres:
        - name: "postgres-staging"
          username: "STAGING_POSTGRES_USER"
          password: "STAGING_POSTGRES_PASSWORD"
          host: "staging-db.example.com"
          port: 5432
          database: "analytics"

  production:
    connections:
      postgres:
        - name: "postgres-prod"
          username: "PROD_POSTGRES_USER"
          password: "PROD_POSTGRES_PASSWORD"
          host: "prod-db.example.com"
          port: 5432
          database: "analytics"
```

## Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `default_environment` | string | No | Environment to use when none is specified. Defaults to `default`. |
| `environments` | map | Yes | Map of environment names to their configurations. |

## Environment Configuration

Each environment contains:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connections` | object | Yes | Connection definitions grouped by type. |
| `schema_prefix` | string | No | Prefix added to schema names (useful for dev/staging environments). |

## Using Environments

### Switching Environments

Use the `--environment` flag to run pipelines against a specific environment:

```bash
# Run against the default environment
bruin run

# Run against staging
bruin run --environment staging

# Run against production
bruin run --environment production
```

### Environment Variables

Use environment variables to keep credentials out of `.bruin.yml`. This example uses literal values for clarity:

```yaml
environments:
  default:
    connections:
      postgres:
        - name: my_postgres
          username: "bruin_user"
          password: "super_secret"
          host: "db.example.com"
          port: 5432
          database: "analytics"
```

> [!NOTE]
> You can reference environment variables in connection fields using `${VAR_NAME}` placeholders, which are expanded at runtime (not when the file is parsed).

## Local vs Cloud

### Local Development

For local development, Bruin reads credentials from your local `.bruin.yml` file. This is the simplest setup:

1. Run `bruin init` or any `bruin` command to create `.bruin.yml`
2. Add your connections to the file
3. Run `bruin run` to execute your pipeline

### Cloud Deployment

When deploying to Bruin Cloud or other environments, you have several options:

1. **Environment Variables**: Reference environment variables in `.bruin.yml` that are set in your deployment environment
2. **Secret Providers**: Use external secret managers like [HashiCorp Vault](/secrets/vault), [Doppler](/secrets/doppler), or [AWS Secrets Manager](/secrets/aws-secrets-manager)
3. **CI/CD Integration**: Inject secrets during CI/CD pipeline execution

See the [Deployment](/deployment/vm-deployment) documentation for platform-specific guidance.

## Schema-Based Environments

For scenarios where separate databases are impractical, Bruin supports schema-based environments. This automatically prefixes schema names to isolate environments within the same database. If the prefix does not end with `_`, Bruin appends it for you.

```yaml
environments:
  dev_jane:
    schema_prefix: jane_
    connections:
      postgres:
        - name: "postgres-dev"
          # ...connection details...
```

When running with this environment, an asset named `mart.customers` becomes `jane_mart.customers`.

[Learn more about schema-based environments →](/getting-started/devenv#schema-based-environments)

## Related Topics

- [Connections](/core-concepts/connections) - Configure connections to data platforms
- [Secrets](/core-concepts/secrets) - Manage custom credentials and API keys
- [.bruin.yml Reference](/secrets/bruinyml) - Complete configuration reference
