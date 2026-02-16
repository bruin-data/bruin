# Secrets

Secrets are custom credentials—API keys, passwords, tokens, and other sensitive values—that can be injected into your assets during execution. They complement [connections](/core-concepts/connections) for cases where you need direct access to credentials in your code.

## Overview

There are two main approaches to managing secrets in Bruin:

1. **Generic Connections**: Key-value pairs defined in `.bruin.yml` and injected as environment variables
2. **External Secret Providers**: Integration with secret management solutions like Vault, Doppler, or AWS Secrets Manager

## Generic Connections

Generic connections are key-value pairs that inject secrets into your assets:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      generic:
        - name: MY_API_KEY
          value: "sk-abc123..."
        - name: SLACK_WEBHOOK
          value: "https://hooks.slack.com/..."
```

Access these in your Python assets via environment variables:

```python
"""@bruin
name: my_api_asset
secrets:
  - key: MY_API_KEY
@bruin"""

import os

api_key = os.environ["MY_API_KEY"]
# Use the API key...
```

## Injecting Secrets into Assets

To inject secrets into an asset, specify them in the asset definition:

```python
"""@bruin
name: tier1.external_data
secrets:
  - key: MY_API_KEY
  - key: SLACK_WEBHOOK
    inject_as: WEBHOOK_URL
@bruin"""

import os

api_key = os.environ["MY_API_KEY"]
webhook = os.environ["WEBHOOK_URL"]  # Injected with a different name
```

The `inject_as` field allows you to rename the environment variable.

## Pipeline-Level Secrets

You can define default secrets at the pipeline level that apply to all assets:

```yaml
# pipeline.yml
name: analytics-daily

default:
  secrets:
    - key: MY_API_KEY
      inject_as: API_KEY
    - key: DATABASE_PASSWORD
```

## External Secret Providers

For production environments, Bruin supports external secret management solutions:

| Provider | Documentation |
|----------|---------------|
| HashiCorp Vault | [Vault Integration](/secrets/vault) |
| Doppler | [Doppler Integration](/secrets/doppler) |
| AWS Secrets Manager | [AWS Secrets Manager](/secrets/aws-secrets-manager) |

### Using an External Provider

Specify the secrets backend when running:

```bash
# Use Doppler
bruin run --secrets-backend doppler

# Use Vault
bruin run --secrets-backend vault
```

Or set via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=doppler
bruin run
```

## Environment Variables in .bruin.yml

Reference system environment variables in your `.bruin.yml` configuration:

```yaml
environments:
  default:
    connections:
      postgres:
        - name: my_postgres
          username: ${POSTGRES_USERNAME}
          password: ${POSTGRES_PASSWORD}
          host: ${POSTGRES_HOST}
```

Environment variables are expanded at runtime, allowing you to:

- Keep sensitive values out of configuration files
- Use different values in different deployment environments
- Integrate with CI/CD secret injection

## Best Practices

1. **Never commit secrets**: Ensure `.bruin.yml` is in `.gitignore`
2. **Use environment variables**: Reference `${VAR}` syntax instead of hardcoding values
3. **Minimize secret scope**: Only inject secrets into assets that need them
4. **Use external providers in production**: Vault, Doppler, or AWS Secrets Manager provide better security and auditing

## Related Topics

- [Environments](/core-concepts/environments) - Configure multiple environments
- [Connections](/core-concepts/connections) - Built-in platform connections
- [.bruin.yml Reference](/secrets/bruinyml) - Complete configuration reference
