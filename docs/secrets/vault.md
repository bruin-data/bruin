# Using HashiCorp Vault as a Secrets Backend

Bruin supports using [HashiCorp Vault](https://www.vaultproject.io/) with a **kv generic secrets engine** as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag on the `run` command.

## Enabling Vault

To use Vault as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend vault
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=vault
```

## Configuring Vault Connection

Bruin connects to Vault using environment variables. The following are required:

- `BRUIN_VAULT_HOST`: The URL of your Vault server (e.g., `https://vault.example.com:8200`)
- `BRUIN_VAULT_MOUNT_PATH`: The path of the kv secrets engine
- `BRUIN_VAULT_PATH`: The subpath within the engine to where the secrets are
- either `BRUIN_VAULT_TOKEN` or `BRUIN_VAULT_ROLE`: The authentication token for Vault access, or if you are running Bruin inside a Kubernetes cluster, you can use a Kubernetes role for authentication with Vault by setting the `BRUIN_VAULT_ROLE` environment variable in your pod or deployment.
- `BRUIN_VAULT_K8S_AUTH_MOUNT` (optional): The Kubernetes auth mount path in Vault. Defaults to `"kubernetes"` if not set.

Bruin automatically retries transient network errors and Vault responses such as HTTP 429, 412, and most 5xx errors. Retries use exponential backoff with jitter and honor `Retry-After` responses. Authentication and secret reads share one overall timeout per operation, so retry waits cannot make a request hang indefinitely.

The retry and timeout settings can be tuned with these optional environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `BRUIN_VAULT_TIMEOUT` | `30s` | Overall timeout for one Vault authentication or secret-read operation, including retries. Uses Go duration syntax such as `500ms`, `10s`, or `1m`. |
| `BRUIN_VAULT_MAX_RETRIES` | `3` | Maximum retries after the initial request. Set to `0` to disable retries. |
| `BRUIN_VAULT_RETRY_WAIT_MIN` | `200ms` | Minimum backoff used to calculate retry delays. |
| `BRUIN_VAULT_RETRY_WAIT_MAX` | `2s` | Maximum backoff used to calculate retry delays. Must be at least the minimum. |

Use an `https://` Vault address for remote clusters so Vault tokens and secret data are encrypted in transit. Plain HTTP remains available for local Vault agents and development servers.

## Storing Secrets in Vault

Bruin expects connection credentials to be stored in Vault using a path convention based on the connection name. By default, secrets are stored at `{BRUIN_VAULT_MOUNT_PATH}/data/{BRUIN_VAULT_PATH}/{secret/connection name}`.

The content of the secret should follow a certain format. For example for a postgres connection it should be :

```json
{
  "details": {
    "database": "some-postgres",
    "host": "some.host.com",
    "password": "xxxxxxxxxx",
    "port": 5432,
    "schema": "public",
    "username": "some-user"
  },
  "type": "postgres"
}
```

As you see the schema is the same as in [`bruin.yml`](../getting-started/introduction/quickstart.md#bruin-yml). It's required that the data is inside a `details` attribute and `type` contains a valid connection type, that can take the same values as the types in the connection lists in [`bruin.yml`](../getting-started/introduction/quickstart.md#bruin-yml), e.g `databricks`, `postgres`, `athena`....
