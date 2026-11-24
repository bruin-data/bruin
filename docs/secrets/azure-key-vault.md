# Using Azure Key Vault as a Secrets Backend

Bruin supports using [Azure Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/general/overview) as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag.

## Enabling Azure Key Vault

To use Azure Key Vault as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend azure
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=azure
bruin run
```

## Configuring the Azure Connection

Bruin needs the URL of your vault and a way to authenticate to Microsoft Entra ID:

```bash
export BRUIN_AZURE_KEYVAULT_URL=https://my-vault.vault.azure.net

bruin run --secrets-backend azure
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRUIN_AZURE_KEYVAULT_URL` | Yes | The vault URL, e.g. `https://my-vault.vault.azure.net`. Must use `https`. See [Sovereign clouds](#sovereign-clouds) for other hosts. |
| `BRUIN_AZURE_AUTH_METHOD` | No | How to authenticate: `default` (the default), `workload_identity`, `managed_identity`, `client_credentials`, or `cli`. See [Authentication](#authentication). |
| `BRUIN_AZURE_TENANT_ID` | For `client_credentials` | Microsoft Entra tenant ID. With `workload_identity`, overrides `AZURE_TENANT_ID`. |
| `BRUIN_AZURE_CLIENT_ID` | For `client_credentials` | Application (client) ID. With `managed_identity`, selects a user-assigned identity. With `workload_identity`, overrides `AZURE_CLIENT_ID`. |
| `BRUIN_AZURE_CLIENT_SECRET` | For `client_credentials` | Client secret of the app registration. |
| `BRUIN_AZURE_SECRET_PREFIX` | No | Prepended to connection names to form secret names, e.g. `bruin-prod-`. See [Namespacing secrets with a prefix](#namespacing-secrets-with-a-prefix). |

### Authentication

`BRUIN_AZURE_AUTH_METHOD` selects one credential type:

| Value | Credential | Use it for |
|-------|------------|------------|
| `default` | [`DefaultAzureCredential`](https://learn.microsoft.com/en-us/azure/developer/go/sdk/authentication/credential-chains#defaultazurecredential-overview) | Most cases. Tries, in order: the `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_CLIENT_SECRET` (or certificate) environment variables, AKS workload identity, managed identity, the Azure CLI, and the Azure Developer CLI. |
| `workload_identity` | Workload identity | AKS pods with [Microsoft Entra Workload ID](#running-on-kubernetes-aks). Reads `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_FEDERATED_TOKEN_FILE`, which the AKS webhook injects. |
| `managed_identity` | Managed identity | Azure VMs, Container Apps, App Service, or AKS node identities. Set `BRUIN_AZURE_CLIENT_ID` to pick a user-assigned identity. |
| `client_credentials` | Client secret | A service principal. Needs `BRUIN_AZURE_TENANT_ID`, `BRUIN_AZURE_CLIENT_ID` and `BRUIN_AZURE_CLIENT_SECRET`. |
| `cli` | Azure CLI | Local development after `az login`. |

`default` already picks up AKS workload identity and managed identity, so you don't have to change anything to run on Azure. Use `workload_identity` or `managed_identity` when you want Bruin to use exactly that identity and fail clearly when it isn't available, instead of falling through to other credentials.

### Credential Precedence

- An explicit `BRUIN_AZURE_AUTH_METHOD` always wins: Bruin uses only that credential.
- `client_credentials` uses only the `BRUIN_AZURE_*` variables, never the `AZURE_*` ones.
- With `workload_identity`, `BRUIN_AZURE_CLIENT_ID` and `BRUIN_AZURE_TENANT_ID` take precedence over the `AZURE_CLIENT_ID` and `AZURE_TENANT_ID` set by the webhook.
- With `default`, the order is the `DefaultAzureCredential` order in the table above. `BRUIN_AZURE_*` credentials are not used, so set `BRUIN_AZURE_AUTH_METHOD=client_credentials` if you want Bruin-specific static credentials to win.

### Example: Service Principal

```bash
export BRUIN_AZURE_KEYVAULT_URL=https://my-vault.vault.azure.net
export BRUIN_AZURE_AUTH_METHOD=client_credentials
export BRUIN_AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000
export BRUIN_AZURE_CLIENT_ID=11111111-1111-1111-1111-111111111111
export BRUIN_AZURE_CLIENT_SECRET=my-client-secret

bruin run --secrets-backend azure
```

### Example: Local Development

```bash
az login
export BRUIN_AZURE_KEYVAULT_URL=https://my-vault.vault.azure.net
export BRUIN_AZURE_AUTH_METHOD=cli

bruin run --secrets-backend azure
```

### Sovereign Clouds

Bruin accepts vault URLs in the public cloud and the sovereign clouds:

| Cloud | Vault URL |
|-------|-----------|
| Azure public cloud | `https://<name>.vault.azure.net` |
| Azure China (21Vianet) | `https://<name>.vault.azure.cn` |
| Azure US Government | `https://<name>.vault.usgovcloudapi.net` |
| Azure Germany | `https://<name>.vault.microsoftazure.de` |

For China and US Government vaults, Bruin signs in against that cloud's Microsoft Entra authority automatically. Set `AZURE_AUTHORITY_HOST` to override it, for example for Azure Germany (`https://login.microsoftonline.de/`). On AKS, the workload identity webhook sets `AZURE_AUTHORITY_HOST` for you. The `cli` method uses the cloud selected with `az cloud set`.

## Storing Secrets in Azure Key Vault

Each connection is one secret, named after the connection. The secret value is a JSON string with two required fields:

- `type`: The connection type (must match a valid Bruin connection type)
- `details`: An object containing the connection-specific configuration

### Example: PostgreSQL Connection

Create a secret named `my-postgres`:

```bash
az keyvault secret set --vault-name my-vault --name my-postgres --value '{
  "type": "postgres",
  "details": {
    "host": "localhost",
    "port": 5432,
    "username": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
    "schema": "public"
  }
}'
```

### Example: Snowflake Connection

Create a secret named `my-snowflake` with this value:

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

### Connection Names

Key Vault secret names may only contain letters, digits and hyphens (`0-9`, `a-z`, `A-Z`, `-`) and be at most 127 characters long, and they are case-insensitive. Connection names stored in Key Vault must follow the same rule, including any prefix. A connection named `my_postgres` can't be stored in Key Vault; Bruin reports:

```text
connection 'my_postgres' can't be read from Azure Key Vault: secret 'my_postgres' is not a valid Key Vault secret name, which may only contain letters, digits and hyphens (0-9, a-z, A-Z, -) and be at most 127 characters; rename the connection to match
```

Rename the connection, e.g. to `my-postgres`, in both the secret and the assets that use it. Bruin doesn't rewrite names for you.

### Namespacing Secrets with a Prefix

Set `BRUIN_AZURE_SECRET_PREFIX` to keep Bruin's secrets apart from others in the same vault, or to keep one set of secrets per environment in a shared vault:

```bash
export BRUIN_AZURE_SECRET_PREFIX=bruin-prod-
```

With this prefix, the connection `my-postgres` is read from the secret `bruin-prod-my-postgres`. The prefix must only use letters, digits and hyphens, since Key Vault names can't contain `/`. Without a prefix, the secret name is the connection name. Using a separate vault per environment is often simpler than a prefix.

## Listing Connections

`bruin connections list --secrets-backend azure` lists the connections stored in the vault without a local `.bruin.yml`:

```bash
bruin connections list --secrets-backend azure -o json
```

Bruin lists the enabled secrets in the vault (only those starting with `BRUIN_AZURE_SECRET_PREFIX`, if set) and reads each one. Disabled secrets, secrets backing Key Vault certificates, and secrets that aren't in the Bruin [format](#storing-secrets-in-azure-key-vault) are skipped.

## Permissions

Bruin only needs to read secrets.

- With [Azure RBAC](https://learn.microsoft.com/en-us/azure/key-vault/general/rbac-guide) (recommended), assign the **Key Vault Secrets User** role to Bruin's identity on the vault. It allows reading secret values and listing secrets.
- With vault access policies, grant the **Get** and **List** secret permissions. **List** is only needed for `bruin connections list`.

```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <client-id-of-bruin-identity> \
  --scope "$(az keyvault show --name my-vault --query id -o tsv)"
```

## Running on Kubernetes (AKS)

On AKS, use [Microsoft Entra Workload ID](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview) to give the Bruin pod an Azure identity through its Kubernetes ServiceAccount. No client secret is needed.

1. Enable the OIDC issuer and workload identity on the cluster:

   ```bash
   az aks update --resource-group my-rg --name my-cluster --enable-oidc-issuer --enable-workload-identity
   ```

2. Create a user-assigned managed identity and give it the **Key Vault Secrets User** role on the vault, as shown in [Permissions](#permissions):

   ```bash
   az identity create --resource-group my-rg --name bruin-secrets-reader
   ```

3. Federate the identity with the ServiceAccount the Bruin pod runs as:

   ```bash
   az identity federated-credential create \
     --name bruin \
     --identity-name bruin-secrets-reader \
     --resource-group my-rg \
     --issuer "$(az aks show --resource-group my-rg --name my-cluster --query oidcIssuerProfile.issuerUrl -o tsv)" \
     --subject system:serviceaccount:bruin:bruin \
     --audience api://AzureADTokenExchange
   ```

4. Annotate the ServiceAccount with the identity's client ID:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: bruin
     namespace: bruin
     annotations:
       azure.workload.identity/client-id: 11111111-1111-1111-1111-111111111111
   ```

5. Run the Bruin pod with that ServiceAccount and the `azure.workload.identity/use` label. The label is on the **pod**, not the ServiceAccount, and is what makes the webhook inject `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE` and `AZURE_AUTHORITY_HOST`:

   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: bruin
     namespace: bruin
     labels:
       azure.workload.identity/use: "true"
   spec:
     serviceAccountName: bruin
     containers:
       - name: bruin
         image: ghcr.io/bruin-data/bruin:latest
         # /project holds your Bruin project, e.g. baked into the image or mounted from a volume.
         command: ["bruin", "run", "--secrets-backend", "azure", "/project"]
         env:
           - name: BRUIN_AZURE_KEYVAULT_URL
             value: https://my-vault.vault.azure.net
           - name: BRUIN_AZURE_AUTH_METHOD
             value: workload_identity
   ```

`BRUIN_AZURE_AUTH_METHOD=default` also works here. `workload_identity` makes Bruin fail with a clear error if the webhook didn't inject its variables, for example because the pod label is missing, instead of falling back to the node's managed identity.

To use the node pool's or a VM's managed identity instead of workload identity, set `BRUIN_AZURE_AUTH_METHOD=managed_identity` (and `BRUIN_AZURE_CLIENT_ID` for a user-assigned identity).

## How It Works

When you run Bruin with `--secrets-backend azure`:

1. Bruin authenticates to Microsoft Entra ID with the configured credential
2. For each connection referenced in your pipeline, Bruin fetches the latest version of the secret with that name (with `BRUIN_AZURE_SECRET_PREFIX` prepended, if set)
3. The secret is parsed and validated according to the connection type
4. Each secret is fetched once and cached in memory for the duration of the run, even when several assets use it at the same time

## Troubleshooting

### Invalid Vault URL

```text
invalid Azure Key Vault URL: must be https://<name>.vault.azure.net, ...
```

Use the vault's **Vault URI** from the Azure portal (or `az keyvault show --name my-vault --query properties.vaultUri`). It must start with `https://`.

### Workload Identity Not Available

```text
failed to create Azure workload identity credential; it needs AZURE_CLIENT_ID, AZURE_TENANT_ID and AZURE_FEDERATED_TOKEN_FILE, ...
```

The workload identity webhook didn't mutate the pod. Check that the pod has the `azure.workload.identity/use: "true"` label, that its ServiceAccount has the `azure.workload.identity/client-id` annotation, and that workload identity is enabled on the cluster. Pods have to be recreated after these change.

### Secret Not Found

```text
secret 'my-connection' not found in Azure Key Vault
```

Verify that the secret exists in this vault with the connection's name (plus `BRUIN_AZURE_SECRET_PREFIX`, if set) and is enabled.

### Access Denied

A `403 Forbidden` response means the identity authenticated but lacks permission. Assign **Key Vault Secrets User** on the vault, or add **Get** and **List** secret permissions to the vault's access policy. Role assignments can take a few minutes to apply.
