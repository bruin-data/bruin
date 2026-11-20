# Using GCP Secret Manager as a Secrets Backend

Bruin supports using [Google Cloud Secret Manager](https://cloud.google.com/secret-manager/docs/overview) as a secrets backend for managing connection credentials. This is controlled via the `--secrets-backend` flag.

## Enabling GCP Secret Manager

To use GCP Secret Manager as your secrets backend, pass the flag:

```bash
bruin run --secrets-backend gcp
```

You can also set the backend via environment variable:

```bash
export BRUIN_SECRETS_BACKEND=gcp
bruin run
```

## Configuring the GCP Connection

Bruin authenticates with [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) (ADC), the same way `gcloud` and the Google Cloud client libraries do. On GKE, GCE and Cloud Run, and with a [workload identity federation](#workload-identity-federation-eks-aks-and-other-clusters) config, no Bruin-specific credentials are needed:

```bash
export BRUIN_GCP_PROJECT=my-project

bruin run --secrets-backend gcp
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRUIN_GCP_PROJECT` | No | The project that holds the secrets. Defaults to the project the credentials report. See [Project](#project). |
| `BRUIN_GCP_CREDENTIALS_FILE` | No | Path to a credentials file to use instead of ADC. Any type ADC accepts works: a service account key, an `external_account` workload identity federation config, an `impersonated_service_account` config, or `authorized_user` credentials. |
| `BRUIN_GCP_CREDENTIALS_JSON` | No | The contents of a credentials file, as an alternative to `BRUIN_GCP_CREDENTIALS_FILE`. Set only one of the two. |
| `BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT` | No | Email of a service account to impersonate before reading secrets, e.g. `bruin-secrets@my-project.iam.gserviceaccount.com`. See [Impersonating a service account](#impersonating-a-service-account). |
| `BRUIN_GCP_SECRET_PREFIX` | No | Prepended to connection names to form secret IDs, e.g. `bruin-prod-`. See [Namespacing secrets with a prefix](#namespacing-secrets-with-a-prefix). |
| `BRUIN_GCP_SECRET_VERSION` | No | The secret version to read for every connection: `latest` (the default), a version number, or a [version alias](https://cloud.google.com/secret-manager/docs/assign-alias-to-secret-version). |

### Credential Precedence

Bruin picks credentials in this order and uses the first that is available:

1. `BRUIN_GCP_CREDENTIALS_JSON` or `BRUIN_GCP_CREDENTIALS_FILE`, if set.
2. Application Default Credentials, which itself tries, in order:
   1. The file `GOOGLE_APPLICATION_CREDENTIALS` points at. This is how a [workload identity federation](#workload-identity-federation-eks-aks-and-other-clusters) config is usually passed in.
   2. The gcloud application default credentials file, written by `gcloud auth application-default login`.
   3. The metadata server: [GKE Workload Identity](#gke-workload-identity), or the service account attached to a GCE VM or Cloud Run service.

If `BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT` is set, Bruin then impersonates that service account using whichever credentials were found above, and reads secrets as that service account.

To see which source was used, run with `--debug`. Bruin logs the credential type and where it came from, for example `GCP Secret Manager: using external_account credentials from GOOGLE_APPLICATION_CREDENTIALS`. It never logs credential values.

### Project

Bruin reads secrets from the first project it finds:

1. `BRUIN_GCP_PROJECT`
2. `GOOGLE_CLOUD_PROJECT`
3. The project the credentials report: the `project_id` of a service account key, or the project of the GKE cluster or GCE VM from the metadata server
4. The quota project of the credentials: `quota_project_id` in the credentials file, or `GOOGLE_CLOUD_QUOTA_PROJECT`

Bruin fails at startup with `no GCP project configured for GCP Secret Manager` when none of these is set. Workload identity federation configs don't name a project, so set `BRUIN_GCP_PROJECT` with them. Also set it when the secrets live in a different project from the credentials.

### Example: Local Development

```bash
gcloud auth application-default login
export BRUIN_GCP_PROJECT=my-project

bruin run --secrets-backend gcp
```

### Example: Service Account Key

```bash
export BRUIN_GCP_CREDENTIALS_FILE=/path/to/service-account.json

bruin run --secrets-backend gcp
```

The project defaults to the key's `project_id`. Prefer workload identity over keys wherever you can.

### Impersonating a Service Account

Set `BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT` to read secrets as another service account, for example one that only has access to Bruin's secrets:

```bash
export BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT=bruin-secrets@my-project.iam.gserviceaccount.com
export BRUIN_GCP_PROJECT=my-project
```

The credentials Bruin found need `roles/iam.serviceAccountTokenCreator` on that service account. The project is resolved from the original credentials, so set `BRUIN_GCP_PROJECT` if the secrets are in the impersonated service account's project.

## Storing Secrets in GCP Secret Manager

Each connection is one secret, whose ID is the connection name. The secret value is a JSON string with two required fields:

- `type`: The connection type (must match a valid Bruin connection type)
- `details`: An object containing the connection-specific configuration

### Example: PostgreSQL Connection

Create a secret named `my-postgres`:

```bash
printf '%s' '{
  "type": "postgres",
  "details": {
    "host": "localhost",
    "port": 5432,
    "username": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
    "schema": "public"
  }
}' | gcloud secrets create my-postgres --project my-project --data-file=-
```

To change it later, add a new version. Bruin reads the `latest` version by default:

```bash
printf '%s' '{"type": "postgres", "details": {...}}' | gcloud secrets versions add my-postgres --data-file=-
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

Secret IDs may only contain letters, digits, underscores and hyphens (`a-z`, `A-Z`, `0-9`, `_`, `-`) and be at most 255 characters long. They are case-sensitive. Connection names stored in Secret Manager must follow the same rule, including any prefix. A connection named `my.postgres` can't be stored in Secret Manager; Bruin reports:

```text
connection 'my.postgres' can't be read from GCP Secret Manager: secret 'my.postgres' is not a valid secret ID, which may only contain letters, digits, underscores and hyphens (a-z, A-Z, 0-9, _, -) and be at most 255 characters; rename the connection to match
```

Rename the connection in both the secret and the assets that use it. Bruin doesn't rewrite names for you.

### Namespacing Secrets with a Prefix

Set `BRUIN_GCP_SECRET_PREFIX` to keep Bruin's secrets apart from others in the same project, or to keep one set of secrets per environment in a shared project:

```bash
export BRUIN_GCP_SECRET_PREFIX=bruin-prod-
```

With this prefix, the connection `my-postgres` is read from the secret `bruin-prod-my-postgres`. The prefix must only use letters, digits, underscores and hyphens, since secret IDs can't contain `/`. Without a prefix, the secret ID is the connection name. Using a separate project per environment is often simpler than a prefix.

## Listing Connections

`bruin connections list --secrets-backend gcp` lists the connections stored in the project without a local `.bruin.yml`:

```bash
bruin connections list --secrets-backend gcp -o json
```

Bruin lists the secrets in the project (only those starting with `BRUIN_GCP_SECRET_PREFIX`, if set) and reads the configured version of each. Secrets without that version, with the version disabled or destroyed, and secrets that aren't in the Bruin [format](#storing-secrets-in-gcp-secret-manager) are skipped.

## IAM Permissions

Bruin only needs to read secrets.

- `roles/secretmanager.secretAccessor` lets Bruin read secret values (`secretmanager.versions.access`). Grant it on the project, or on each secret Bruin reads.
- `bruin connections list` also needs `secretmanager.secrets.list` on the **project**. `roles/secretmanager.secretAccessor` doesn't include it; `roles/secretmanager.viewer` does, or use a custom role.

```bash
gcloud projects add-iam-policy-binding my-project \
  --role=roles/secretmanager.secretAccessor \
  --member=serviceAccount:bruin-secrets@my-project.iam.gserviceaccount.com

# Only needed for `bruin connections list`.
gcloud projects add-iam-policy-binding my-project \
  --role=roles/secretmanager.viewer \
  --member=serviceAccount:bruin-secrets@my-project.iam.gserviceaccount.com
```

To grant access to a single secret instead of the whole project:

```bash
gcloud secrets add-iam-policy-binding my-postgres \
  --project=my-project \
  --role=roles/secretmanager.secretAccessor \
  --member=serviceAccount:bruin-secrets@my-project.iam.gserviceaccount.com
```

With workload identity, the `--member` is the workload's principal instead of a service account, as shown below.

## Running on Kubernetes

Bruin picks up the pod's identity through Application Default Credentials, so no `BRUIN_GCP_*` credentials are needed. Don't set `BRUIN_GCP_CREDENTIALS_FILE` or `BRUIN_GCP_CREDENTIALS_JSON` in the pod: they take precedence over the pod's identity.

### GKE Workload Identity

On GKE, use [Workload Identity Federation for GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) so the Bruin pod's Kubernetes ServiceAccount has a Google Cloud identity. Autopilot clusters have it enabled; on Standard clusters enable it on the cluster and its node pools:

```bash
gcloud container clusters update my-cluster --location=europe-west1 \
  --workload-pool=my-project.svc.id.goog

gcloud container node-pools update my-pool --cluster=my-cluster --location=europe-west1 \
  --workload-metadata=GKE_METADATA
```

Then give the ServiceAccount access to the secrets in one of two ways.

**Direct principal binding (recommended).** Grant the roles to the Kubernetes ServiceAccount itself. `123456789012` is the project number of the cluster's project:

```bash
gcloud projects add-iam-policy-binding my-project \
  --role=roles/secretmanager.secretAccessor \
  --member=principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-project.svc.id.goog/subject/ns/bruin/sa/bruin
```

The ServiceAccount needs no annotation:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: bruin
  namespace: bruin
```

**Through a Google service account.** Grant the roles to a Google service account as shown in [IAM Permissions](#iam-permissions), allow the Kubernetes ServiceAccount to act as it, and annotate the Kubernetes ServiceAccount with its email:

```bash
gcloud iam service-accounts add-iam-policy-binding bruin-secrets@my-project.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:my-project.svc.id.goog[bruin/bruin]"
```

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: bruin
  namespace: bruin
  annotations:
    iam.gke.io/gcp-service-account: bruin-secrets@my-project.iam.gserviceaccount.com
```

Either way, run the Bruin pod with that ServiceAccount:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: bruin
  namespace: bruin
spec:
  serviceAccountName: bruin
  containers:
    - name: bruin
      image: ghcr.io/bruin-data/bruin:latest
      # /project holds your Bruin project, e.g. baked into the image or mounted from a volume.
      command: ["bruin", "run", "--secrets-backend", "gcp", "/project"]
      env:
        - name: BRUIN_GCP_SECRET_PREFIX
          value: bruin-prod-
```

Bruin gets credentials and the project from the GKE metadata server. Set `BRUIN_GCP_PROJECT` if the secrets are in another project.

### Workload Identity Federation (EKS, AKS and Other Clusters)

A cluster outside Google Cloud, such as EKS or AKS, can read GCP Secret Manager without a service account key through [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation-with-kubernetes). The pod exchanges its Kubernetes ServiceAccount token for Google credentials, using a credential configuration file that Bruin picks up through `GOOGLE_APPLICATION_CREDENTIALS`.

1. Find the cluster's OIDC issuer URL:

   ```bash
   # EKS
   aws eks describe-cluster --name my-cluster --query cluster.identity.oidc.issuer --output text
   # AKS (needs --enable-oidc-issuer)
   az aks show --resource-group my-rg --name my-cluster --query oidcIssuerProfile.issuerUrl -o tsv
   ```

2. Create a workload identity pool and an OIDC provider for the cluster:

   ```bash
   gcloud iam workload-identity-pools create bruin --project=my-project --location=global

   gcloud iam workload-identity-pools providers create-oidc my-cluster \
     --project=my-project \
     --location=global \
     --workload-identity-pool=bruin \
     --issuer-uri=https://oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE \
     --attribute-mapping=google.subject=assertion.sub
   ```

3. Grant the roles to the pod's Kubernetes ServiceAccount, whose subject is `system:serviceaccount:<namespace>:<name>`. `123456789012` is the project number of `my-project`:

   ```bash
   gcloud projects add-iam-policy-binding my-project \
     --role=roles/secretmanager.secretAccessor \
     --member=principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/bruin/subject/system:serviceaccount:bruin:bruin
   ```

   To act as a Google service account instead, grant that service account the roles, give the principal above `roles/iam.workloadIdentityUser` on it, and add `--service-account=<email>` in the next step.

4. Create the credential configuration. It says where the pod's token is mounted and contains no secrets:

   ```bash
   gcloud iam workload-identity-pools create-cred-config \
     projects/123456789012/locations/global/workloadIdentityPools/bruin/providers/my-cluster \
     --credential-source-file=/var/run/service-account/token \
     --credential-source-type=text \
     --output-file=credential-configuration.json

   kubectl create configmap gcp-credential-configuration -n bruin \
     --from-file=credential-configuration.json
   ```

5. Run the Bruin pod with a projected ServiceAccount token for the provider's audience, the configuration mounted, and `GOOGLE_APPLICATION_CREDENTIALS` pointing at it:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: bruin
     namespace: bruin
   ---
   apiVersion: v1
   kind: Pod
   metadata:
     name: bruin
     namespace: bruin
   spec:
     serviceAccountName: bruin
     containers:
       - name: bruin
         image: ghcr.io/bruin-data/bruin:latest
         command: ["bruin", "run", "--secrets-backend", "gcp", "/project"]
         env:
           - name: GOOGLE_APPLICATION_CREDENTIALS
             value: /etc/workload-identity/credential-configuration.json
           - name: BRUIN_GCP_PROJECT
             value: my-project
         volumeMounts:
           - name: token
             mountPath: /var/run/service-account
             readOnly: true
           - name: workload-identity-credential-configuration
             mountPath: /etc/workload-identity
             readOnly: true
     volumes:
       - name: token
         projected:
           sources:
             - serviceAccountToken:
                 audience: https://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/bruin/providers/my-cluster
                 expirationSeconds: 3600
                 path: token
       - name: workload-identity-credential-configuration
         configMap:
           name: gcp-credential-configuration
   ```

The pod's IRSA or AKS workload identity setup, if any, keeps working alongside this: the token above is a separate projected token with Google's audience.

## How It Works

When you run Bruin with `--secrets-backend gcp`:

1. Bruin resolves credentials and the project as described in [Credential Precedence](#credential-precedence) and [Project](#project)
2. For each connection referenced in your pipeline, Bruin reads the configured version (`latest` by default) of the secret with that ID (with `BRUIN_GCP_SECRET_PREFIX` prepended, if set)
3. The secret is parsed and validated according to the connection type
4. Each secret is fetched once and cached in memory for the duration of the run, even when several assets use it at the same time

## Troubleshooting

### Credentials Not Found

```text
failed to initialize GCP Secret Manager client: failed to find GCP credentials: ...
```

No credentials were found. Run `gcloud auth application-default login` locally, set `GOOGLE_APPLICATION_CREDENTIALS` or `BRUIN_GCP_CREDENTIALS_FILE`, or check that the pod or VM has a service account. On GKE, check that the node pool uses `GKE_METADATA`.

### Project Not Found

```text
no GCP project configured for GCP Secret Manager: set BRUIN_GCP_PROJECT, ...
```

Set `BRUIN_GCP_PROJECT` to the project that holds the secrets.

### Secret Not Found

```text
secret 'my-connection' (version 'latest') in GCP project 'my-project' not found in GCP Secret Manager
```

Check that the secret exists in this project with the connection's name (plus `BRUIN_GCP_SECRET_PREFIX`, if set), and that it has the version Bruin reads (an enabled version, or the `BRUIN_GCP_SECRET_VERSION` alias).

### Permission Denied

```text
permission denied reading secret 'my-connection' (version 'latest') in GCP project 'my-project': the identity needs the secretmanager.versions.access permission on the secret, ...
```

The identity authenticated but can't read the secret. Grant `roles/secretmanager.secretAccessor` as shown in [IAM Permissions](#iam-permissions). When the role is granted on single secrets rather than the project, a secret that doesn't exist is also reported as permission denied, so check the name too. IAM changes can take a few minutes to apply.

For `bruin connections list`, the error names `secretmanager.secrets.list`, which has to be granted on the project.

### Workload Identity Federation Errors

If the token exchange fails, run with `--debug` to confirm Bruin uses `external_account` credentials from `GOOGLE_APPLICATION_CREDENTIALS`, then check that:

- the projected token's `audience` matches the provider (`https://iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`, or the provider's `--allowed-audiences`)
- the provider's issuer URL is the cluster's OIDC issuer
- the `principal://` member uses the project **number** and the exact `system:serviceaccount:<namespace>:<name>` subject
