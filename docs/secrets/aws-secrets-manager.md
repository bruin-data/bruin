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

Bruin connects to AWS Secrets Manager using the default AWS credential provider chain. This means Bruin can use the same credentials that the AWS SDK normally finds, including:

- Environment variables such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`
- Shared AWS config and credentials files, including profiles selected with `AWS_PROFILE`
- IAM roles for Amazon EC2, ECS, EKS (IRSA and EKS Pod Identity), Lambda, or other AWS runtime environments
- AWS SSO or other credential processes configured in your AWS profile

Set the AWS region through your normal AWS configuration, for example `AWS_REGION`, `AWS_DEFAULT_REGION`, your selected AWS profile, or `BRUIN_AWS_REGION`. Bruin fails at startup with `no AWS region configured for AWS Secrets Manager` when none of these is set.

Bruin also supports explicit `BRUIN_` environment variables when you want to override the default AWS credential chain for this secrets backend:

- `BRUIN_AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `BRUIN_AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `BRUIN_AWS_REGION`: The AWS region where your secrets are stored (e.g., `us-east-1`, `eu-west-1`)

The following is optional when using temporary explicit credentials:

- `BRUIN_AWS_SESSION_TOKEN`: A session token for temporary credentials (e.g., when using AWS STS)

### All Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRUIN_AWS_ACCESS_KEY_ID` | No | Static access key ID. Must be set together with `BRUIN_AWS_SECRET_ACCESS_KEY`. |
| `BRUIN_AWS_SECRET_ACCESS_KEY` | No | Static secret access key. |
| `BRUIN_AWS_SESSION_TOKEN` | No | Session token for temporary static credentials. |
| `BRUIN_AWS_REGION` | No | Region of the secrets. Overrides `AWS_REGION` and the profile's region. |
| `BRUIN_AWS_SECRET_PREFIX` | No | Prepended to connection names to form secret names, e.g. `bruin/prod/`. See [Namespacing secrets with a prefix](#namespacing-secrets-with-a-prefix). |
| `BRUIN_AWS_ROLE_ARN` | No | IAM role to assume via STS before reading secrets, e.g. for secrets in another account. See [Assuming a role](#assuming-a-role). |
| `BRUIN_AWS_EXTERNAL_ID` | No | External ID passed when assuming `BRUIN_AWS_ROLE_ARN`. |
| `BRUIN_AWS_ROLE_SESSION_NAME` | No | Session name used when assuming `BRUIN_AWS_ROLE_ARN`. Defaults to `bruin-secrets`. |

### Credential Precedence

Bruin picks credentials in this order and uses the first that is available:

1. `BRUIN_AWS_ACCESS_KEY_ID` and `BRUIN_AWS_SECRET_ACCESS_KEY` (plus `BRUIN_AWS_SESSION_TOKEN`), if set.
2. The AWS SDK default chain, which itself tries, in order:
   1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
   2. Web identity: `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`, which EKS injects for [IRSA](#iam-roles-for-service-accounts-irsa)
   3. The shared config and credentials files (`~/.aws/config`, `~/.aws/credentials`, or the profile in `AWS_PROFILE`), including SSO and `credential_process`
   4. Container credentials: the ECS task role, or [EKS Pod Identity](#eks-pod-identity) (`AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`)
   5. The EC2 instance profile, through the instance metadata service (IMDSv2)

If `BRUIN_AWS_ROLE_ARN` is set, Bruin then assumes that role using whichever credentials were found above, and reads secrets with the role's credentials.

To see which source was used, run with `--debug`. Bruin logs the source name, for example `AWS Secrets Manager: using credentials from WebIdentityCredentials`. It never logs credential values.

### Example Setup

Use the AWS CLI or your cloud runtime to configure credentials as usual:

```bash
export AWS_PROFILE=my-profile
export AWS_REGION=us-east-1

bruin run --secrets-backend aws
```

Or set Bruin-specific credentials explicitly:

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

### Assuming a Role

Set `BRUIN_AWS_ROLE_ARN` to have Bruin assume an IAM role before reading secrets. This is useful when the secrets live in a different AWS account than the one your credentials belong to:

```bash
export AWS_REGION=eu-west-1
export BRUIN_AWS_ROLE_ARN=arn:aws:iam::222222222222:role/bruin-secrets-reader
export BRUIN_AWS_EXTERNAL_ID=my-external-id   # only if the role's trust policy requires it

bruin run --secrets-backend aws
```

The role is assumed on top of the credentials from [Credential Precedence](#credential-precedence), so it also works from IRSA or EKS Pod Identity (role chaining). The source role must be allowed to call `sts:AssumeRole` on the target role, and the target role's trust policy must allow the source role.

`BRUIN_AWS_ROLE_ARN` is separate from the SDK's `AWS_ROLE_ARN`. On EKS, `AWS_ROLE_ARN` is set by the IRSA webhook to the pod's own role; leave it alone and use `BRUIN_AWS_ROLE_ARN` for the role Bruin should switch to.

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

### Namespacing Secrets with a Prefix

Set `BRUIN_AWS_SECRET_PREFIX` to keep Bruin's secrets apart from others in the same account, or to keep one set of secrets per environment:

```bash
export BRUIN_AWS_SECRET_PREFIX=bruin/prod/
```

With this prefix, the connection `my-postgres` is read from the secret `bruin/prod/my-postgres`. The prefix is used exactly as given, so include the trailing `/` if you want one. Assets and `.bruin.yml`-style references keep using the plain connection name. Without a prefix, the secret name is the connection name.

## Listing Connections

`bruin connections list --secrets-backend aws` lists the connections stored in AWS Secrets Manager without a local `.bruin.yml`:

```bash
bruin connections list --secrets-backend aws -o json
```

Bruin lists the secrets in the region (only those starting with `BRUIN_AWS_SECRET_PREFIX`, if set) and reads each one. Secrets that aren't in the Bruin [secret format](#secret-format) are skipped, so other secrets in the account don't break the listing. Without a prefix, Bruin still reads every secret in the region to check it, so setting a prefix makes listing faster and needs fewer permissions. Listing needs the `secretsmanager:ListSecrets` permission in addition to `secretsmanager:GetSecretValue`.

## Supported Connection Types

The `type` field must be one of the valid Bruin connection types. Common types include:

- `postgres` - PostgreSQL database
- `mysql` - MySQL database
- `snowflake` - Snowflake data warehouse
- `google_cloud_platform` - Google BigQuery
- `redshift` - AWS Redshift
- `databricks` - Databricks
- `generic` - Generic key-value secrets

For a complete list of supported connection types and their configuration schemas, see the [connections documentation](../getting-started/introduction/quickstart.md#setting-up-your-bruin-yml-file).

## IAM Permissions

Bruin only needs read access. A minimal policy for secrets under the `bruin/prod/` prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:eu-west-1:111111111111:secret:bruin/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": "secretsmanager:ListSecrets",
      "Resource": "*"
    }
  ]
}
```

- `secretsmanager:GetSecretValue` is required to read connections.
- `secretsmanager:ListSecrets` is only needed for `bruin connections list`. AWS doesn't support resource-level restrictions for it, so it uses `"Resource": "*"`.
- If the secrets are encrypted with a customer-managed KMS key, also allow `kms:Decrypt` on that key.

## Running on Kubernetes (EKS)

On EKS, give the Bruin pod an IAM role through its Kubernetes ServiceAccount. Bruin picks the role up through the default credential chain, so no `BRUIN_AWS_*` credentials are needed. Only the region (if your cluster doesn't inject one) and the optional prefix:

```yaml
env:
  - name: BRUIN_SECRETS_BACKEND
    value: aws
  - name: AWS_REGION
    value: eu-west-1
  - name: BRUIN_AWS_SECRET_PREFIX
    value: bruin/prod/
```

Don't set `BRUIN_AWS_ACCESS_KEY_ID` or `BRUIN_AWS_SECRET_ACCESS_KEY` in the pod: they take precedence over the pod's role.

Use either IRSA or EKS Pod Identity. Both give the pod an IAM role with the [permissions above](#iam-permissions).

### IAM Roles for Service Accounts (IRSA)

1. [Create an IAM OIDC provider](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html) for the cluster.
2. Create an IAM role whose trust policy allows the ServiceAccount to assume it:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Federated": "arn:aws:iam::111111111111:oidc-provider/oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
         },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": {
             "oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:sub": "system:serviceaccount:bruin:bruin",
             "oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:aud": "sts.amazonaws.com"
           }
         }
       }
     ]
   }
   ```

3. Annotate the ServiceAccount with the role ARN and run the Bruin pod with it:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: bruin
     namespace: bruin
     annotations:
       eks.amazonaws.com/role-arn: arn:aws:iam::111111111111:role/bruin-secrets-reader
   ```

   ```yaml
   spec:
     serviceAccountName: bruin
   ```

The EKS webhook then injects `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` into the pod.

### EKS Pod Identity

1. Install the EKS Pod Identity Agent add-on on the cluster.
2. Create an IAM role whose trust policy allows `pods.eks.amazonaws.com` to call `sts:AssumeRole` and `sts:TagSession`.
3. Associate the role with the ServiceAccount:

   ```bash
   aws eks create-pod-identity-association \
     --cluster-name my-cluster \
     --namespace bruin \
     --service-account bruin \
     --role-arn arn:aws:iam::111111111111:role/bruin-secrets-reader
   ```

4. Run the Bruin pod with that ServiceAccount (no annotation needed):

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: bruin
     namespace: bruin
   ```

EKS then injects `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` into the pod.

### Secrets in Another Account

If the secrets live in another account, keep the pod's role from IRSA or Pod Identity and set `BRUIN_AWS_ROLE_ARN` to a role in the secrets account. Bruin assumes it on top of the pod's role. See [Assuming a Role](#assuming-a-role).

## How It Works

When you run Bruin with `--secrets-backend aws`:

1. Bruin connects to AWS Secrets Manager using your credentials
2. For each connection referenced in your pipeline, Bruin fetches the corresponding secret by name (with `BRUIN_AWS_SECRET_PREFIX` prepended, if set)
3. The secret is parsed and validated according to the connection type
4. The connection is established using the fetched credentials
5. Each secret is fetched once and cached in memory for the duration of the run, even when several assets use it at the same time

## Troubleshooting

### AWS Credentials or Region Not Found

If you see an error like:

```bash
failed to initialize AWS Secrets Manager client: no AWS region configured for AWS Secrets Manager
```

set `BRUIN_AWS_REGION` or `AWS_REGION`, or add a region to your AWS profile.

If reads fail with a credentials error, make sure AWS credentials are available through either the default AWS credential chain or the Bruin-specific environment variables, and run Bruin with `--debug` to see which credential source it used:

```bash
aws sts get-caller-identity
aws configure get region
```

If you are using Bruin-specific credentials, both `BRUIN_AWS_ACCESS_KEY_ID` and `BRUIN_AWS_SECRET_ACCESS_KEY` must be set together. `BRUIN_AWS_REGION` is optional when the region is already available through the default AWS configuration, but setting it explicitly is recommended when your secrets are in a specific region.

### Secret Not Found

If you see an error like:

```bash
secret 'my-connection' not found in AWS Secrets Manager
```

Verify that:

1. The secret exists in AWS Secrets Manager with the exact name used in your pipeline, including `BRUIN_AWS_SECRET_PREFIX` if you set one
2. The secret is in the correct AWS region
3. Your AWS credentials have the `secretsmanager:GetSecretValue` permission for that secret

### Invalid Secret Format

If you see an error like:

```bash
failed to parse secret 'my-connection' as JSON
```

Verify that:

1. The secret value in AWS Secrets Manager is valid JSON
2. The JSON includes both `type` and `details` fields
3. The `type` value matches a supported connection type
4. The `details` object contains all required fields for that connection type

### Secret Has No String Value

If you see an error like:

```bash
secret 'my-connection' has no string value
```

Make sure the secret is stored as a plaintext string (not binary) in AWS Secrets Manager.
