# Payrails

[Payrails](https://www.payrails.com/) is a payment operations and orchestration platform that lets enterprises connect and manage multiple payment providers through a single integration.

Bruin supports Payrails as a source for [Ingestr assets](/assets/ingestr). You can ingest data from Payrails into your data platform.

To set up a Payrails connection, add a configuration item in the `.bruin.yml` file and in your asset file. Payrails secures API traffic with mutual TLS (mTLS), so you provide a client certificate and key in addition to your `client_id` and `client_secret`, created under **Settings > API Credentials** in the Payrails Portal.

Follow these steps to set up Payrails and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  payrails:
    - name: "payrails"
      client_id: "your-client-id"
      client_secret: "your-client-secret"
      cert_path: "/path/to/client.pem"
      key_path: "/path/to/client.key"
      environment: "production"
```

- `client_id`: (Required) Payrails API credential ID.
- `client_secret`: (Required) Payrails API credential secret.
- `cert_path` / `key_path`: (Required) Paths to your mTLS client certificate (`.pem`) and private key (`.key`).
- `cert` / `key`: PEM contents of the certificate and key, as an alternative to `cert_path`/`key_path` when local files are not available.
- `environment`: (Optional) `production` (default) or `sandbox`.
- `base_url`: (Optional) Full API base URL; overrides `environment` for account-specific hosts.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `payrails_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.payrails
type: ingestr

parameters:
  source_connection: payrails
  source_table: 'payments'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for Payrails.
- `source_connection`: The Payrails connection name defined in `.bruin.yml`.
- `source_table`: Name of the Payrails table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| payments | id | createdAt | merge | Payments with their status, amount, references, and transaction metadata. |
| instruments | id | createdAt | merge | Stored payment instruments (e.g. tokenized cards) with their status and metadata. |
| executions | id | updatedAt | merge | Workflow executions with their status and references. |

`payments` and `instruments` support incremental date-range loads via `--interval-start` / `--interval-end` (filtered on `createdAt`); `executions` is filtered on `updatedAt`.

Payrails has no endpoint that lists all workflows, so for `executions` you must specify which workflow(s) to pull by appending the workflow code(s) to the table name after a colon. Each row includes a `workflow_code` column. For example:

```yaml
name: public.payrails_executions
type: ingestr

parameters:
  source_connection: payrails
  source_table: 'executions:payment-acceptance'

  destination: postgres
```

Pass multiple workflow codes as a comma-separated list, e.g. `source_table: 'executions:payment-acceptance,payout'`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/payrails_ingestion.yml
```

Running this command ingests data from Payrails into your Postgres database.
