# Deploying Bruin with GitHub Actions

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to deploy and run Bruin pipelines automatically using GitHub Actions. You can schedule pipeline runs, trigger them on code changes, or run them manually.

## Prerequisites

Before you begin, ensure you have:
- A GitHub repository with your Bruin project
- Access to configure GitHub Actions in your repository
- Credentials for your data platforms (stored as GitHub Secrets)

## Step 1: Prepare Your .bruin.yml Configuration

Create your `.bruin.yml` file with your production credentials. This file will NOT be committed to your repository.

Example `.bruin.yml`:

```yaml
environments:
  production:
    connections:
      postgres:
        - name: "my_postgres"
          username: "your_username"
          password: "your_password"
          host: "your-db-host.com"
          port: 5432
          database: "mydb"

      google_cloud_platform:
        - name: "my_gcp"
          service_account_json: |
            {
              "type": "service_account",
              "project_id": "my-project-id",
              "private_key_id": "...",
              "private_key": "...",
              "client_email": "...",
              "client_id": "...",
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token",
              "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
              "client_x509_cert_url": "..."
            }
          project_id: "my-project-id"
```

**Important:** Never commit this file to your repository. Add it to `.gitignore`:

```bash
echo ".bruin.yml" >> .gitignore
```

**Note:** For Google Cloud Platform, you can store the entire service account JSON directly in the YAML using `service_account_json` as shown above, instead of using `service_account_file`.

## Step 2: Store .bruin.yml as a GitHub Secret

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Create a secret named `BRUIN_CONFIG`
5. Copy the entire contents of your `.bruin.yml` file and paste it as the value

## Step 3: Create a GitHub Actions Workflow

Create a `.github/workflows` directory in your repository if it doesn't exist:

```bash
mkdir -p .github/workflows
```

Create a workflow file, e.g., `.github/workflows/bruin-pipeline.yml`:

```yaml
name: Run Bruin Pipeline

on:
  # Run on every push to main branch
  push:
    branches: [ main ]

  # Run on a schedule (every day at 3 AM UTC)
  schedule:
    - cron: '0 3 * * *'

  # Allow manual triggering
  workflow_dispatch:

jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    steps:
      # Checkout your repository
      - name: Checkout code
        uses: actions/checkout@v4

      # Install Bruin CLI
      - name: Setup Bruin
        uses: bruin-data/setup-bruin@main

      # Create .bruin.yml from secret
      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      # Run your pipeline
      - name: Run Bruin Pipeline
        run: bruin run . --environment production
```

## Workflow Triggers

GitHub Actions supports multiple trigger types:

### On Push
Run the pipeline whenever code is pushed to specific branches:

```yaml
on:
  push:
    branches: [ main, production ]
```

### On Schedule
Run the pipeline on a schedule using cron syntax:

```yaml
on:
  schedule:
    # Every day at 3 AM UTC
    - cron: '0 3 * * *'
    # Every hour
    - cron: '0 * * * *'
    # Every Monday at 8 AM UTC
    - cron: '0 8 * * 1'
```

**Cron syntax reference:**
```
* * * * *
│ │ │ │ │
│ │ │ │ └─── Day of week (0-7, Sunday = 0 or 7)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

### Manual Trigger
Allow manual workflow runs from the GitHub UI:

```yaml
on:
  workflow_dispatch:
```

### On Pull Request
Run validation on pull requests:

```yaml
on:
  pull_request:
    branches: [ main ]
```

### Combined Triggers
You can combine multiple triggers:

```yaml
on:
  push:
    branches: [ main ]
  schedule:
    - cron: '0 3 * * *'
  workflow_dispatch:
```

## Step 4: Run Specific Pipelines

If you have multiple pipelines, you can create separate jobs for each:

```yaml
name: Run Bruin Pipelines

on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours
  workflow_dispatch:

jobs:
  run-ingestion:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      - name: Run Ingestion Pipeline
        run: bruin run pipelines/ingestion --environment production

  run-analytics:
    runs-on: ubuntu-latest
    needs: run-ingestion  # Wait for ingestion to complete
    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      - name: Run Analytics Pipeline
        run: bruin run pipelines/analytics --environment production
```

## Step 5: Validate Before Running

It's a good practice to validate your pipeline before running it:

```yaml
jobs:
  validate-and-run:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      # Validate first
      - name: Validate Pipeline
        run: bruin validate .

      # Only run if validation succeeds
      - name: Run Pipeline
        if: success()
        run: bruin run . --environment production
```

## Step 6: Add Notifications

### Slack Notifications

Add Slack notifications for pipeline failures:

```yaml
jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      - name: Run Pipeline
        id: bruin-run
        run: bruin run . --environment production

      - name: Notify Slack on Failure
        if: failure()
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "Bruin pipeline failed in ${{ github.repository }}",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "❌ *Bruin Pipeline Failed*\n*Repository:* ${{ github.repository }}\n*Workflow:* ${{ github.workflow }}\n*Branch:* ${{ github.ref_name }}"
                  }
                }
              ]
            }
```

### Email Notifications

GitHub Actions automatically sends email notifications to workflow authors on failure. You can customize this in your GitHub notification settings.

## Advanced Examples

### Multi-Environment Deployment

Deploy to different environments based on the branch:

```yaml
name: Deploy Bruin Pipeline

on:
  push:
    branches: [ main, staging, development ]

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Determine Environment and Config
        id: env
        run: |
          if [ "${{ github.ref }}" = "refs/heads/main" ]; then
            echo "environment=production" >> $GITHUB_OUTPUT
            echo "config_secret=BRUIN_CONFIG_PROD" >> $GITHUB_OUTPUT
          elif [ "${{ github.ref }}" = "refs/heads/staging" ]; then
            echo "environment=staging" >> $GITHUB_OUTPUT
            echo "config_secret=BRUIN_CONFIG_STAGING" >> $GITHUB_OUTPUT
          else
            echo "environment=development" >> $GITHUB_OUTPUT
            echo "config_secret=BRUIN_CONFIG_DEV" >> $GITHUB_OUTPUT
          fi

      - name: Create Bruin config
        run: |
          echo '${{ secrets[steps.env.outputs.config_secret] }}' > .bruin.yml

      - name: Run Pipeline
        run: bruin run . --environment ${{ steps.env.outputs.environment }}
```

### Matrix Strategy for Multiple Pipelines

Run multiple pipelines in parallel:

```yaml
name: Run All Pipelines

on:
  schedule:
    - cron: '0 2 * * *'
  workflow_dispatch:

jobs:
  run-pipelines:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pipeline: [ingestion, analytics, reporting]

    steps:
      - uses: actions/checkout@v4
      - uses: bruin-data/setup-bruin@main

      - name: Create Bruin config
        run: echo '${{ secrets.BRUIN_CONFIG }}' > .bruin.yml

      - name: Run ${{ matrix.pipeline }} Pipeline
        run: bruin run pipelines/${{ matrix.pipeline }} --environment production
```

## Troubleshooting

### Workflow Not Running on Schedule

1. **Branch requirement**: Scheduled workflows only run on the default branch (usually `main`)
2. **Inactivity**: GitHub disables scheduled workflows after 60 days of repository inactivity
3. **Syntax**: Verify your cron syntax is correct

### Authentication Failures

1. **Check secrets**: Ensure all required secrets are added to your repository
2. **Secret names**: Verify secret names match exactly (case-sensitive)
3. **Test locally**: Test with the same credentials locally to verify they work

### Pipeline Fails in GitHub Actions but Works Locally

1. **Environment variables**: Ensure all necessary environment variables are set in the workflow
2. **File paths**: Use relative paths in your Bruin pipeline
3. **Dependencies**: Ensure all required tools are installed in the workflow

### Viewing Logs

1. Go to your repository on GitHub
2. Click **Actions** tab
3. Click on the workflow run
4. Click on the job to see detailed logs

## Best Practices

### 1. Use Environment-Specific Secrets

Create separate secrets for different environments:
- `PROD_POSTGRES_PASSWORD`
- `STAGING_POSTGRES_PASSWORD`
- `DEV_POSTGRES_PASSWORD`

### 2. Add Concurrency Control

Prevent multiple instances of the same workflow from running simultaneously:

```yaml
concurrency:
  group: bruin-pipeline-${{ github.ref }}
  cancel-in-progress: false
```

### 3. Set Timeouts

Prevent workflows from running indefinitely:

```yaml
jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    timeout-minutes: 60  # 1 hour max
```

### 4. Use Workflow Artifacts

Save pipeline outputs as artifacts:

```yaml
- name: Upload logs
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: bruin-logs
    path: logs/
```

### 5. Version Pin the Bruin Action

For production stability, pin to a specific version:

```yaml
- uses: bruin-data/setup-bruin@v1.2.0
```

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for advanced orchestration
- Learn about [quality checks](/quality/overview) to add to your CI/CD pipeline
- Review [VM deployment](/deployment/vm-deployment) for alternative deployment options

## Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Setup Bruin Action](https://github.com/marketplace/actions/setup-bruin)
- [Bruin CLI Documentation](/)
- [Credentials Management](/getting-started/credentials)
