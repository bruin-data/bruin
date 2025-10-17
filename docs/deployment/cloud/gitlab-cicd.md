# Deploying Bruin with GitLab CI/CD

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to deploy and run Bruin pipelines automatically using GitLab CI/CD. You can schedule pipeline runs, trigger them on code changes, or run them manually.

## Prerequisites

Before you begin, ensure you have:
- A GitLab repository with your Bruin project
- Access to configure GitLab CI/CD in your repository
- Credentials for your data platforms (stored as GitLab CI/CD Variables)

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
              "client_id": "..."
            }
          project_id: "my-project-id"
```

**Important:** Never commit this file to your repository. Add it to `.gitignore`:

```bash
echo ".bruin.yml" >> .gitignore
```

## Step 2: Store .bruin.yml as a GitLab CI/CD Variable

1. Go to your GitLab project
2. Click **Settings** → **CI/CD**
3. Expand **Variables**
4. Click **Add variable**
5. Set **Key** to `BRUIN_CONFIG`
6. Set **Type** to `File`
7. Paste the entire contents of your `.bruin.yml` file in the **Value** field
8. Check **Mask variable** and **Protect variable** for security
9. Click **Add variable**

## Step 3: Create a GitLab CI/CD Pipeline

Create a `.gitlab-ci.yml` file in the root of your repository:

```yaml
stages:
  - validate
  - run

# Install Bruin CLI
.install_bruin:
  before_script:
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH="$HOME/.local/bin:$PATH"

# Validate pipeline
validate:
  extends: .install_bruin
  stage: validate
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin validate .
  only:
    - merge_requests
    - main

# Run pipeline
run_pipeline:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - main
```

## Pipeline Triggers

GitLab CI/CD supports multiple trigger types:

### On Push to Branch

Run on specific branches:

```yaml
run_pipeline:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - main
    - production
```

### On Schedule

Create scheduled pipelines in GitLab UI:

1. Go to **CI/CD** → **Schedules**
2. Click **New schedule**
3. Set description (e.g., "Daily pipeline run")
4. Set interval pattern (e.g., `0 3 * * *` for 3 AM daily)
5. Select target branch
6. Click **Save pipeline schedule**

Then add a job that only runs on schedules:

```yaml
scheduled_run:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - schedules
```

### Manual Trigger

Allow manual pipeline runs:

```yaml
manual_run:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  when: manual
  only:
    - main
```

### On Merge Request

Run validation on merge requests:

```yaml
validate_mr:
  extends: .install_bruin
  stage: validate
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin validate .
  only:
    - merge_requests
```

## Step 4: Run Specific Pipelines

Run multiple pipelines with dependencies:

```yaml
stages:
  - validate
  - ingestion
  - analytics
  - reporting

.install_bruin:
  before_script:
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH="$HOME/.local/bin:$PATH"
    - cp $BRUIN_CONFIG .bruin.yml

validate:
  extends: .install_bruin
  stage: validate
  script:
    - bruin validate .
  only:
    - main

ingestion:
  extends: .install_bruin
  stage: ingestion
  script:
    - bruin run pipelines/ingestion --environment production
  only:
    - main

analytics:
  extends: .install_bruin
  stage: analytics
  script:
    - bruin run pipelines/analytics --environment production
  needs:
    - ingestion
  only:
    - main

reporting:
  extends: .install_bruin
  stage: reporting
  script:
    - bruin run pipelines/reporting --environment production
  needs:
    - analytics
  only:
    - main
```

## Using Docker

You can also use the official Bruin Docker image:

```yaml
stages:
  - run

run_pipeline:
  stage: run
  image: ghcr.io/bruin-data/bruin:latest
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - main
```

### With Git Repository Cloning

```yaml
stages:
  - run

run_from_git:
  stage: run
  image: ghcr.io/bruin-data/bruin:latest
  before_script:
    - apk add --no-cache git
  script:
    - git clone https://github.com/your-username/your-bruin-project.git /workspace
    - cd /workspace
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - main
```

## Multi-Environment Deployment

Deploy to different environments based on the branch:

```yaml
stages:
  - run

.install_bruin:
  before_script:
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH="$HOME/.local/bin:$PATH"

deploy_dev:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG_DEV .bruin.yml
    - bruin run . --environment development
  only:
    - develop

deploy_staging:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG_STAGING .bruin.yml
    - bruin run . --environment staging
  only:
    - staging

deploy_prod:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG_PROD .bruin.yml
    - bruin run . --environment production
  only:
    - main
```

Create separate CI/CD variables for each environment:
- `BRUIN_CONFIG_DEV` (Type: File)
- `BRUIN_CONFIG_STAGING` (Type: File)
- `BRUIN_CONFIG_PROD` (Type: File, Protected)

## Parallel Pipeline Execution

Run multiple pipelines in parallel:

```yaml
stages:
  - validate
  - run

.install_bruin:
  before_script:
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH="$HOME/.local/bin:$PATH"
    - cp $BRUIN_CONFIG .bruin.yml

validate:
  extends: .install_bruin
  stage: validate
  script:
    - bruin validate .

ingestion:
  extends: .install_bruin
  stage: run
  script:
    - bruin run pipelines/ingestion --environment production
  needs:
    - validate

analytics:
  extends: .install_bruin
  stage: run
  script:
    - bruin run pipelines/analytics --environment production
  needs:
    - validate

reporting:
  extends: .install_bruin
  stage: run
  script:
    - bruin run pipelines/reporting --environment production
  needs:
    - validate
```

## Notifications

### Email Notifications

GitLab automatically sends email notifications. Configure in **Settings** → **Integrations** → **Emails on push**.

### Slack Notifications

Add Slack notifications on failure:

```yaml
run_pipeline:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  after_script:
    - |
      if [ $CI_JOB_STATUS == 'failed' ]; then
        curl -X POST -H 'Content-type: application/json' \
        --data "{\"text\":\"❌ Bruin pipeline failed in $CI_PROJECT_NAME\n*Branch:* $CI_COMMIT_REF_NAME\n*Pipeline:* $CI_PIPELINE_URL\"}" \
        $SLACK_WEBHOOK_URL
      fi
  only:
    - main
```

Add `SLACK_WEBHOOK_URL` as a CI/CD variable.

## Artifacts and Reports

Save pipeline outputs as artifacts:

```yaml
run_pipeline:
  extends: .install_bruin
  stage: run
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production | tee pipeline.log
  artifacts:
    paths:
      - pipeline.log
    expire_in: 1 week
    when: always
  only:
    - main
```

## Caching

Cache Bruin CLI installation to speed up pipelines:

```yaml
.install_bruin:
  before_script:
    - |
      if [ ! -f "$HOME/.local/bin/bruin" ]; then
        curl -LsSf https://getbruin.com/install/cli | sh
      fi
    - export PATH="$HOME/.local/bin:$PATH"
  cache:
    key: bruin-cli
    paths:
      - $HOME/.local/bin/
```

## Using GitLab Runners on Kubernetes

For self-hosted GitLab Runners on Kubernetes:

```yaml
run_pipeline:
  stage: run
  image: ghcr.io/bruin-data/bruin:latest
  tags:
    - kubernetes
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
  only:
    - main
```

## Best Practices

### 1. Use Protected Variables

Mark production credentials as **Protected** to ensure they're only available on protected branches.

### 2. Use File Type for .bruin.yml

Always use **Type: File** for `BRUIN_CONFIG` variables to properly handle multiline YAML.

### 3. Validate Before Running

Always validate your pipeline before running it:

```yaml
stages:
  - validate
  - run

validate:
  extends: .install_bruin
  stage: validate
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin validate .
```

### 4. Use Specific Docker Image Versions

Pin to specific Bruin versions in production:

```yaml
image: ghcr.io/bruin-data/bruin:latest
```

### 5. Set Timeouts

Prevent jobs from running indefinitely:

```yaml
run_pipeline:
  extends: .install_bruin
  stage: run
  timeout: 1h
  script:
    - cp $BRUIN_CONFIG .bruin.yml
    - bruin run . --environment production
```

## Troubleshooting

### Pipeline Fails to Find Bruin

Ensure the PATH is exported after installation:

```yaml
before_script:
  - curl -LsSf https://getbruin.com/install/cli | sh
  - export PATH="$HOME/.local/bin:$PATH"
  - bruin --version
```

### BRUIN_CONFIG Variable Not Found

1. Verify the variable is created in **Settings** → **CI/CD** → **Variables**
2. Check the variable is not protected if running on an unprotected branch
3. Ensure **Type** is set to **File**

### Permission Denied

Ensure `.bruin.yml` has correct permissions:

```yaml
script:
  - cp $BRUIN_CONFIG .bruin.yml
  - chmod 600 .bruin.yml
  - bruin run . --environment production
```

### Pipeline Fails in CI but Works Locally

Check that all necessary files are committed to the repository and not in `.gitignore`.

## Complete Example

Here's a complete `.gitlab-ci.yml` for a production setup:

```yaml
stages:
  - validate
  - ingestion
  - analytics
  - reporting

.install_bruin:
  before_script:
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH="$HOME/.local/bin:$PATH"
    - cp $BRUIN_CONFIG .bruin.yml

# Validate on all branches
validate:
  extends: .install_bruin
  stage: validate
  script:
    - bruin validate .
  only:
    - merge_requests
    - main

# Run ingestion
ingestion:
  extends: .install_bruin
  stage: ingestion
  script:
    - bruin run pipelines/ingestion --environment production
  after_script:
    - |
      if [ $CI_JOB_STATUS == 'failed' ]; then
        curl -X POST -H 'Content-type: application/json' \
        --data "{\"text\":\"❌ Ingestion failed in $CI_PROJECT_NAME\"}" \
        $SLACK_WEBHOOK_URL
      fi
  artifacts:
    paths:
      - logs/
    when: always
  only:
    - main
    - schedules

# Run analytics after ingestion
analytics:
  extends: .install_bruin
  stage: analytics
  script:
    - bruin run pipelines/analytics --environment production
  needs:
    - ingestion
  only:
    - main
    - schedules

# Run reporting after analytics
reporting:
  extends: .install_bruin
  stage: reporting
  script:
    - bruin run pipelines/reporting --environment production
  needs:
    - analytics
  only:
    - main
    - schedules
```

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration
- Learn about [quality checks](/quality/overview) to add validation
- Review other [deployment options](/deployment/vm-deployment)

## Additional Resources

- [GitLab CI/CD Documentation](https://docs.gitlab.com/ee/ci/)
- [Bruin Docker Images](https://github.com/bruin-data/bruin/pkgs/container/bruin)
- [Bruin CLI Documentation](/)
- [Credentials Management](/getting-started/credentials)
