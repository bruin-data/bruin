# Deploying Bruin with Google Cloud Run

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to run Bruin pipelines using Google Cloud Run Jobs. This serverless approach provides automatic scaling and runs containers on-demand without requiring a web service.

## Prerequisites

Before you begin, ensure you have:
- A Google Cloud Platform (GCP) project
- `gcloud` CLI installed and configured
- Docker installed
- A Bruin project ready to deploy
- Credentials for your data platforms

## Overview

Google Cloud Run Jobs allows you to run containers for batch workloads. For Bruin, we'll use:

- **Cloud Run Jobs** to execute Bruin pipelines as containers
- **Secret Manager** for storing credentials
- **Cloud Scheduler** for scheduling
- **Cloud Logging** for monitoring

## Step 1: Enable Required APIs

```bash
# Set your project ID
PROJECT_ID=your-project-id
gcloud config set project ${PROJECT_ID}

# Enable required APIs
gcloud services enable \
    run.googleapis.com \
    secretmanager.googleapis.com \
    cloudscheduler.googleapis.com \
    artifactregistry.googleapis.com
```

## Step 2: Create Dockerfile

Create a `Dockerfile` in your project root:

```dockerfile
FROM ghcr.io/bruin-data/bruin:latest

# Copy your Bruin project and credentials
COPY . /workspace
WORKDIR /workspace
```

That's it! The Bruin image already has everything you need.

## Step 3: Build and Push Docker Image

### Create Artifact Registry Repository

```bash
# Set your region
REGION=us-central1

# Create repository
gcloud artifacts repositories create bruin-repo \
    --repository-format=docker \
    --location=${REGION} \
    --description="Bruin pipeline containers"
```

### Build and Push Image

```bash
# Configure Docker authentication
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Build image
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest .

# Push image
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest
```

## Step 4: Create Cloud Run Job

```bash
gcloud run jobs create bruin-pipeline \
    --image=${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest \
    --region=${REGION} \
    --max-retries=1 \
    --task-timeout=3600 \
    --memory=2Gi \
    --cpu=2 \
    --args="run",".","--environment","production"
```

## Step 5: Execute the Job

### Run Manually

```bash
gcloud run jobs execute bruin-pipeline --region=${REGION}
```

### Run Specific Pipeline

```bash
gcloud run jobs execute bruin-pipeline \
    --region=${REGION} \
    --args="run","pipelines/analytics","--environment","production"
```

### Run Validation

```bash
gcloud run jobs execute bruin-pipeline \
    --region=${REGION} \
    --args="validate","."
```

## Scheduling with Cloud Scheduler

### Create Scheduled Job (Daily at 3 AM)

```bash
# Create service account for scheduler
gcloud iam service-accounts create bruin-scheduler \
    --display-name="Bruin Cloud Scheduler"

# Grant Cloud Run Invoker role
gcloud run jobs add-iam-policy-binding bruin-pipeline \
    --region=${REGION} \
    --member="serviceAccount:bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

# Create scheduler job
gcloud scheduler jobs create http bruin-daily-run \
    --location=${REGION} \
    --schedule="0 3 * * *" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/bruin-pipeline:run" \
    --http-method=POST \
    --oauth-service-account-email="bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"
```

### Schedule Multiple Pipelines

Create separate jobs for different pipelines:

```bash
# Create ingestion job - runs every 6 hours
gcloud run jobs create bruin-ingestion \
    --image=${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest \
    --region=${REGION} \
    --max-retries=1 \
    --task-timeout=3600 \
    --memory=2Gi \
    --cpu=2 \
    --args="run","pipelines/ingestion","--environment","production"

gcloud run jobs add-iam-policy-binding bruin-ingestion \
    --region=${REGION} \
    --member="serviceAccount:bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

gcloud scheduler jobs create http bruin-ingestion-schedule \
    --location=${REGION} \
    --schedule="0 */6 * * *" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/bruin-ingestion:run" \
    --http-method=POST \
    --oauth-service-account-email="bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

# Create analytics job - runs daily at 6 AM
gcloud run jobs create bruin-analytics \
    --image=${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest \
    --region=${REGION} \
    --max-retries=1 \
    --task-timeout=3600 \
    --memory=2Gi \
    --cpu=2 \
    --args="run","pipelines/analytics","--environment","production"

gcloud run jobs add-iam-policy-binding bruin-analytics \
    --region=${REGION} \
    --member="serviceAccount:bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

gcloud scheduler jobs create http bruin-analytics-schedule \
    --location=${REGION} \
    --schedule="0 6 * * *" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/bruin-analytics:run" \
    --http-method=POST \
    --oauth-service-account-email="bruin-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"
```

### Test Scheduler Job

```bash
gcloud scheduler jobs run bruin-daily-run --location=${REGION}
```

## Using Cloud Workflows for Orchestration

For complex workflows with dependencies, use Cloud Workflows:

### Create Workflow Definition

Create `workflow.yaml`:

```yaml
main:
  steps:
    - validate:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: projects/${PROJECT_ID}/locations/${REGION}/jobs/bruin-pipeline
          body:
            overrides:
              containerOverrides:
                - env:
                    - name: COMMAND
                      value: validate
                    - name: PIPELINE
                      value: .
        result: validation_result

    - check_validation:
        switch:
          - condition: ${validation_result.status.succeededCount > 0}
            next: ingestion
        next: end

    - ingestion:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: projects/${PROJECT_ID}/locations/${REGION}/jobs/bruin-ingestion
        result: ingestion_result

    - parallel_analytics:
        parallel:
          branches:
            - analytics:
                call: googleapis.run.v1.namespaces.jobs.run
                args:
                  name: projects/${PROJECT_ID}/locations/${REGION}/jobs/bruin-analytics

            - reporting:
                call: googleapis.run.v1.namespaces.jobs.run
                args:
                  name: projects/${PROJECT_ID}/locations/${REGION}/jobs/bruin-reporting

    - end:
        return: "Workflow completed"
```

### Deploy Workflow

```bash
# Create service account for Workflow
gcloud iam service-accounts create bruin-workflow \
    --display-name="Bruin Workflow"

# Grant Cloud Run Invoker role for all jobs
for JOB in bruin-pipeline bruin-ingestion bruin-analytics bruin-reporting; do
  gcloud run jobs add-iam-policy-binding ${JOB} \
    --region=${REGION} \
    --member="serviceAccount:bruin-workflow@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"
done

# Deploy workflow
gcloud workflows deploy bruin-orchestrator \
    --location=${REGION} \
    --source=workflow.yaml \
    --service-account=bruin-workflow@${PROJECT_ID}.iam.gserviceaccount.com
```

### Schedule Workflow

```bash
gcloud scheduler jobs create http bruin-workflow-trigger \
    --location=${REGION} \
    --schedule="0 3 * * *" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/bruin-orchestrator/executions" \
    --http-method=POST \
    --oauth-service-account-email="bruin-workflow@${PROJECT_ID}.iam.gserviceaccount.com"
```

## Monitoring and Logging

### View Job Executions

```bash
# List recent executions
gcloud run jobs executions list \
    --job=bruin-pipeline \
    --region=${REGION} \
    --limit=10

# Describe a specific execution
gcloud run jobs executions describe EXECUTION_NAME \
    --region=${REGION}
```

### View Logs

```bash
# View logs for a specific execution
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=bruin-pipeline" \
    --limit=50 \
    --format=json

# Tail logs in real-time
gcloud logging tail "resource.type=cloud_run_job AND resource.labels.job_name=bruin-pipeline"

# Filter by severity
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=bruin-pipeline AND severity>=ERROR" \
    --limit=50
```

### Create Log-Based Metrics

```bash
gcloud logging metrics create bruin_job_errors \
    --description="Count of Bruin job errors" \
    --log-filter='resource.type="cloud_run_job"
                  resource.labels.job_name="bruin-pipeline"
                  severity>=ERROR'
```

### Set Up Alerts

```bash
# Create notification channel (example with email)
gcloud alpha monitoring channels create \
    --display-name="Bruin Alerts Email" \
    --type=email \
    --channel-labels=email_address=your-email@example.com

# Create alert policy
gcloud alpha monitoring policies create \
    --notification-channels=CHANNEL_ID \
    --display-name="Bruin Job Errors" \
    --condition-display-name="Error rate" \
    --condition-threshold-value=1 \
    --condition-threshold-duration=60s \
    --condition-filter='metric.type="logging.googleapis.com/user/bruin_job_errors" resource.type="cloud_run_job"'
```

## VPC Access

To connect to databases in a VPC:

### Create VPC Connector

```bash
gcloud compute networks vpc-access connectors create bruin-connector \
    --region=${REGION} \
    --network=default \
    --range=10.8.0.0/28
```

### Update Cloud Run Job

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --vpc-connector=bruin-connector \
    --vpc-egress=private-ranges-only
```

## CI/CD Integration

### Using Cloud Build

Create `cloudbuild.yaml`:

```yaml
steps:
  # Build the container image
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', '${_REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:${SHORT_SHA}', '.']

  # Push the container image to Artifact Registry
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', '${_REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:${SHORT_SHA}']

  # Update Cloud Run Job with new image
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: gcloud
    args:
      - 'run'
      - 'jobs'
      - 'update'
      - 'bruin-pipeline'
      - '--image=${_REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:${SHORT_SHA}'
      - '--region=${_REGION}'

images:
  - '${_REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:${SHORT_SHA}'

substitutions:
  _REGION: us-central1

options:
  logging: CLOUD_LOGGING_ONLY
```

Create build trigger:

```bash
gcloud builds triggers create github \
    --repo-name=your-repo \
    --repo-owner=your-username \
    --branch-pattern="^main$" \
    --build-config=cloudbuild.yaml
```

## Parallel Task Execution

Cloud Run Jobs can run multiple tasks in parallel:

```bash
gcloud run jobs create bruin-parallel \
    --image=${REGION}-docker.pkg.dev/${PROJECT_ID}/bruin-repo/bruin-pipeline:latest \
    --region=${REGION} \
    --set-env-vars=GCP_PROJECT=${PROJECT_ID},SECRET_NAME=bruin-config \
    --tasks=5 \
    --parallelism=5 \
    --max-retries=1 \
    --task-timeout=3600 \
    --memory=2Gi \
    --cpu=2
```

Each task gets a `CLOUD_RUN_TASK_INDEX` environment variable (0-4 in this case) that you can use to process different data partitions.

## Best Practices

### 1. Set Appropriate Resource Limits

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --memory=4Gi \
    --cpu=4 \
    --task-timeout=3600 \
    --max-retries=2
```

### 2. Use Secret Manager for All Credentials

Never use environment variables for sensitive data.

### 3. Tag Your Resources

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --labels=project=bruin,environment=production
```

### 4. Set Up Retries Appropriately

For transient failures:

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --max-retries=3
```

### 5. Monitor Execution Times

Use Cloud Logging and Monitoring to track job duration and optimize resource allocation.

## Cost Optimization

### 1. Right-Size Resources

Start with smaller resources and scale up based on monitoring data:

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --memory=1Gi \
    --cpu=1
```

### 2. Use Appropriate Timeouts

Set task timeout based on expected execution time:

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --task-timeout=1800  # 30 minutes
```

### 3. Minimize Retries for Non-Transient Failures

Only retry for transient failures to avoid wasted executions.

## Troubleshooting

### Job Fails to Start

Check job status:

```bash
gcloud run jobs executions describe EXECUTION_NAME \
    --region=${REGION}
```

View logs for errors:

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=bruin-pipeline AND severity>=ERROR" \
    --limit=10
```

### Secret Access Denied

Verify service account has Secret Manager access:

```bash
gcloud secrets add-iam-policy-binding bruin-config \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

### Timeout Errors

Increase task timeout:

```bash
gcloud run jobs update bruin-pipeline \
    --region=${REGION} \
    --task-timeout=3600  # 60 minutes
```

### VPC Connection Issues

Ensure:
- VPC connector is properly configured
- VPC connector has connectivity to required resources
- Job has appropriate VPC egress settings

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration
- Learn about [AWS ECS deployment](/deployment/cloud/aws-ecs) for container orchestration
- Review [quality checks](/quality/overview) to add validation

## Additional Resources

- [Cloud Run Jobs Documentation](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Cloud Workflows Documentation](https://cloud.google.com/workflows/docs)
- [Bruin Docker Images](https://github.com/bruin-data/bruin/pkgs/container/bruin)
- [Bruin CLI Documentation](/)
