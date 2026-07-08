# Deployment Overview

Bruin can run anywhere the CLI can run. The right deployment option depends on how much infrastructure you want to operate, where your credentials live, and whether you need a managed scheduler, CI/CD runner, serverless job, container platform, or existing orchestrator.

For a managed setup with scheduling, monitoring, lineage, runs, backfills, and secure connection management, start with Bruin Cloud. If you prefer to own the runtime, use one of the self-managed options below.

<script setup>
import { withBase } from 'vitepress'
</script>

<div class="deployment-grid">
  <a class="deployment-card deployment-card--featured" :href="withBase('/cloud/overview.html')">
    <span class="deployment-card__eyebrow">Managed</span>
    <strong>Bruin Cloud</strong>
    <span>Run scheduled pipelines from your Git repo without maintaining servers. Bruin Cloud handles orchestration, logs, lineage, backfills, notifications, and encrypted connections.</span>
    <span class="deployment-card__button">Open Bruin Cloud</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/github-actions.html')">
    <span class="deployment-card__eyebrow">CI/CD runner</span>
    <strong>GitHub Actions</strong>
    <span>Use GitHub-hosted or self-hosted runners to validate, test, and run pipelines on pushes, schedules, or manual triggers.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/cloud/gitlab-cicd.html')">
    <span class="deployment-card__eyebrow">CI/CD runner</span>
    <strong>GitLab CI/CD</strong>
    <span>Run Bruin from GitLab pipelines with variables for secrets, scheduled pipelines, manual jobs, and deployment environments.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/vm-deployment.html')">
    <span class="deployment-card__eyebrow">Virtual machine</span>
    <strong>Ubuntu VM with Cron</strong>
    <span>Install the Bruin CLI on a VM and schedule runs with cron. This is simple, explicit, and works well when you already manage servers.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/cloud/aws-lambda.html')">
    <span class="deployment-card__eyebrow">AWS serverless</span>
    <strong>AWS Lambda</strong>
    <span>Package Bruin in a Lambda container image for short scheduled or event-driven runs with EventBridge and Secrets Manager.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/cloud/aws-ecs.html')">
    <span class="deployment-card__eyebrow">AWS containers</span>
    <strong>AWS ECS</strong>
    <span>Run Bruin as ECS Fargate tasks for containerized workloads that need more runtime, memory, or operational control than Lambda.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/cloud/google-cloud-run.html')">
    <span class="deployment-card__eyebrow">GCP serverless</span>
    <strong>Google Cloud Run Jobs</strong>
    <span>Run Bruin in Cloud Run Jobs with Artifact Registry, Secret Manager, Cloud Scheduler, and Cloud Logging.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>

  <a class="deployment-card" :href="withBase('/deployment/airflow.html')">
    <span class="deployment-card__eyebrow">Orchestrator</span>
    <strong>Apache Airflow</strong>
    <span>Use Airflow DAGs to call Bruin from BashOperator or KubernetesPodOperator when Airflow already owns your scheduling layer.</span>
    <span class="deployment-card__button">Open guide</span>
  </a>
</div>

## Choosing an Option

| Option | Best for | Tradeoff |
| --- | --- | --- |
| [Bruin Cloud](/cloud/overview) | Managed scheduling, monitoring, lineage, backfills, and secrets | Requires using the hosted Bruin Cloud control plane |
| [GitHub Actions](/deployment/github-actions) | GitHub-native scheduled or manual production runs | Runner timeouts and concurrency depend on your GitHub plan and runner setup |
| [GitLab CI/CD](/deployment/cloud/gitlab-cicd) | GitLab-native scheduled or manual production runs | Runtime behavior depends on GitLab runners and CI/CD variables |
| [Ubuntu VM with Cron](/deployment/vm-deployment) | Simple self-managed deployments on EC2, Compute Engine, DigitalOcean, or another VM | You maintain the server, cron, logs, updates, and credentials |
| [AWS Lambda](/deployment/cloud/aws-lambda) | Short event-driven or scheduled AWS workloads | Lambda timeout and package constraints limit longer pipelines |
| [AWS ECS](/deployment/cloud/aws-ecs) | Longer AWS container tasks with more control over CPU, memory, and networking | Requires ECS, ECR, IAM, scheduling, and logging setup |
| [Google Cloud Run Jobs](/deployment/cloud/google-cloud-run) | Serverless GCP batch jobs with Cloud Scheduler | Requires container build, Artifact Registry, IAM, and GCP job configuration |
| [Apache Airflow](/deployment/airflow) | Teams that already operate Airflow and want Bruin inside existing DAGs | You maintain Airflow and the worker or Kubernetes runtime |

## CI/CD Validation

If you only want to validate and unit-test pipelines in pull requests, use the CI/CD integration guides instead of the deployment guides:

- [GitHub Actions CI](/cicd/github-action)
- [GitLab CI/CD validation](/cicd/gitlab-ci)
- [CircleCI](/cicd/circleci)
- [Jenkins](/cicd/jenkins)
- [Azure Pipelines](/cicd/azure-pipelines)
