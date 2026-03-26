# Core Concepts

Bruin is built around a few simple but powerful concepts. This page gives a brief orientation — each concept links to its full documentation.

| Concept | Description |
|---------|-------------|
| [Pipelines](/pipelines/definition) | A group of assets that are executed together in dependency order |
| [Assets](/assets/definition-schema) | Anything that carries value derived from data (tables, views, files, models) |
| [Variables](/variables/overview) | Dynamic values injected into your asset code during execution |
| [Connections](/connections/overview) | Named configurations for authenticating with data platforms and sources |
| [Commands](/commands/overview) | CLI operations to run, validate, and manage your pipelines |
| [Project](/core-concepts/project) | A Git repository containing your pipelines, configured via `.bruin.yml` |
| Orchestration | How Bruin executes pipelines — scheduling, dependency resolution, concurrency, and deployment |

## Orchestration

Bruin orchestrates pipeline execution through several features that work together:

- **[Dependencies](/assets/definition-schema#depends)**: Assets declare their dependencies via the [`depends`](/assets/definition-schema#depends) field. Bruin uses this to determine execution order — assets run only after all their upstream dependencies have succeeded, and assets without dependencies on each other run in parallel automatically.
- **[Lineage](/commands/lineage)**: The dependency graph forms a lineage that lets you trace how data flows through your pipeline. You can visualize it via the [`lineage` command](/commands/lineage) or the [VS Code lineage panel](/vscode-extension/panels/lineage-panel). In Bruin Cloud, lineage extends [across pipelines](/cloud/cross-pipeline).
- **[Scheduling](/pipelines/definition#schedule)**: Pipelines can be scheduled using cron expressions in `pipeline.yml`, defining when and how often they run.
- **[Concurrency](/getting-started/concurrency)**: Control how many assets run simultaneously (`--workers`) and how many pipeline runs can overlap (`concurrency` setting in Bruin Cloud).
- **[Deployment](/deployment/vm-deployment)**: Run pipelines locally, on VMs with cron, via CI/CD (GitHub Actions, GitLab), or on cloud infrastructure (AWS Lambda, ECS, Google Cloud Run).
- **[Bruin Cloud](/cloud/overview)**: Managed orchestration with scheduling, monitoring, notifications, and cross-pipeline dependencies — no infrastructure to manage.
