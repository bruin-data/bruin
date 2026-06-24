# GitLab

[GitLab](https://gitlab.com/) is a DevOps platform for hosting Git repositories, managing issues and merge requests, and running CI/CD pipelines.

Bruin supports GitLab as a source for [Ingestr assets](/assets/ingestr).

## Configuration

### Step 1: Add a connection to .bruin.yml

```yaml
connections:
    gitlab:
        - name: 'my-gitlab'
          access_token: 'glpat-xxx'
```

- `access_token`: a GitLab personal access token with the `read_api` scope.
- `base_url` (optional): API base URL for self-managed instances, including the `/api/v4` suffix (e.g. `https://gitlab.example.com/api/v4`). Defaults to `https://gitlab.com/api/v4`.

### Step 2: Create an asset file

```yaml
name: public.gitlab
type: ingestr

parameters:
  source_connection: my-gitlab
  source_table: 'issues'
  destination: postgres
```

- `source_connection`: the GitLab connection defined in `.bruin.yml`.
- `source_table`: the GitLab table to ingest (see below).

## Available Source Tables

GitLab source allows ingesting the following resources:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `projects` | id | updated_at | merge | Projects the token is a member of. Scoped to the run interval via `updated_after`/`updated_before`. |
| `groups` | id | – | replace | Groups the token is a member of. Full reload on each run. |
| `users` | id | – | replace | Users across the projects the token is a member of. Full reload on each run. |
| `issues` | id | updated_at | merge | All issues across the projects the token is a member of. Scoped to the run interval via `updated_after`/`updated_before`. |
| `merge_requests` | id | updated_at | merge | All merge requests across the projects the token is a member of. Scoped to the run interval via `updated_after`/`updated_before`. |

> **Note**: GitLab objects carry both a global `id` and a project-scoped `iid`. ingestr keys on the global `id`. Nested fields such as `labels`, `assignees`, and `references` are preserved as JSON, and `description` fields remain Markdown strings.

