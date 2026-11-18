# GitHub

GitHub is a developer platform that allows developers to create, store, manage and share their code.

Bruin supports GitHub as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from GitHub into your data platform.

In order to set up GitHub connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. Which fields the connection needs depends on the tables you read:

- `issues`, `pull_requests`, `stargazers` and `repo_events` read one repository, so they need `owner` and `repo`.
- `repositories` lists the repositories of `owner`, so it needs `owner`.
- `commits` can read one repository, every repository of `owner`, or a list of repositories of any owner. With a list such as `commits:bruin-data/*`, the connection needs neither `owner` nor `repo`. See [Reading commits from several repositories](#reading-commits-from-several-repositories).

For details on how to create an access token and which permissions it needs, see the [ingestr GitHub documentation](https://getbruin.com/docs/ingestr/supported-sources/github.html).

Follow the steps below to correctly set up GitHub as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to GitHub as a source, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  github:
    - name: my-github
      access_token: "token_123"
      owner: "John"
      repo: "repo_123"
```

- `access_token` (optional): Used for authentication with the GitHub API. It is required for `issues`, `pull_requests` and `stargazers`, and for private repositories. The other tables can read public data without it, but GitHub allows only 60 unauthenticated requests per hour, against 5,000 with a token.
- `owner` (optional): The user or organization that owns the repositories, e.g. `bruin-data`. Required for every table except `commits` with a repository list.
- `repo` (optional): The name of one repository, without the owner (e.g. `ingestr`, not `bruin-data/ingestr`). It needs `owner`. Required for `issues`, `pull_requests`, `stargazers` and `repo_events`.

A connection with only a token can read the commits of any repository the token can see:

```yaml
connections:
  github:
    - name: my-github-token
      access_token: "token_123"
```

When GitHub reports that a rate limit is exceeded, ingestr waits for the time GitHub asks for, at most one hour per wait, and retries up to 5 times, printing a warning with each wait.

### Step 2: Create an asset file for data ingestion

To ingest data from GitHub, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., github_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.github
type: ingestr
connection: postgres

parameters:
  source_connection: my-github
  source_table: 'issues'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the GitHub connection defined in .bruin.yml.
- `source_table`: The name of the table in GitHub you want to ingest.

## Available Source Tables

GitHub source allows ingesting the following sources into separate tables:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `commits` | `sha` (one repository) or `repo`, `sha` (several repositories) | `committed_at` | merge | Retrieves commits on the default branch of one or more repositories, with the repository's `owner/name` in `repo`, including signature verification (`commit.verification`). The interval start and end filter on the committer date, which `committed_at` mirrors. Commits pushed after their interval has already run are not picked up; use a lookback window (e.g. `interval_modifiers`). |
| `issues` | - | – | replace | Retrieves GitHub issues along with their comments and reactions. Full reload on each run. |
| `pull_requests` | - | – | replace | Retrieves pull requests with comments and reactions. Full reload on each run. |
| `repo_events` | `id` | `created_at` | merge | Retrieves recent repository events. Appends only new events using `created_at` filter. Only events from the past 30 days allowed. |
| `repositories` | `id` | – | replace | Retrieves the repositories of `owner`, including forks and archived repositories. Private repositories are included when the token can see them. Full reload on each run. |
| `stargazers` | - | – | replace | Retrieves stargazers. Full reload on each run. |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/github_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given GitHub table into your Postgres database.

<img alt="github" src="./media/github.png">

## Reading commits from several repositories

The `commits` table can read many repositories into one table. Which repositories it reads depends on the connection's `repo` and on an optional comma-separated list after `commits:` in `source_table`:

| `source_table` | Connection | Repositories read | PK |
|----------------|------------|-------------------|----|
| `commits` | `owner` and `repo` | that repository | `sha` |
| `commits` | `owner`, no `repo` | every repository of `owner` the token can list, except forks; archived repositories are included | `repo`, `sha` |
| `commits:<list>` | any; the list overrides `repo` | the listed repositories | `repo`, `sha` |

Each entry of the list is one of:

- `<repo>`: a repository of the connection's `owner`. Without `owner` on the connection, the run fails before reading anything.
- `<owner>/<repo>`: a repository of any owner.
- `<owner>/*`: every repository of `<owner>` the token can list, except forks.

Any other form, such as `<owner>/`, `*` or `<owner>/bruin-*`, fails the run. If a named repository does not exist or the token cannot read it, the run fails before any commit is read. A repository found through `<owner>/*` or through the connection's `owner` that GitHub refuses to serve is skipped with a warning. A repository that is both named and matched by `<owner>/*` is read once. See the [ingestr GitHub documentation](https://getbruin.com/docs/ingestr/supported-sources/github.html#reading-commits-from-several-repositories) for the full rules.

For example, with the token-only connection above, this asset reads every repository of `bruin-data` except forks, plus one repository of another owner:

```yaml
name: raw.github_commits
type: ingestr
connection: postgres

parameters:
  source_connection: my-github-token
  source_table: 'commits:bruin-data/*, other-org/tool'

  destination: postgres
```

With a connection that has `owner: bruin-data`, `source_table: 'commits:bruin, ingestr'` reads `bruin-data/bruin` and `bruin-data/ingestr`.

> [!WARNING]
> When the asset reads several repositories, do not mark `sha` alone as `primary_key: true` in the asset's `columns`. Bruin passes the asset's primary key to ingestr, which then uses it instead of `repo`, `sha`. A commit that is in more than one repository (for example a fork and its upstream) is then kept only once, under one of the repositories. Either leave the primary key out, or mark both `repo` and `sha`.

> [!WARNING]
> To switch an existing single-repository table to several repositories, run it once with `bruin run --full-refresh`. Rows written before the `repo` column existed have no `repo`, so they would not match the new rows.
