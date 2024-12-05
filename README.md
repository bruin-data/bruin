<p align="center">
  <img src="./resources/logo-horizontal.svg" width="500" />
</p>

Bruin is a command-line tool for validating and running data transformations on SQL, similar to dbt. On top, bruin can
also run Python assets within the same pipeline.

<img alt="Welcome to VHS" src="./resources/demo.gif" width="1200" />

Bruin is built to make your life easier when it comes to data transformations:
- ✨ run SQL transformations on BigQuery/Snowflake
- 🐍 run Python in isolated environments in the same pipeline
- 💅 built-in data quality checks
- 🚀 Jinja templating to avoid repetition
- ✅ validate data pipelines end-to-end to catch issues early on via dry-run on live
- 📐 table/view materialization
- ➕ incremental tables
- ⚡ blazing fast pipeline execution: bruin is written in Golang and uses concurrency at every opportunity
- 🔒 secrets injection via environment variables
- 📦 easy to install and use

## Installation

### Linux/MacOS/Windows
```shell
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```

If you don't have `curl` installed, you can use `wget`:
```shell
wget -qO- https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```

> [!IMPORTANT]
> If you are on Windows, make sure to run the command in the [Git Bash](https://git-scm.com/downloads/win) or [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) terminal.

### macOS (Homebrew)
Alternatively, if you are a macOS users, you can use Homebrew to install Bruin CLI:
```shell
brew install bruin-data/tap/bruin
```

## Docs
You can see our documentation [here](https://bruin-data.github.io/bruin/).

## Community

Join our Slack community [here](https://join.slack.com/t/bruindatacommunity/shared_invite/zt-2dl2i8foy-bVsuMUauHeN9M2laVm3ZVg).

## Getting Started

All you need is a simple `pipeline.yml` in your Git repo:

```yaml
name: bruin-example
schedule: "daily"
start_date: "2023-03-01"

default_connections:
  google_cloud_platform: "gcp"
```

create a new folder called `assets` and create your first asset there `assets/bruin-test.sql`:

```sql
/* @bruin
name: dataset.bruin_test
type: bq.sql
materialization:
  type: table
@bruin */

SELECT 1 as result
```

bruin will take this result, and will create a `dataset.bruin_test` table on BigQuery. You can also use `view`
materialization type instead of `table` to create a view instead.

> **Snowflake assets**
> If you'd like to run the asset on Snowflake, simply replace the `bq.sql` with `sf.sql`, and define `snowflake` as a
> connection instead of `google_cloud_platform`.

Then let's create a Python asset `assets/hello.py`:

```python
""" @bruin
name: hello
depends: 
  - dataset.bruin_test
@bruin """

print("Hello, world!")
```

Once you are done, run the following command to validate your pipeline:

```shell
bruin validate .
```

You should get an output that looks like this:

```shell
Pipeline: bruin-example (.)
  No issues found

✓ Successfully validated 2 assets across 1 pipeline, all good.
```

If you have defined your credentials, bruin will automatically detect them and validate all of your queries using
dry-run.

## Environments

bruin allows you to run your pipelines / assets against different environments, such as development or production. The
environments are managed in the `.bruin.yml` file.

The following is an example configuration that defines two environments called `default` and `production`:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-dev-schema"
      generic:
        - name: KEY1
          value: value1
  production:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/prod-key.json"
          project_id: "my-project-prod"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-prod-schema" 
      generic:
        - name: KEY1
          value: value1
```

You can simply switch the environment using the `--environment` flag, e.g.:

```shell
bruin validate --environment production . 
```

### Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.
