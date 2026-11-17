# Test Your Snowflake Pipelines Locally with LocalStack

Every test run on real Snowflake wakes up a warehouse, and you pay for it. This tutorial shows you how to run your existing Bruin pipelines against the [LocalStack Snowflake emulator](https://docs.localstack.cloud/snowflake/) instead. It's a Docker container that Bruin connects to like any other Snowflake account. Your SQL stays the same, and one small change switches between the emulator and real Snowflake.

**By the end of this tutorial, you'll:**

- Run the Snowflake emulator on your machine
- Point your own pipeline at it with its own connection or environment
- Run your pipeline and its quality checks without touching real Snowflake

## Prerequisites

- Bruin CLI v0.11.825 or newer. Run `bruin version` to check and `bruin upgrade` to update. [Installation instructions](../introduction/installation.md).
- Docker.
- A [LocalStack account](https://www.localstack.cloud/start-snowflake-trial) (30-day free trial, no credit card needed). Copy your auth token from the [LocalStack web app](https://app.localstack.cloud/).
- The [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`), or any SQL client that can connect to Snowflake.

## Step 1: Start the Snowflake emulator

We use the [`localstack/snowflake-next`](https://hub.docker.com/r/localstack/snowflake-next) image. The free trial covers it.

```bash
export LOCALSTACK_AUTH_TOKEN=<your-token>
docker pull localstack/snowflake-next:latest
docker run -d --name localstack-snowflake -p 127.0.0.1:4567:4566 \
  -e LOCALSTACK_AUTH_TOKEN localstack/snowflake-next:latest
```

This downloads the image and starts the emulator in the background on port 4567. If you choose another port, use it everywhere this tutorial says 4567.

You'll connect with host `snowflake.localhost.localstack.cloud`, user `test` and password `test`. The host name always points to your own machine.

If the container stops right away, run `docker logs localstack-snowflake`. A license error usually means the Snowflake trial license isn't assigned to your user yet. The [Auth Token page](https://docs.localstack.cloud/snowflake/getting-started/auth-token/) shows how to assign it.

## Step 2: Try the example pipeline (optional)

Want to see a working pipeline on the emulator first? Create one from our template:

```bash
bruin init localstack-snowflake-nyc-taxi
```

The [template's page](https://getbruin.com/docs/bruin/getting-started/templates-docs/localstack-snowflake-nyc-taxi-README.html) walks you through setting it up and running it.

## Step 3: Add an emulator connection

Add the emulator to the `.bruin.yml` at your project root. Pick a tab: a separate connection next to your real one, or an `emulator` environment that reuses your connection's name. The `localstack-snowflake-nyc-taxi` template uses a separate connection.

::: code-group

```yaml [Separate connection]
default_environment: default
environments:
  default:
    connections:
      snowflake:
        - name: snowflake-default
          account: myorg-myaccount
          username: ${SNOWFLAKE_USER}
          private_key_path: ~/.ssh/snowflake_key.p8
          database: ANALYTICS
          warehouse: TRANSFORMING
        - name: snowflake-emulator
          account: test
          username: test
          password: test
          host: snowflake.localhost.localstack.cloud
          port: 4567
          database: ANALYTICS
          warehouse: TRANSFORMING
```

```yaml [Emulator environment]
default_environment: default
environments:
  default:
    connections:
      snowflake:
        - name: snowflake-default
          account: myorg-myaccount
          username: ${SNOWFLAKE_USER}
          private_key_path: ~/.ssh/snowflake_key.p8
          database: ANALYTICS
          warehouse: TRANSFORMING
  emulator:
    connections:
      snowflake:
        - name: snowflake-default
          account: test
          username: test
          password: test
          host: snowflake.localhost.localstack.cloud
          port: 4567
          database: ANALYTICS
          warehouse: TRANSFORMING
```

:::

- **Separate connection:** point your pipeline at it by setting `snowflake: snowflake-emulator` under `default_connections` in `pipeline.yml`. Change any asset that names its own `connection:` too.
- **Emulator environment:** use the connection name your pipeline already uses (`snowflake-default` if it doesn't name one), and copy any other connections it needs under `emulator`. Then add `--environment emulator` to your commands.

Keep your production database and warehouse names, and use `snowflake.localhost.localstack.cloud` as the host. With `localhost` or `127.0.0.1`, the connection fails with a certificate error.

## Step 4: Prepare the empty emulator

A new emulator is as empty as a new Snowflake account. Your connection can't log in yet, because the warehouse and database it names don't exist. Create them once, with your own names:

```bash
snow sql --temporary-connection --account test --user test --password test \
  --host snowflake.localhost.localstack.cloud --port 4567 -q "
CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING;
CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS._BRUIN_STAGING;"
```

Any SQL client works too, and the emulator accepts any username and password. Both statements use `IF NOT EXISTS`, so it's safe to run them again, for example after you restart the container. Bruin creates your other schemas on the first run. `_BRUIN_STAGING` is the exception if your pipeline uses ingestr: it's ingestr's staging schema, and when several ingestr assets run at once, the emulator can reject one of their attempts to create it with "Object already exists".

Now check that Bruin can log in:

::: code-group

```bash [Separate connection]
bruin connections test --name snowflake-emulator
```

```bash [Emulator environment]
bruin connections test --name snowflake-default --env emulator
```

:::

If your pipeline reads tables it doesn't build, load a small sample of them too. Keep the data small, since the emulator runs on your machine.

> [!INFO]
> The emulator forgets everything when the container restarts. To keep your data, see LocalStack's [state management docs](https://docs.localstack.cloud/snowflake/capabilities/state-management/).

## Step 5: Run your pipeline on the emulator

The first time, add `--full-refresh`:

::: code-group

```bash [Separate connection]
bruin run --full-refresh path/to/your/pipeline
```

```bash [Emulator environment]
bruin run --environment emulator --full-refresh path/to/your/pipeline
```

:::

Incremental assets expect their tables to exist already, and on an empty emulator they don't. A full refresh creates them. After that, run it again without `--full-refresh`.

Your quality checks run on the emulator too. To look at the data, query it through the emulator connection:

::: code-group

```bash [Separate connection]
bruin query -c snowflake-emulator -q "SELECT COUNT(*) FROM mart.orders"
```

```bash [Emulator environment]
bruin query -c snowflake-default --env emulator -q "SELECT COUNT(*) FROM mart.orders"
```

:::

## Before you ship

The emulator tests your SQL and checks, not everything Snowflake does. Roles, permissions, performance and some SQL features only show up on real Snowflake, so run your pipeline there once before you ship. If a query works on Snowflake but fails on the emulator, check LocalStack's [feature coverage](https://docs.localstack.cloud/snowflake/feature-coverage/).

To run the emulator in CI, see LocalStack's [CI guide](https://docs.localstack.cloud/snowflake/getting-started/ci-cd/). Then run the same `bruin run` command in your job.
