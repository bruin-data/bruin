# Snowflake in CI/CD

LocalStack's Snowflake emulator lets you test a pipeline without a real Snowflake warehouse. Most Snowflake pipelines get tested in a dev warehouse that costs additional credits. This tutorial runs your pipeline against the emulator on every pull request instead. A failing check shows up on the pull request, before anything is merged.

**What you get:**

- **No Snowflake credits used.** The [LocalStack Snowflake emulator](https://docs.localstack.cloud/snowflake/) runs in a Docker container inside the CI job. Your SQL stays the same.
- **Production stays untouched.** CI uses its own config with the emulator's test login.
- **Your whole pipeline runs**, including ingestr loads and every quality check.
- **About a minute per run**, and about 10 minutes to set up.

We use our example pipeline, `localstack-snowflake-nyc-taxi`, throughout, so you can watch it pass before switching to your own.

## Prerequisites

- A GitHub repository and the [Bruin CLI](../introduction/installation.md).
- A [LocalStack account](https://www.localstack.cloud/start-snowflake-trial) (30-day free trial, no credit card needed).
- A LocalStack **CI Auth Token** from the [Auth Tokens page](https://app.localstack.cloud/workspace/auth-tokens). Personal developer tokens aren't allowed in CI.

## Step 1: Add the example pipeline

From your repository root, run:

```bash
bruin init localstack-snowflake-nyc-taxi
```

This adds a `localstack-snowflake-nyc-taxi/` folder that loads NYC taxi trips, weather, and taxi zones with ingestr, builds tables with SQL and runs 20 quality checks. Its [template page](../templates-docs/localstack-snowflake-nyc-taxi-README.md) explains each asset.

## Step 2: Store the token as a secret

In your repository, go to **Settings** → **Secrets and variables** → **Actions** and add a secret named `LOCALSTACK_AUTH_TOKEN` with your CI Auth Token.

## Step 3: Store the emulator config as a secret

Your `.bruin.yml` usually holds production credentials and isn't in Git, so CI gets its own config with the same connection names. Add a second secret named `BRUIN_CONFIG` in the same place, with this as its value. For the example pipeline, that's its Snowflake connection and three public data sources:

```yaml
default_environment: emulator
environments:
  emulator:
    connections:
      snowflake:
        - name: snowflake-localstack-demo
          account: test
          username: test
          password: test
          host: snowflake.localhost.localstack.cloud
          port: 4566
          database: LOCALSTACK_DEMO
          warehouse: COMPUTE_WH
      http:
        - name: nyc-taxi-trips-2024-01
          url: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
        - name: nyc-weather-2024-01
          url: https://archive-api.open-meteo.com/v1/archive?latitude=40.78&longitude=-73.97&start_date=2024-01-01&end_date=2024-01-31&daily=temperature_2m_max,precipitation_sum,snowfall_sum&timezone=America%2FNew_York
        - name: nyc-taxi-zones
          url: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Since the connection names match, the assets don't change.

For your own pipeline, use its connection names, and:

- **Keep your production database and warehouse names**, so fully qualified table names still work.
- **Use `snowflake.localhost.localstack.cloud` as the host.** It's the only name the emulator's certificate covers; `localhost` fails with a certificate error.

## Step 4: Add the workflow

Create `.github/workflows/snowflake-emulator.yml`:

```yaml
name: Snowflake emulator tests

on:
  pull_request:

jobs:
  bruin:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4

      - name: Start LocalStack for Snowflake
        run: |
          npm install -g @localstack/lstk
          lstk start --type snowflake --image localstack/snowflake-next --non-interactive
        env:
          LOCALSTACK_AUTH_TOKEN: ${{ secrets.LOCALSTACK_AUTH_TOKEN }}

      - uses: bruin-data/setup-bruin@main
        with:
          token: ${{ github.token }}

      - name: Install the Snowflake CLI
        run: pipx install snowflake-cli

      - name: Create the warehouse and database
        run: |
          snow sql --temporary-connection --account test --user test --password test \
            --host snowflake.localhost.localstack.cloud --port 4566 -q "
          CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
          CREATE DATABASE IF NOT EXISTS LOCALSTACK_DEMO;"

      - name: Create Bruin config
        run: echo "$BRUIN_CONFIG" > .bruin.yml
        env:
          BRUIN_CONFIG: ${{ secrets.BRUIN_CONFIG }}

      - name: Run the pipeline and its checks
        run: bruin run --full-refresh localstack-snowflake-nyc-taxi
```

For your own pipeline, replace `localstack-snowflake-nyc-taxi` with its folder, and `COMPUTE_WH` and `LOCALSTACK_DEMO` with its warehouse and database.

Here's what the steps do:

- **Start LocalStack for Snowflake** is the setup from LocalStack's [Snowflake CI guide](https://docs.localstack.cloud/snowflake/getting-started/ci-cd/). `lstk start` returns once the emulator is ready. `--image` picks the `snowflake-next` emulator, which runs this pipeline.
- **`setup-bruin`** installs the latest Bruin. The `token` avoids "API rate limit exceeded" errors.
- **Create the warehouse and database.** The emulator starts empty, and Bruin can't log in until they exist. Bruin creates the schemas itself.
- **Create Bruin config** writes the `BRUIN_CONFIG` secret to `.bruin.yml`, where `bruin run` looks for it. Passing it through `env` keeps quotes in the config from breaking the command.
- **Run the pipeline.** `--full-refresh` builds every table from scratch, which incremental assets need on an empty emulator. Quality checks run as part of `bruin run`.

> [!TIP]
> `localstack/snowflake-next` tracks `latest`, which changes often. Once your workflow passes, pin `--image` to a digest, like `localstack/snowflake-next@sha256:…`.

## Step 5: Open a pull request

Your repository now looks like this:

```text
your-repo/
├── .github/
│   └── workflows/
│       └── snowflake-emulator.yml
├── localstack-snowflake-nyc-taxi/
│   ├── assets/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── dashboards/
│   ├── pipeline.yml
│   └── README.md
├── .bruin.yml        # local only, ignored by Git
└── .gitignore
```

Push the pipeline folder and the workflow on a branch and open a pull request. The job ends with:

```text
✓ Assets executed      7 succeeded
✓ Quality checks       20 succeeded
```

If an asset or check fails, the job fails and the pull request shows it. To try it, open `localstack-snowflake-nyc-taxi/assets/bronze/taxi_zones.asset.yml` and remove `"Manhattan"` from the `accepted_values` list of the `borough` check:

```diff
-        value: ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island", "EWR", "Unknown", "N/A"]
+        value: ["Bronx", "Brooklyn", "Queens", "Staten Island", "EWR", "Unknown", "N/A"]
```

The check stays, but the Manhattan rows no longer match it, so it fails. Push again, and the job ends with:

```text
✗ Assets executed      1 failed due to checks / 2 succeeded / 4 skipped
✗ Quality checks       1 failed / 2 succeeded / 17 skipped

1 assets failed
└── bronze.taxi_zones
    └── borough.accepted_values - column 'borough' has 69 rows that are not in the accepted values
```

## Run it on your machine

To debug a failed job, start the emulator locally with a [developer auth token](https://app.localstack.cloud/workspace/auth-tokens):

```bash
export LOCALSTACK_AUTH_TOKEN=<your-token>
docker run -d --name localstack-snowflake -p 127.0.0.1:4566:4566 \
  -e LOCALSTACK_AUTH_TOKEN localstack/snowflake-next:latest
```

Then run the workflow's `snow sql` and `bruin run` commands. `bruin run` uses your local `.bruin.yml`. If port 4566 is taken, map another and update `port` there and in the `snow sql` command.

## Before you ship

The emulator tests your SQL and checks, not everything Snowflake does. Roles, permissions, performance, and some SQL features only show up on real Snowflake, so run there once before you ship. If a query fails only on the emulator, check LocalStack's [feature coverage](https://docs.localstack.cloud/snowflake/feature-coverage/).

Using another CI provider? LocalStack's [CI guide](https://docs.localstack.cloud/snowflake/getting-started/ci-cd/) covers CircleCI and GitLab CI; the Bruin steps stay the same.
