# Pipelines

The **Pipelines** page in Bruin Cloud is where you enable pipelines, trigger runs, watch run history, manage backfills, and inspect lineage.

## Enable a pipeline

Pipelines synced from a repo start disabled. Enabling one tells Bruin Cloud to run it on schedule. If connections are missing, you are prompted to add them inline.

### 1. Open your pipelines

From the homepage, go to **Catalog → Pipelines**.

### 2. Select and enable

Pick a pipeline and click **Enable selected pipelines**.

### 3. Add missing connections

If the pipeline references connections you have not configured, Bruin Cloud lists them. For each:

1. Click the missing connection — the name is filled in for you.
2. Pick the **connection type** — a built-in (BigQuery, Postgres, Snowflake, etc.) or a generic secret.
3. Enter the credentials.
4. Click **Create**. Bruin Cloud validates the connection. To skip validation, click **Create without validation**.

Repeat until the list is empty. See [Connections](/cloud/connections) for the longer walkthrough.

### 4. First run

The first run triggers automatically when a new pipeline is enabled — no need to click **New run**.

### 5. Confirm it is running

Open the pipeline page and check:

- Status is **active**.
- A new run appears in the runs list.

## The pipeline page

Once a pipeline is enabled, the pipeline page is where you operate it. Runs, assets, backfills, lineage, and manual actions all live here.

### Open a pipeline

Two ways from the **Overview** page:

- Click a pipeline in the left sidebar.
- Open **Catalogs → Pipelines** and pick one.

### Runs

The bottom panel lists previous runs. You can:

- Filter by status.
- Mark a run as successful or failed.
- Rerun all assets, or rerun only the failed jobs.

### Assets

The **Assets** tab lists every asset in the pipeline with its type, owner, schedule, and last run state.

### Backfills

The **Backfills** tab shows every backfill that has been created. Click one to see its intervals, jobs, date range, status, and progress bar.

### Lineage

The lineage panel shows how assets connect. Click any asset to jump to it. Expand to full screen for large pipelines.

### Pipeline details

The right panel shows:

- **Name**, **schedule**, **start date**, **owner**
- **Last commit** from the connected repo
- **Run durations** — recent runs at a glance, so failures stand out
- **Connections** used by this pipeline
- **Activity** — a log of manual actions taken by users

### New run

The **New run** button (top right) triggers a manual run. Options:

- Toggle **full refresh**.
- Add **notes** and **tags** — they show up in the activity log.
- Run a single interval, or create multiple jobs across intervals.

#### Backfills

- **Auto split by schedule** — one job per scheduled interval. A 23-day range on a daily schedule produces 23 jobs.
- **Manual split** — pick the interval and the number of splits. Useful for processing data in chunks.

### Status

- **Next run** sits next to the New run button. Hover for local, UTC, and ISO time.
- The pipeline's **status** and **timestamp** (UTC) are at the top.

### Pipeline menu

The menu (top right) lets you **disable** or **delete** the pipeline.

> [!TIP]
> Want to exclude a single asset from the pipeline's scheduled runs? See [Can I skip a single asset from scheduled runs?](/cloud/faq#can-i-skip-a-single-asset-from-scheduled-runs) in the FAQ.

