# AI Templates

AI templates install starter files for AI agents that work on Bruin repositories. They are installed with [`bruin init`](/commands/init), but they are not pipeline templates.

AI templates install files into the Git repository root when Bruin can find one, otherwise into the current directory. They do not create a pipeline folder, initialize Git, or create `.bruin.yml` by default. They also do not accept a folder argument or `--in-place`.

```bash
# Open the AI template selector
bruin init ai

# Install AGENTS.md directly
bruin init ai-agents-md

# Install the troubleshooting skill pack directly
bruin init ai-skill-self-heal
```

## Available AI Templates

| Template | What it installs |
| --- | --- |
| `ai-agents-md` | A generic `AGENTS.md` starter with Bruin-oriented agent instructions. |
| `ai-skill-self-heal` | Starter troubleshooting skills under `.agents/skills/`. |

The self-healing starter is a pack of focused skills:

* `pipeline-diagnose`
* `schema-drift-check`
* `duplicate-investigate`
* `freshness-check`
* `quality-check-investigate`
* `maintenance-action`

Each skill includes a placeholder `Actions` section for repository-specific behavior. Until customized, the skills only diagnose and report findings.

## Runtime Expectations

The starter skills are primarily meant for AI agents configured inside Bruin Cloud. In that environment, agents should use Bruin Cloud MCP tools when available. If they use the CLI, the relevant Cloud commands include:

```bash
# Diagnose the latest failed or recent run
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest

# Read run and asset logs
bruin cloud runs get --project-id <project-id> --run-id <run-id>
bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>
bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>

# Create or rerun Cloud runs
bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline-name>
bruin cloud runs rerun --project-id <project-id> --run-id <run-id> --only-failed

# Enable or disable Cloud pipelines
bruin cloud pipelines enable --project-id <project-id> --pipeline <pipeline-name>
bruin cloud pipelines disable --project-id <project-id> --pipeline <pipeline-name>
```

For local development, the skills should rely on local terminal commands such as `bruin validate`, `bruin render`, `bruin query`, and `bruin run`. Local troubleshooting should read terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Local runs should be created with `bruin run` rather than Bruin Cloud run commands.

When troubleshooting data issues, the skills guide agents to find one specific failing row, key, partition, or timestamp first and keep upstream queries filtered to that example. For layered pipelines such as bronze, silver, and gold, agents should start at the asset where the issue appears, trace upstream through lineage until the issue first appears, then inspect that asset's SQL or Python logic to isolate the likely join, filter, cast, incremental condition, function, or transformation step. If the user has allowed fixes, agents should change only the isolated logic, verify the same bad example first, and only then run the broader quality check, freshness check, duplicate check, or pipeline command.

If investigation or verification requires actual asset or pipeline runs, agents should prefer a dev or shadow environment. If one is not available, they should ask whether to run in production or create temporary copies of affected tables to reproduce and test the issue.

For any other agent runtime or orchestrator, customize the installed skills with the correct log source and action mechanism before using them to read logs, trigger runs, enable or disable pipelines, mark statuses, or change external systems.

## Safe Sample Copies

When an agent needs to investigate real data issues, instruct it to reproduce the smallest useful slice of the problem outside production before changing pipeline logic. The safest pattern is:

* Find one failing row, key, partition, timestamp, or column first.
* Create dev, shadow, or temporary copies of only the affected upstream and downstream tables.
* Prefer a small sample filtered to the failing example plus a small amount of surrounding context, such as the same date partition, tenant, customer, order, or batch.
* Preserve enough lineage to reproduce the issue locally or in a dev warehouse: source sample, intermediate sample, final asset sample, and the SQL or Python logic under test.
* Run the suspected fix against the sampled copies first.
* Verify the known bad example before running broader checks, reruns, or backfills.

For large tables, avoid full-table copies unless the user explicitly approves the cost and blast radius. A useful agent instruction is:

```text
Before changing production logic, create the smallest safe dev sample that reproduces the issue.
Copy only the affected keys, partitions, or time window from each upstream table needed for lineage.
Run the current logic against the sample, prove the failure, apply the proposed fix to the sample, and prove the same key or partition is fixed before touching the real asset.
```

## Sample Pipeline and Test

This section is a fully mocked local demo. It creates a dummy DuckDB pipeline, deliberately introduces common data and pipeline issues, then gives agent prompts and proof commands for testing the installed self-healing skills end to end.

Use a throwaway local DuckDB project so agents can safely investigate, run assets, and test fixes without production data. Recreate the fixture before each scenario unless you intentionally want to test multiple failures at once.

### Create the Sample Pipeline

```bash
BRUIN=${BRUIN:-bruin}
WORKDIR=$(mktemp -d)
cd "$WORKDIR"
git init

$BRUIN init ai-skill-self-heal
mkdir -p assets

cat > .bruin.yml <<'YAML'
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-skill-test
          path: skill-test.duckdb
YAML

cat > pipeline.yml <<'YAML'
name: ai-skill-test
default_connections:
  duckdb: duckdb-skill-test
YAML

cat > assets/bronze_orders.sql <<'SQL'
/* @bruin
name: bronze_orders
type: duckdb.sql
materialization:
  type: table
columns:
  - name: order_id
    type: INTEGER
  - name: user_id
    type: INTEGER
  - name: transaction_date
    type: DATE
  - name: amount
    type: DOUBLE
  - name: status
    type: VARCHAR
@bruin */

SELECT 1001 AS order_id, 501 AS user_id, DATE '2025-01-01' AS transaction_date, 25.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1002 AS order_id, 502 AS user_id, DATE '2025-01-01' AS transaction_date, 35.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1003 AS order_id, 503 AS user_id, DATE '2025-01-02' AS transaction_date, 40.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1004 AS order_id, 504 AS user_id, DATE '2025-01-03' AS transaction_date, 50.00 AS amount, 'paid' AS status;
SQL

cat > assets/silver_orders.sql <<'SQL'
/* @bruin
name: silver_orders
type: duckdb.sql
materialization:
  type: table
depends:
  - bronze_orders
columns:
  - name: order_id
    type: INTEGER
  - name: user_id
    type: INTEGER
  - name: transaction_date
    type: DATE
  - name: amount
    type: DOUBLE
  - name: status
    type: VARCHAR
@bruin */

WITH typed_orders AS (
    SELECT
        order_id,
        user_id,
        transaction_date,
        amount AS amount,
        status
    FROM bronze_orders
    WHERE status <> 'cancelled'
)

SELECT * FROM typed_orders
UNION ALL
SELECT * FROM typed_orders WHERE order_id = 1002;
SQL

cat > assets/gold_order_report.sql <<'SQL'
/* @bruin
name: gold_order_report
type: duckdb.sql
materialization:
  type: table
depends:
  - silver_orders
columns:
  - name: order_id
    type: INTEGER
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: user_id
    type: INTEGER
  - name: transaction_date
    type: DATE
    checks:
      - name: not_null
  - name: amount
    type: DOUBLE
    checks:
      - name: positive
  - name: status
    type: VARCHAR
custom_checks:
  - name: latest partition exists
    query: SELECT CASE WHEN max(transaction_date) = DATE '2025-01-03' THEN 1 ELSE 0 END FROM gold_order_report
    value: 1
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount,
    status
FROM silver_orders;
SQL

$BRUIN validate .
$BRUIN render assets/gold_order_report.sql
$BRUIN query --connection duckdb-skill-test --query "SELECT current_database() AS duckdb_database;"
```

The setup should print a successful validation for three assets, render SQL that reads from `silver_orders`, and return the active DuckDB database. That proves the skills, pipeline files, and DuckDB connection are in place before you create any issue.

### Run Agent Test Scenarios

Use one fresh copy of the fixture for each scenario. If you are testing with an agent runner that supports parallel work, create one sub-agent per scenario and give each sub-agent its own copy of the throwaway directory. Ask every agent to read the named skill file under `.agents/skills/`, print the commands it runs, capture the row or error that proves the issue exists, isolate the smallest upstream cause, apply only the approved fix, and print the rerun/backfill proof.

Use this prompt shape for each scenario:

```text
Use .agents/skills/<skill-name>/SKILL.md.
Create the documented issue in this throwaway DuckDB pipeline.
Prove the tables were created and prove the issue exists with a filtered query or failing Bruin command.
Trace lineage from gold_order_report to silver_orders to bronze_orders, keeping queries filtered to the failing key, date, or column.
Fix only the isolated cause.
Rerun with bruin run --full-refresh --backfill-id <scenario>-fix --backfill-total 1 .
Verify the specific bad example first, then run bruin run --only checks .
Report the commands, important output, changed file, and final status.
```

For these local table-materialization tests, `--full-refresh` makes reruns deterministic after an agent edits SQL. It is not a requirement for every real pipeline fix.

### Mock Duplicate Investigation

The base fixture already contains a duplicate row for `order_id = 1002` in `silver_orders`.

```bash
$BRUIN run . || true
$BRUIN query --connection duckdb-skill-test --query "SHOW TABLES;"
$BRUIN query --connection duckdb-skill-test --query "SELECT order_id, count(*) AS row_count FROM gold_order_report GROUP BY 1 HAVING count(*) > 1;"
$BRUIN query --connection duckdb-skill-test --query "SELECT 'gold' AS layer, count(*) AS row_count FROM gold_order_report WHERE order_id = 1002 UNION ALL SELECT 'silver' AS layer, count(*) AS row_count FROM silver_orders WHERE order_id = 1002 UNION ALL SELECT 'bronze' AS layer, count(*) AS row_count FROM bronze_orders WHERE order_id = 1002;"
```

Expected proof:

* `bruin run` fails `gold_order_report:order_id:unique`.
* `SHOW TABLES` lists `bronze_orders`, `silver_orders`, and `gold_order_report`.
* The duplicate query returns `1002` with `row_count = 2`.
* The lineage query shows the duplicate first appears in `silver_orders`, not `bronze_orders`.

Ask the agent to use `duplicate-investigate`, start from `order_id = 1002`, trace upstream through `gold_order_report -> silver_orders -> bronze_orders`, isolate the extra `UNION ALL` in `assets/silver_orders.sql`, and fix it only if you explicitly allow fixes. After the fix, verify the specific key first, then run the broader check and a tagged rerun:

```bash
$BRUIN run --full-refresh --backfill-id duplicate-fix --backfill-total 1 .
$BRUIN query --connection duckdb-skill-test --query "SELECT order_id, count(*) AS row_count FROM gold_order_report WHERE order_id = 1002 GROUP BY 1 HAVING count(*) > 1;"
$BRUIN run --only checks .
```

The filtered duplicate query should return no rows, and the check-only run should pass.

### Mock Quality Check Investigation

Create a negative amount after removing the duplicate from the base fixture:

```bash
perl -0pi -e 's/\nUNION ALL\nSELECT \* FROM typed_orders WHERE order_id = 1002;//' assets/silver_orders.sql
perl -0pi -e 's/amount AS amount/CASE WHEN order_id = 1003 THEN -amount ELSE amount END AS amount/' assets/silver_orders.sql
$BRUIN run --full-refresh . || true
$BRUIN query --connection duckdb-skill-test --query "SHOW TABLES;"
$BRUIN query --connection duckdb-skill-test --query "SELECT order_id, amount FROM gold_order_report WHERE order_id = 1003;"
$BRUIN query --connection duckdb-skill-test --query "SELECT 'gold' AS layer, amount FROM gold_order_report WHERE order_id = 1003 UNION ALL SELECT 'silver' AS layer, amount FROM silver_orders WHERE order_id = 1003 UNION ALL SELECT 'bronze' AS layer, amount FROM bronze_orders WHERE order_id = 1003;"
```

Expected proof:

* `bruin run` fails `gold_order_report:amount:positive`.
* `SHOW TABLES` lists `bronze_orders`, `silver_orders`, and `gold_order_report`.
* `order_id = 1003` has a negative amount in `gold_order_report`.
* The lineage query shows the negative amount first appears in `silver_orders`.

Ask the agent to use `quality-check-investigate`, start from `order_id = 1003`, trace the negative amount upstream, isolate the expression in `silver_orders`, and fix only that expression if fixes are allowed. Verify the row, then rerun:

```bash
$BRUIN run --full-refresh --backfill-id quality-fix --backfill-total 1 .
$BRUIN query --connection duckdb-skill-test --query "SELECT order_id, amount FROM gold_order_report WHERE order_id = 1003;"
$BRUIN run --only checks .
```

The row should show a positive amount, and the check-only run should pass.

### Mock Freshness Check

Create a stale latest partition after removing the duplicate from the base fixture:

```bash
perl -0pi -e 's/\nUNION ALL\nSELECT \* FROM typed_orders WHERE order_id = 1002;//' assets/silver_orders.sql
perl -0pi -e "s/WHERE status <> 'cancelled'/WHERE status <> 'cancelled' AND transaction_date < DATE '2025-01-03'/" assets/silver_orders.sql
$BRUIN run . || true
$BRUIN query --connection duckdb-skill-test --query "SHOW TABLES;"
$BRUIN query --connection duckdb-skill-test --query "SELECT max(transaction_date) AS max_transaction_date FROM gold_order_report;"
$BRUIN query --connection duckdb-skill-test --query "SELECT 'gold' AS layer, count(*) AS row_count FROM gold_order_report WHERE transaction_date = DATE '2025-01-03' UNION ALL SELECT 'silver' AS layer, count(*) AS row_count FROM silver_orders WHERE transaction_date = DATE '2025-01-03' UNION ALL SELECT 'bronze' AS layer, count(*) AS row_count FROM bronze_orders WHERE transaction_date = DATE '2025-01-03';"
```

Expected proof:

* `bruin run` fails the `latest partition exists` custom check.
* `SHOW TABLES` lists `bronze_orders`, `silver_orders`, and `gold_order_report`.
* `max(transaction_date)` is `2025-01-02`.
* The lineage query shows `2025-01-03` exists in `bronze_orders` but not in `silver_orders` or `gold_order_report`.

Ask the agent to use `freshness-check`, start from the missing `transaction_date = DATE '2025-01-03'`, trace upstream, isolate the stale filter in `silver_orders`, and fix it only if allowed. Verify the date, then rerun:

```bash
$BRUIN run --full-refresh --backfill-id freshness-fix --backfill-total 1 .
$BRUIN query --connection duckdb-skill-test --query "SELECT max(transaction_date) AS max_transaction_date FROM gold_order_report;"
$BRUIN run --only checks .
```

The max date should be `2025-01-03`, and the check-only run should pass.

### Mock Schema Drift Check

Create a source column rename after removing the duplicate from the base fixture:

```bash
perl -0pi -e 's/\nUNION ALL\nSELECT \* FROM typed_orders WHERE order_id = 1002;//' assets/silver_orders.sql
perl -0pi -e 's/ AS amount/ AS gross_amount/g' assets/bronze_orders.sql
$BRUIN render assets/bronze_orders.sql --raw-query
$BRUIN render assets/silver_orders.sql --raw-query
$BRUIN run --full-refresh . || true
$BRUIN query --connection duckdb-skill-test --query "DESCRIBE bronze_orders;"
```

Expected proof:

* `bruin run` fails while executing `silver_orders`.
* The error says the `amount` column cannot be found and suggests `gross_amount`.
* Rendered SQL shows `assets/bronze_orders.sql` now emits `gross_amount` while `assets/silver_orders.sql` still selects `amount AS amount`.
* `DESCRIBE bronze_orders` shows `gross_amount`.

Ask the agent to use `schema-drift-check`, identify that `bronze_orders.amount` became `gross_amount`, confirm `silver_orders` still references `amount`, and update only the affected mapping if fixes are allowed. Verify with render or the smallest failing command, then rerun:

```bash
$BRUIN render assets/silver_orders.sql
$BRUIN run --full-refresh --backfill-id schema-drift-fix --backfill-total 1 .
$BRUIN query --connection duckdb-skill-test --query "DESCRIBE silver_orders;"
$BRUIN run --only checks .
```

The run should rebuild all three tables, `DESCRIBE silver_orders` should show the downstream `amount` column again, and the check-only run should pass.

### Mock Pipeline Diagnose and Maintenance Action

Create a broken table reference after removing the duplicate from the base fixture:

```bash
perl -0pi -e 's/\nUNION ALL\nSELECT \* FROM typed_orders WHERE order_id = 1002;//' assets/silver_orders.sql
perl -0pi -e 's/FROM silver_orders/FROM missing_table/' assets/gold_order_report.sql
$BRUIN run . || true
$BRUIN render assets/gold_order_report.sql
```

Expected proof:

* `bruin run` fails while executing `gold_order_report`.
* The error says `missing_table` does not exist.
* `bruin render assets/gold_order_report.sql` shows `FROM missing_table`.

Ask the agent to use `pipeline-diagnose`, classify the failure, inspect the failing asset, and isolate the broken table reference. Then explicitly approve `maintenance-action` to fix only that reference. Verify the specific command, then rerun:

```bash
$BRUIN render assets/gold_order_report.sql
$BRUIN validate .
$BRUIN run --full-refresh --backfill-id pipeline-diagnose-fix --backfill-total 1 .
$BRUIN run --only checks .
```

The rendered SQL should read from `silver_orders`, validation should pass, and both reruns should pass.

## Publish the Fix

In a real repository, have the agent show its diff and publish the fix after the focused verification passes:

```bash
git diff
git status --short
git add assets/<fixed-asset>.sql
git commit -m "Fix <scenario> in ai skill test pipeline"

# Only when the repository has a remote:
git push -u origin <branch-name>
```

The proof to save in the agent's final report is the failing command output, the filtered investigation query, the fixed diff, the `bruin run --backfill-id ...` output, the `bruin run --only checks .` output, and the commit or push reference.

## AI Template Conflicts

If an AI template target already exists, Bruin asks what to do:

* `add`: append or update the marked Bruin AI section in `AGENTS.md`; for skill files, keep existing files.
* `overwrite`: replace the existing target file.
* `skip`: leave the existing target unchanged.

In non-interactive shells, existing targets fail safely and Bruin prints the path that needs a choice.

## Optional AI Connections

Some AI starter skills can use Bruin Cloud or GitHub context. If `bruin init ai-skill-self-heal` does not find matching connections in `.bruin.yml`, Bruin asks whether to add placeholder connections:

```yaml
default_environment: default
environments:
  default:
    connections:
      bruin:
        - name: bruin-cloud
          api_token: "${BRUIN_CLOUD_API_TOKEN}"
      github:
        - name: github
          access_token: "${GITHUB_TOKEN}"
          owner: "<github-owner>"
          repo: "<github-repo>"
```

Bruin infers `owner` and `repo` from the `origin` remote when possible. Existing connections are never duplicated.
