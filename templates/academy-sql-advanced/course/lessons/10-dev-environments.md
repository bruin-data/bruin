# Lesson 10: dev-environments

## Objectives
- Explain `default_environment` and `--environment`.
- Run the pipeline in an isolated dev database.
- Describe how permissions and `data-diff` complete the boundary.

## Concepts to teach
The `local` environment writes `academy.duckdb`; `dev` writes the same unqualified table names to
`academy-dev.duckdb`. The code stays the same while the database destination changes. This makes
experimentation reversible and keeps agent work away from shared output; it is database isolation,
not schema-prefixing.

The database should enforce read-only production credentials and allow-listed tables. A markdown
rule is advisory. `bruin data-diff` compares the two named connections in the `dev` environment:
`duckdb-default` points at `academy-dev.duckdb`, while `duckdb-prod` points at `academy.duckdb`.
Comparison is not permission enforcement.

## Quiz
1. Q: What does selecting `dev` change in this project?
   A: It resolves the pipeline's `duckdb-default` connection to the separate `academy-dev.duckdb` file; table names remain unqualified.
2. Q: How do you select the dev environment?
   A: Pass `--environment dev` to the Bruin command.
3. Q: Where should the strongest write boundary live?
   A: In database permissions, backed by environment separation and tool controls.

## Task
Run the full pipeline with `bruin run --environment dev pipeline` and inspect the dev database.
The command exits non-zero because the intentional `mart.churn_risk` check still fails; that is
expected when every other asset succeeds. Then add `config.full_refresh_restricted: true` under
the `dev` environment in `.bruin.yml`, rerun the dev pipeline with
`bruin run --environment dev --full-refresh pipeline`, and record the restriction warning. Use
`bruin data-diff --environment dev duckdb-prod:weekly_category_revenue duckdb-default:weekly_category_revenue`
as the read-only comparison plan. Record the destination names and results in `docs/dev-run.md`.
Do not write to the default environment during this task.

## Rubric (for `review my work`)
- [ ] The dev run completes every non-broken asset in the isolated dev database, and the documented non-zero exit is attributed only to `mart.churn_risk`.
- [ ] Records the dev database path and demonstrates that default output was not modified by the dev run.
- [ ] Adds and verifies `config.full_refresh_restricted: true` in `dev`, including the full-refresh warning.
- [ ] Describes the read-only two-connection `bruin data-diff` comparison and why any expected differences must be explained.

## Done signal
You can give an agent a safe place to experiment. Carry forward: the next lesson separates advisory agent rules from enforceable controls.
