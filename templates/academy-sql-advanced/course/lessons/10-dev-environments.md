# Lesson 10: dev-environments

## Objectives
- Explain `default_environment`, `--environment`, and `schema_prefix`.
- Run the pipeline in the prefixed dev database.
- Describe how permissions and `data-diff` complete the boundary.

## Concepts to teach
The `local` environment writes `academy.duckdb`; `dev` writes `academy-dev.duckdb` with a `dev_` schema prefix. The code stays the same while the destination changes. This makes experimentation reversible and keeps agent work away from shared output.

The database should enforce read-only production credentials and allow-listed tables. A markdown rule is advisory. `bruin data-diff` compares dev output with the default environment, but comparison is not permission enforcement.

## Quiz
1. Q: What does `schema_prefix: dev_` change?
   A: It prefixes the schemas or destinations written by that environment.
2. Q: How do you select the dev environment?
   A: Pass `--environment dev` to the Bruin command.
3. Q: Where should the strongest write boundary live?
   A: In database permissions, backed by environment separation and tool controls.

## Task
Run the full pipeline with `bruin run --environment dev pipeline` and inspect the dev database.
The command exits non-zero because the intentional `mart.churn_risk` check still fails; that is
expected when every other asset succeeds. Record the destination names and a read-only
`bruin data-diff` comparison plan in `docs/dev-run.md`. Do not write to the default environment
during this task.

## Rubric (for `review my work`)
- [ ] The dev run completes every non-broken asset in the prefixed dev destination, and the documented non-zero exit is attributed only to `mart.churn_risk`.
- [ ] Records the dev database path and demonstrates that default output was not modified by the dev run.
- [ ] Describes a read-only `bruin data-diff` comparison and why any expected differences must be explained.

## Done signal
You can give an agent a safe place to experiment. Carry forward: the next lesson separates advisory agent rules from enforceable controls.
