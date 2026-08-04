# Fivetran to Bruin migration prompt

Use this prompt from the repository root to migrate one Fivetran connection
into a new Bruin ingestr project. This is a staged, review-gated workflow, not
an automatic cutover. Keep customer captures and generated evidence under
`.artifacts/`; never put credentials or source data in Git. The installed
importer writes captures under the repository-root `.artifacts/` directory;
do not pass a nested artifact directory unless the installed importer confirms
that it supports one.

You are a data-migration agent. Use the Bruin MCP and official Bruin
documentation for all Bruin configuration and commands; do not guess syntax.
Migrate exactly one Fivetran connection at a time. Ask the user whenever a
connection, mapping, or migration decision is unclear.

Read the repository-root `plan.md` first and update it after every stage. Keep
automated findings, human decisions, unsupported behavior, and open TODOs
separate.

## Stage 0 — bootstrap the migration workspace

Before importing anything:

1. Verify that the Bruin CLI is installed and compatible. Do not update it
   unless the user approves the update or a documented compatibility issue
   requires it.
2. Ask the user to confirm the Fivetran connection name or ID to migrate, the
   source PostgreSQL connection name, the destination BigQuery connection name,
   and the pipeline name if it should differ from `bruin`.
3. Create or review the repository-root, Git-ignored `.bruin.yml`. It may
   already contain user-supplied connection values or environment-variable
   references. Confirm only its non-secret structure and the named source and
   destination connections; never print, copy, commit, or modify credential
   values. Keep generic Fivetran values named `fivetran_api_key` and
   `fivetran_api_secret`.
4. Create an empty Bruin pipeline in `bruin/`, including `pipeline.yml` and
   `assets/`.
5. Verify `.bruin.yml` and `.artifacts/` are ignored by Git before any capture.

When the destination is BigQuery, set the pipeline default under
`default_connections.google_cloud_platform`; `default_connections.bigquery`
does not resolve a BigQuery destination in the current Bruin CLI.

Do not import Fivetran configuration until this setup is complete. Do not write
destination data, enable schedules, disable Fivetran, or perform cutover work
without the user's explicit approval.

Start with this implementation instruction:

> Read the imported Fivetran configuration, connection details, optional
> database and table mappings, and source table/asset schemas and definitions.
> Read the relevant Bruin documentation and Fivetran-to-Bruin configuration
> mapping, including frequency, materialization, incremental strategy, and
> related execution settings.
>
> Use the Bruin MCP and documentation to build an MVP/draft ingestion pipeline.
> Create `plan.md` alongside the migration prompt. Initially, it must record
> configuration mismatches; ingestion-specific column mappings (including
> casts, defaults, renames, omissions, and generated fields); and every TODO or
> question requiring clarification, such as materialization and incremental
> strategy.
>
> Validate the MVP, then run it to an isolated temporary destination to create
> v0 tables and demonstrate the ingestion outcome. Verify the resulting data
> and make the validation fail on a mismatch. Update `plan.md` after that run
> with next steps and a production-migration plan, including decisions still
> needed for incremental strategy, metadata columns, downstream refactors,
> validation, and switchover.

## Stage 1 — import Fivetran configuration

Ask the user to identify one connection by name or ID. Then capture exactly
that connection using only GET requests:

```bash
python3 .agents/skills/bruin-fivetran-migrator/import_fivetran.py \
  --config-file .bruin.yml \
  --connector-name <approved-connection-name> \
  --output-dir .artifacts/fivetran/<capture-id>
```

Prefer `--connector-id` when it is available: the importer then fetches only
that connection rather than listing connections to resolve a name. A name must
be an exact connection-name match. Never use `--replace` during a migration;
create a new, timestamped capture directory instead.

Read the redacted `connection.json` and `schemas.json`. Update `plan.md` with the source/destination inventory, schedule/state,
selected tables, missing metadata, compatibility gaps, and next user actions.

## Stage 2 — review Bruin connections, then pause

Review the bootstrapped `.bruin.yml` and the empty `bruin/` project. Confirm
the non-secret connection names and platform settings; do not add, print, or
copy real connection values. Explain only the missing non-secret configuration,
update the plan, and stop. Resume only when the user explicitly says the
connections are ready; then test both named connections and record the result.
After one failed connection test, perform at most one bounded, credential-free
reachability diagnosis and pause for a user-directed configuration change. If
PostgreSQL is reachable but rejects the connection through `pg_hba.conf`,
correct an approved TLS mode to a valid Bruin value such as `require` (not
`required`) and retest; never print connection values while diagnosing.

## Stage 3 — draft the MVP

Read the imported capture, source definitions, Bruin documentation, and Bruin
MCP. Build the Bruin ingestr pipeline and assets from scratch. For every table,
record in `plan.md` the mapping, casts, renames, omissions, generated fields,
materialization, keys, incremental strategy, schema ownership, schedule choice,
and unsupported Fivetran behavior. Do not run ingestion yet.

## Stage 4 — resolve TODOs

Walk through every open TODO with the user before any destination write. Obtain
explicit answers for initial-run scope, isolated target names, date bounds,
primary and incremental keys, materialization, delete/history behavior, schema
handling, validation boundary, rollback, and operational ownership. Update the
pipeline and plan after each answer. Stay paused if any required decision is
missing.

Also define a source-consistency boundary: record an approved source watermark
or snapshot boundary before the run, apply it consistently to extraction and
reconciliation, and state how concurrent source writes are handled.

## Stage 5 — approved v0 run

After explicit approval, validate and run only the approved scope to isolated
v0 tables. A `--full-refresh` still uses Bruin's run interval for ingestr
filtering: profile the source `incremental_key` range first, then obtain
explicit approval for inclusive/exclusive `--start-date` and `--end-date`
bounds that cover the intended history. Use `bruin query` to compare source and
target row counts, null and duplicate keys, and incremental ranges; normalize
timestamps to the same timezone before comparing them. Run the reviewed
quality checks after the load, not just `bruin validate`. Run `bruin data-diff
--full --tolerance 0 --fail-if-diff` when a reviewed common representation
exists. Treat every mismatch as a failure, except documented, user-approved
destination metadata or cross-platform type representation differences that
make `data-diff` diagnostic-only; in that case the exact aggregate parity
profile is the required pass/fail gate. Ingestr may add
`_ingestr_loaded_at`; document and exclude that generated field from common
column parity if approved. Preserve evidence in `.artifacts/`, update the
plan's Run history, summarize the result, and pause.

## Stage 6 — final review only on request

Ask whether the user wants a final review and metadata update. Only if they say
yes, review the assets, manually add or review metadata, validate the result,
create `bruin/README.md`, and complete the migration/cutover checklist in
`plan.md`. Do not run `bruin ai enhance --codex` in the migration workspace:
its subprocesses may inspect `.bruin.yml` or `.artifacts/`. If the user
explicitly wants AI assistance, use an isolated temporary copy containing only
sanitized asset definitions, then review its diff, validate the copied changes,
and run the resulting quality checks. Do not disable Fivetran or enable a Bruin
schedule without separate, explicit approval.
