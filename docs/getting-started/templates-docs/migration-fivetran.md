# Migrate a Fivetran connection to Bruin

The `migration-fivetran` template provides a review-gated workspace for migrating one Fivetran connection to a Bruin `ingestr` pipeline. It captures the Fivetran configuration with read-only API requests, records decisions in a migration plan, and pauses for your approval before connection tests or destination writes.

## Create the migration workspace

Create a new project, then move into it:

```bash
bruin init migration-fivetran my-migration-project
cd my-migration-project
```

The template includes:

- `fivetran-bruin-prompt.md`: staged instructions for your coding agent.
- `plan.md`: the migration inventory, decisions, open questions, and validation evidence.
- `.agents/skills/bruin-fivetran-migrator/`: the agent skill and a read-only Fivetran configuration importer.
- `bruin/`: an empty pipeline where the reviewed `ingestr` assets are created.

The local `.bruin.yml` file and imported captures under `.artifacts/` are Git-ignored. Keep credentials, source data, and Fivetran cursor state out of Git and out of `plan.md`.

## Configure the connections

Add the source and destination connections you will use with [the connections command](/commands/connections). Use an isolated destination schema or target for the first run.

The importer also needs two generic values in the repository-root `.bruin.yml`. Their names must be exactly `fivetran_api_key` and `fivetran_api_secret`. Keep their values local; never paste them into an agent chat, the migration plan, or a committed file.

## Start the migration agent

Open a fresh agent chat from the migration-project root and send:

```text
Read @fivetran-bruin-prompt.md and execute that prompt. The connections are already configured in the root .bruin.yml.
```

The agent asks you to select one Fivetran connection, imports its redacted configuration into `.artifacts/`, and updates `plan.md` with the inventory and any compatibility gaps. It then pauses for connection readiness, drafts the pipeline and assets, and asks for explicit approval before a v0 write.

## Review before running

Before approving a v0 load, review the source and destination mappings, primary and incremental keys, date bounds, delete or history behavior, schema ownership, and validation checks. Approve only the isolated targets and scope you intend to test.

After the run, review the recorded row counts, key checks, incremental ranges, quality checks, and any approved parity comparison. Keep Fivetran active and do not enable a schedule or cut over production until your team has approved the final validation and rollback plan.

For the generated asset format, see [Ingestr assets](/assets/ingestr). For the available template catalog, return to [Templates](/getting-started/templates).
