# Fivetran to Bruin migration plan

This is a reusable, reviewed plan template. During a runtime migration, replace
only the placeholder values after reviewing the redacted files in
`.artifacts/fivetran/<capture-id>/`. Do not put credentials, raw Fivetran
exports, source data, or Fivetran cursor state in this file.

## Status and provenance

- Migration name: `TODO`
- Fivetran connection name / ID: `TODO`
- Capture location: `.artifacts/fivetran/TODO/`
- Capture importer/version/API version: `TODO`
- Capture time and operator: `TODO`
- Current phase: `imported | waiting_for_connections | drafting | waiting_for_run_approval | validating | MVP_complete`

## Source and target inventory

| Fivetran source object | Fivetran destination name | Sync mode / state | Source connection | Proposed Bruin asset | Isolated target | Dependencies |
| --- | --- | --- | --- | --- | --- | --- |
| `TODO.schema.table` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

## Automated findings from import

- `TODO`: record source service, selected objects, schedule/status, destination
  renames, Fivetran schema policy, configured column overrides, and missing
  metadata from the redacted `connection.json` and `schemas.json` capture.
- `TODO`: list any source-to-Bruin compatibility mismatch. Mark it as automated
  until a reviewer makes a decision.

## Hand-authored decisions

- Source and destination named Bruin connections: `TODO`.
- Target isolation and naming: `TODO`.
- Per-table strategy, primary key, incremental key, explicit bounds, and schema
  contract: `TODO`.
- Delete/history/CDC/system-column behavior: `TODO`.
- Scheduling, monitoring, alerting, ownership, and rollback: `TODO`.

## Column mappings

| Source column | Target column | Type/cast | Action | Rationale / reviewer |
| --- | --- | --- | --- | --- |
| `TODO` | `TODO` | `TODO` | `keep | rename | cast | default | omit | generated` | `TODO` |

## Unsupported features and non-mappings

- Fivetran credentials, networking, cursor/checkpoint state, alerts, retries,
  and managed schedules are not imported.
- `SOFT_DELETE`, `HISTORY`, Query-Based PostgreSQL, and Fivetran system columns
  require a separate approved design. `TODO`: record the selected treatment.
- `TODO`: list service-specific source features the generated ingestr asset
  cannot express.

## Open human-review items — must be empty before a write

- [ ] Exact initial-run scope: full history, bounded history, one table, or
  another scope; list tables and explicit start/end bounds.
- [ ] Isolated destination target is confirmed; replace/truncate permission is
  explicit when relevant.
- [ ] Source/destination connection preflight passed.
- [ ] Primary keys, incremental keys, materialization, delete handling, and
  schema ownership are approved for each selected table.
- [ ] Column mapping, quality checks, validation boundary, expected cost, and
  rollback condition are approved.
- [ ] Schedule/monitoring ownership is assigned; Fivetran remains active until
  cutover criteria are met.

## v0 run and validation evidence

- Requested scope and approval: `TODO`.
- `bruin validate` result: `TODO`.
- Run command, dates, and resulting target tables: `TODO`.
- Source/destination profile: row count `TODO`, null keys `TODO`, duplicate
  keys `TODO`.
- `bruin data-diff --full --tolerance 0 --fail-if-diff` result: `TODO`.
- Mismatches, accepted differences, and follow-up: `TODO`.

## Run history

Add one dated entry for every connection preflight, validation attempt, and v0
run. Keep detailed output only in `.artifacts/`; this plan records the
reviewable summary and the next human decision.

### `YYYY-MM-DDTHH:MM:SSZ` — `preflight | validation | v0_run`

- Approved scope and source consistency boundary: `TODO`.
- Source/target connections and isolated tables: `TODO`.
- Commands and evidence locations under `.artifacts/`: `TODO`.
- Result: `passed | failed | paused`; profile/data-diff outcome: `TODO`.
- Mismatches, rollback decision, owner, and next action: `TODO`.

## Completing the migration

Complete this section only after the user requests final review and metadata
updates.

- [ ] Review pipeline/column metadata and `bruin ai enhance --codex` changes.
- [ ] Publish a pipeline README and assign an operating owner.
- [ ] Run final bounded reconciliation and document a rollback decision.
- [ ] Refactor downstream assets/models/tables from Fivetran-specific names and
  system-column assumptions.
- [ ] Enable approved Bruin scheduling, monitoring, alerting, and runbook.
- [ ] Pause/disable Fivetran only after the documented validation boundary and
  rollback window are satisfied.
- [ ] Retire Fivetran credentials, billing/configuration, and legacy artifacts
  under the owner's change-control process.
