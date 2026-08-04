---
name: bruin-fivetran-migrator
description: Stage a review-gated migration from one Fivetran connection to a new Bruin ingestr project.
---

# Fivetran to Bruin migrator

Use this skill when a user wants to migrate one Fivetran connection to Bruin.
Read the repository-root `fivetran-bruin-prompt.md` and `plan.md` before
acting. Follow the prompt's stages in order.

Use the sibling `import_fivetran.py` for Fivetran access. It makes GET requests
only and writes redacted captures to `.artifacts/` at the repository root.

Rules:

- Capture exactly one connection selected by the user.
- Prefer an approved connector ID, which fetches that connection directly; a
  connector name must match the Fivetran connection name exactly.
- Never expose or store credentials, source data, endpoints, or Fivetran state.
- Treat `.bruin.yml` and `.artifacts/` as sensitive even when they are
  Git-ignored: do not print them, pass them to AI subprocesses, or copy them
  into generated documentation.
- Create the new `bruin/` project yourself; do not expect generated assets.
- Pause after connection placeholders, before any destination write, and after
  each approved v0 run.
- Keep repository-root `plan.md` current with facts, decisions, TODOs, run
  history, source-consistency boundaries, unsupported behavior, and cutover
  work.
- Before a historical ingestr run, obtain explicit start/end bounds and a
  source-consistency boundary; `--full-refresh` does not by itself make the
  ingestr source interval unbounded.
- Run approved aggregate parity and quality checks after every v0 load. Use
  cross-platform data diff as a pass/fail gate only when its representation is
  explicitly reviewed as comparable.
- Never use the importer `--replace` flag during a migration; preserve a new,
  timestamped capture instead.
- Do not enable schedules, run destructive refreshes, or disable Fivetran
  without explicit user approval.
