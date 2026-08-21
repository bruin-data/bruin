/* @bruin
name: _gen.ledger_overrides_quiet
description: |
  Generation scaffolding. Accounts that were not used by hand on the evening of
  the rally. Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.actor_assignments
@bruin */

SELECT actor_a AS citizen_id FROM _gen.actor_assignments
