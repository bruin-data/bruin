# Instructor key - lesson 14

Do not read this file to a student before `docs/capstone-evidence.md` is committed and submitted
for review. Grade the capstone by checking the artifact and the evidence, not by comparing prose.

- Both marts use an incremental strategy with the correct business-time key and are idempotent.
- `_loaded_at` selects changed source rows; `ordered_at` defines the replaced business partitions.
- The late-arrival check proves five rows are more than 90 days late and identifies the changed months.
- Every dependency and the target DAG are correct; relationship checks have corresponding run edges.
- Grain is documented and enforced; source and mart totals reconcile with a blocking check.
- At least four unit tests pass, including a no-order customer and a no-data week.
- Dev runs clean in the `dev_` prefixed database; mart full refresh is restricted.
- The student's injected source defect causes a named blocking check to fail.
- `AGENTS.md` contains exactly five guardrails; enforcement is outside the markdown for the two
  controls identified in the lesson.
