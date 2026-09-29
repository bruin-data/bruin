# Lesson 15: recap-and-next-steps

## Objectives
- State the six durable practices from the course.
- Explain why enforceable systems reduce the need for trust.
- Choose a safe next step for operating the pipeline.

## Concepts to teach
You classified operations by what happens if they are wrong, turned an audit checklist into checks, wrote tests before accepting implementations, separated business and ingestion time, ran in dev first, and put enforceable controls where they belong. Those practices make an agent's work reviewable.

Keep the workflow: ask the agent to state the plan and show SQL, run in dev, inspect evidence, and review the diff before any shared write. Use local cron, server cron, GitHub Actions, or Bruin Cloud according to the operational need; Cloud is one peer option, not a requirement.

## Quiz
1. Q: What is the academy's thesis in one sentence?
   A: Build a system where trust in the agent is not required for safe operation.
2. Q: Which timestamp finds newly learned facts?
   A: `_loaded_at`, the ingestion timestamp.
3. Q: What should happen before a production write?
   A: Validate, show the SQL and command, run in dev when possible, and review the evidence and diff.

## Task
Write `docs/recap.md` with six bullets: classify operation risk, automate the audit, test first, separate timestamps, use dev, and enforce controls at the right layer. Add one paragraph describing the next operational step you will take.

## Rubric (for `review my work`)
- [ ] Contains all 6 required practice bullets.
- [ ] States that trust is reduced by enforceable evidence, not by a more confident agent.
- [ ] Names one concrete next step and one safety check before taking it.

## Done signal
You completed Run the Pipeline and can operate a governed SQL workflow with an agent. Carry forward: keep the evidence, tests, and boundaries in the repository as the work evolves.
