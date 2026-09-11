# Lesson 12: investigate-a-failure

## Objectives
- Investigate the broken `churn_risk` asset without changing four things at once.
- Use lineage, render, an upstream query, and run history in order.
- Fix the root cause and prove the fix with a named check.

## Concepts to teach
The expected symptom is a blocking churn-share check failure. Start with the error, then use `bruin lineage`, `bruin render`, a direct upstream query, and the logs. The failure chain has one root cause: `stg_orders` excludes NULL statuses, which removes recent orders, moves the maximum observed order date backward, and inflates the churned bucket.

State one hypothesis before each check. Do not weaken or remove the check. A diagnosis is complete only when one query confirms the upstream condition and the repaired run passes that same check.

## Quiz
1. Q: What should you read first in a failed run?
   A: The asset, check, and exact error message.
2. Q: What is the single root cause of this shipped failure?
   A: The staging filter excludes NULL-status recent orders and shifts the churn reference date backward.
3. Q: What proves a proposed fix worked?
   A: The named blocking check passes after validation and a rerun, with the same diagnosis query no longer showing the defect.

## Task
Investigate `mart.churn_risk` using the six-step sequence and record each hypothesis, command, result, and decision in `docs/failure-investigation.md`. Do not change a file until the diagnosis is recorded. Then make the smallest fix, rerun validation and the named check, and record the result.

## Rubric (for `review my work`)
- [ ] Names `churn_risk`, the churn-share check, and the exact surfaced failure.
- [ ] Names the filter on NULL `order_status` as the root cause and traces all three hops to the symptom.
- [ ] Includes the single confirming upstream query and shows the repaired named check passing.

## Done signal
You can turn a runtime failure into one tested diagnosis. Carry forward: logs and run history make this investigation observable for an agent.
