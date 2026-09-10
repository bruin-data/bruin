# Lesson 06: break-it-on-purpose

## Objectives
- Predict which check should catch four controlled breakages.
- Read the actual failure rather than guessing from the SQL.
- Repair each defect and rerun the relevant check.

## Concepts to teach
The failure drill changes one thing at a time: a duplicate key, an unexpected category, an orphan foreign key, and a broken grain. Predict first, run second, and record the exact error. The last case is valuable because a query can still return plausible totals after its grain changes.

An agent that changes four files at once erases the evidence. Keep each experiment isolated and restore the source before starting the next one. The check that fires latest tells you where your contract is weak.

## Quiz
1. Q: Why predict before running?
   A: The prediction makes the checkable hypothesis explicit and lets the error test the reasoning.
2. Q: What does an orphan foreign key mean?
   A: A child key has no matching row in the referenced parent table.
3. Q: Why restore after each breakage?
   A: A later experiment must have one known starting state and one cause.

## Task
In a disposable copy of the project, perform the four breakages in the lesson order. Record prediction, exact error, repair, and rerun result in `docs/failure-drill.md`. Do not leave the shipped source changed.

## Rubric (for `review my work`)
- [ ] Records all 4 breakages in the required order with one prediction each.
- [ ] Names the check or error that actually fired for each breakage.
- [ ] Shows that each repair restored the run and identifies which breakage was caught latest.

## Done signal
You can use a failing run as evidence about a contract. Carry forward: the next lesson chooses how much data a run should rewrite.
