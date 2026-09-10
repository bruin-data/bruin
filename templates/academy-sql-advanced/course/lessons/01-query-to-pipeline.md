# Lesson 01: query-to-pipeline

## Objectives
- Name the four properties a recurring pipeline needs.
- Map each property to a Bruin or repository mechanism.
- Explain why tests make an agent safer to use.

## Concepts to teach
A one-off query can be wrong once; a daily pipeline can repeat the same wrong answer for weeks. A pipeline needs idempotence (a repeat gives the same result), observability (a run leaves evidence), recoverability (a past period can be repaired), and enforceability (assumptions are checked).

Bruin materializations and date ranges support repeatable work. `logs/`, validation output, checks, and unit tests provide evidence. Environments and incremental keys make repair bounded. An agent is a collaborator only when these properties make its mistakes loud and reviewable.

## Quiz
1. Q: What does idempotent mean here?
   A: Re-running the same pipeline range produces the same rows and totals rather than duplicates.
2. Q: Which mechanism makes a bad metric fail loudly?
   A: A blocking quality check or unit test turns a silent wrong number into a run failure.
3. Q: Name the four required properties.
   A: Idempotence, observability, recoverability, and enforceability.

## Task
Write a short answer in the conversation: classify one risk for each of the four properties and name the repository or Bruin feature that addresses it. Then say `review my work`.

## Rubric (for `review my work`)
- [ ] Names idempotence, observability, recoverability, and enforceability.
- [ ] Gives one distinct failure risk for each property.
- [ ] Maps each property to a concrete mechanism such as materialization, logs, date-range runs, checks, or tests.

## Done signal
You can explain why a pipeline is an obligation rather than a saved query. Carry forward: every later lesson turns one of these four properties into a concrete practice.
