# Lesson 09: ask-the-agent

## Objectives
- Turn an English question into a request precise enough to answer.
- Read the SQL an agent writes before you run it.

## Concepts to teach
Now you bring in the agent - the one teaching you. Until now you wrote the SQL. From
here you ask for it, and your job shifts to reading what comes back. The skill is
writing a request that pins down the ambiguity: which revenue column, which date
column, what grain, what to do with rows that do not match. "Total revenue in 2024" has
at least three readings in this data; a good request names one.

The rule that matters: read the SQL before you run it. An agent's query looks
authoritative and can still fan out, filter away NULLs, or sum the wrong column - the
silent failures from the last four lessons, now written by something that sounds sure.
Reading it first is how you stay the human in the loop. Ask the agent to explain its
query in one line - what one row of the result represents - before either of you runs
it.

## Quiz
1. Q: Why is "what was our revenue in 2024?" a risky request to hand an agent as-is?
   A: It does not say which revenue definition (`order_total` vs `quantity * net_price`), which date column, or what to do with unmatched rows - so the agent picks, and its pick may not be yours.
2. Q: What should you do with the agent's query before running it?
   A: Read it, and say what one row of the result represents - catch a wrong grain, filter, or column before it produces a number.

## Task
Pick a real question about this data (for example, revenue by store in 2024). Ask the
agent for the SQL, but before running it, save the query to `queries/agent_v1.sql` and
read it top to bottom. Note in a comment what one row of its result represents.

## Rubric (for `review my work`)
- [ ] `queries/agent_v1.sql` exists and holds the agent's query for a clearly-stated question.
- [ ] The student read the SQL before running it, and can say what one row of the result represents (evidence they read it, not just ran it).

## Done signal
Confirm there is a saved query and the student read it before running. Carry forward: an agent's SQL is a draft to audit, not an answer to trust.
