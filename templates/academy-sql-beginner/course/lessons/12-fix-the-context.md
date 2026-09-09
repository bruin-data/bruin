# Lesson 12: fix-the-context

## Objectives
- Persist a correction in the repo instead of only in the chat.
- Understand why a rule in a file outlives a rule in a conversation.

## Concepts to teach
When you catch the agent making a mistake, a correction in the chat fixes this answer
and nothing after it - the next session starts fresh and repeats the error. A
correction in a file is context the agent reads every time. That is the difference
between teaching a person once and writing down the standard.

There are three places, in increasing order of durability. Weakest first: sharpen a
column `description` in the asset that defines it, so the meaning travels with the data
- for example "the price actually charged, after discount - use this for revenue."
Next: add a rule to `AGENTS.md` at the project root, which the agent reads every session
- for example "revenue means `quantity * net_price`, never `unit_price`." Strongest: add
a data-quality check to the asset's `columns[].checks` (like `not_null`, `unique`,
`positive`, or `accepted_values`). A description guides and a rule reminds, but a check
*fails the run loudly* when the data breaks it, so the mistake cannot ship silently.

## Quiz
1. Q: Why is correcting the agent in a file better than correcting it in the chat?
   A: The chat correction is forgotten next session; a file is read every time, so the fix persists and the mistake does not come back.
2. Q: Name the three places a correction can live, weakest to strongest.
   A: A column `description` (meaning travels with the data), an `AGENTS.md` rule (the agent reads it each session), and a data-quality check in `columns[].checks` (fails the run loudly, so the mistake cannot ship).

## Task
Pick a correction you actually needed in an earlier lesson (a wrong revenue column, a
NULL filter, a fan-out) and persist it in one of the three places: sharpen a column
`description`, add a rule to `AGENTS.md`, or - strongest - add a data-quality check to
an asset's `columns[].checks` so the run fails if the mistake recurs. Say which file you
changed and what it now says.

## Rubric (for `review my work`)
- [ ] The correction lives in a file - a column `description`, an `AGENTS.md` rule, or a `columns[].checks` entry - not just in the conversation. Confirm by reading the file from disk.
- [ ] The correction is specific and correct: it would actually prevent the mistake it targets. A quality check is the most durable choice, because it fails loudly rather than only reminding.

## Done signal
Confirm the change is on disk and would stop the error recurring. Carry forward: a correction that is not written down is a correction you will make again.
