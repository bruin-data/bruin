# Lesson 12: fix-the-context

## Objectives
- Persist a correction in the repo instead of only in the chat.
- Understand why a rule in a file outlives a rule in a conversation.

## Concepts to teach
When you catch the agent making a mistake, a correction in the chat fixes this answer
and nothing after it - the next session starts fresh and repeats the error. A
correction in a file is context the agent reads every time. That is the difference
between teaching a person once and writing down the standard.

There are two good places here. `AGENTS.md` at the project root holds the rules the
agent follows - add a rule capturing the mistake you found, for example "revenue means
`quantity * net_price`, never `unit_price`." Or sharpen a column `description` in the
asset that defines it, so the meaning travels with the data. A column `description` that
says "the price actually charged, after discount - use this for revenue" prevents the
wrong-column error at the source.

## Quiz
1. Q: Why is correcting the agent in a file better than correcting it in the chat?
   A: The chat correction is forgotten next session; a file is read every time, so the fix persists and the mistake does not come back.
2. Q: Name one place in this project where a correction belongs.
   A: `AGENTS.md` (a rule for the agent) or a column `description` in the asset that defines the column (meaning travels with the data).

## Task
Pick a correction you actually needed in an earlier lesson (a wrong revenue column, a
NULL filter, a fan-out) and persist it: add a rule to `AGENTS.md`, or sharpen a column
`description` in a `pipeline/assets/` file. Say which file you changed and what it now
says.

## Rubric (for `review my work`)
- [ ] The correction lives in a file (`AGENTS.md` or an asset `description`), not just in the conversation - confirm by reading the file from disk.
- [ ] The rule is specific and correct: it would actually prevent the mistake it targets.

## Done signal
Confirm the change is on disk and would stop the error recurring. Carry forward: a correction that is not written down is a correction you will make again.
