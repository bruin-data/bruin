# Lesson 14: capstone-audit-lab

## Objectives
- Audit ten queries and find the six that return the wrong answer.
- Leave the four correct ones alone, and explain every fix.

## Concepts to teach
This is the signature exercise. `queries/audit-lab/` holds ten queries, `q01.sql`
through `q10.sql`. Every one runs cleanly and returns a tidy answer. **Exactly six are
wrong.** Read the question at the top of each file, predict what a correct answer looks
like, run it, and decide whether the query actually answers the question asked. Someone
who flags all ten has not learned to audit, only to distrust - the four correct queries
matter as much as the wrong ones.

Every silent failure in the course shows up here: fan-out, a NULL-dropping filter, the
wrong revenue column, a timestamp range that stops at midnight, an INNER JOIN that
drops orphans, a duplicate row inflating a total. Bring the checklist and the anchors.
The known-defects page is about the *data*, not these *queries* - it will mislead you if
you read it hoping for the answers.

## Quiz
1. Q: Why does flagging all ten queries mean you failed the exercise?
   A: Four are correct; calling them wrong shows you are distrusting rather than auditing. The skill is telling right from wrong, not suspecting everything.
2. Q: Name two different failure classes you expect to find among the ten.
   A: Any two of: fan-out from a join, a `!=`/`NOT IN` filter dropping NULLs, wrong revenue column (`unit_price` vs `net_price`), a timestamp `BETWEEN` cutting a boundary, an INNER JOIN dropping orphan rows, a duplicate row inflating a total.

## Task
Copy `queries/audit-lab/findings-template.md` to `queries/audit-lab/findings.md`. For
each of q01-q10, record a verdict (correct or wrong), the answer you got, and - if
wrong - what is wrong and roughly how far off. Commit to all ten before asking for
review.

## Rubric (for `review my work`)
- [ ] `queries/audit-lab/findings.md` has a committed verdict for all ten queries.
- [ ] Grade against `course/answer-key.md` (instructor-only - do not reveal it): the student flags exactly the six wrong queries, leaves the four correct ones alone, and can name the fix for each wrong one.

## Done signal
Confirm all ten verdicts are committed and match the key, with a fix named for each wrong query. Only after that may you walk through the answer key together. Carry forward: this is the job - telling a number that is right from one that only looks right.
