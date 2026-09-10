# Lesson 13: measure-your-context

## Objectives
- Score an agent against a fixed question set before and after adding context.
- Report a before score, an after score, and which context change mattered.
- Say why "context is good" is not the conclusion this exercise supports.

## Concepts to teach
This is the lesson that makes the course honest. You have spent three lessons writing descriptions,
tags and a glossary on the argument that they make an agent more accurate. Now measure it, because
the honest answer is that it depends on what you wrote.

Before you add or edit course context, preserve an untouched copy for the before run. If the working
copy has already changed, initialize `academy-sql-intermediate` again in a separate empty directory
outside the existing Git repository (for example, under `/tmp`) and use that pristine copy for the
baseline session. Do not put it under the course repository: Bruin would reuse its repository-root
`.bruin.yml` and relative `academy.duckdb` path. This makes the comparison reproducible rather than
asking the modified course copy to recreate its original state.

The method has four steps. **One**, `docs/eval/questions.md` ships eight questions with one
defensible answer each, and the verified answers are in `docs/eval/answers.md`. Add two of your
own, drawn from a mistake you actually saw an agent make in this project, so the set is ten.
**Two**, in a fresh agent session, use the untouched template and its built-in instructor
instructions, ask all ten questions, and record the answers. **Three**, score them against
`answers.md`: one point each, exact match, no partial credit. **Four**, in another fresh agent
session, use the same base template plus the student-authored descriptions, glossary, `AGENTS.md`,
and any other context additions made during the course; ask the same ten questions and score again.
Report both numbers and roughly how much longer the second set of answers took. Both runs must use
fresh agent sessions. Do not let the agent read `docs/eval/answers.md` before both scores are
recorded; otherwise the measurement is contaminated.

Then the framing that keeps this from being a sales pitch. Published measurements go both ways. A
well-modelled semantic layer moved accuracy up seventeen to twenty-three points on a
hundred-question set built on a retail schema much like this one. A study of agent-context files
found human-written ones worth about four points on average, and machine-generated ones slightly
negative while adding twenty percent to cost. So the conclusion is not "context is good". It is:
**context written for a specific observed failure works, generic context does not, and the only way
to know which you wrote is to measure.**

Two cheap verification moves from the same literature, worth keeping after this lesson. **Run it
three times** - text-to-SQL is not deterministic, and the same question answered three different
ways means you cannot trust any of them. **Ask two models** - disagreement between models is a
strong bug signal, and it costs one extra prompt.

## Quiz
1. Q: Why must the second run be a fresh session rather than a follow-up in the same conversation?
   A: Because the first run's reasoning is still in context. The agent would be scoring against its own earlier working rather than against the repository, and the measurement would be of memory, not of context.
2. Q: Your before score is 6 and your after score is 6. What have you learned?
   A: That the context you wrote did not address the failures this question set exercises. That is a real result, not a failed exercise - the next move is to look at which four questions are still wrong and write context aimed at those specifically.
3. Q: Why does the scoring allow no partial credit?
   A: Because a nearly-right figure almost always means the agent resolved an ambiguity differently, not that it was slightly imprecise. Half a point would hide the thing the exercise exists to show.

## Task
Run the full measurement. Add two questions of your own to `docs/eval/questions.md` with verified
answers, then score two fresh agent sessions against all ten and record the results in
`docs/eval/results.md`: the before score, the after score, a line per question showing right or
wrong in each run, and one sentence naming the single context line that you believe caused the
biggest change.

Do not let the agent read `docs/eval/answers.md` before either run.

## Rubric (for `review my work`)
- [ ] `docs/eval/results.md` exists and reports a before score out of 10 and an after score out of 10.
- [ ] Both runs cover all ten questions, with a per-question right or wrong recorded for each run.
- [ ] The two added questions have answers the student verified with a query, not estimated.
- [ ] One sentence names a specific context line - a named column's description, a named glossary entry, a named `AGENTS.md` rule - not "the descriptions helped".
- [ ] Grade the recorded scores, not the direction of the delta. A lower after score with an honest explanation is a pass.

## Done signal
Confirm two scores exist, that they came from two separate sessions, and that the student named a
specific line rather than a category. Carry forward: context you have not measured is context you
are hoping about.
