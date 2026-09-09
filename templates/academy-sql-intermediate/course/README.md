# The course

This is an interactive, agent-led SQL course. Your coding agent is the instructor:
it teaches each lesson, quizzes you, sets a hands-on task, and grades what you
actually wrote. You drive it with short commands.

## How it runs

Open this project with a coding agent and paste the setup prompt below once. After
that, you only need six commands:

- `next lesson` - teach the next lesson and set its task.
- `review my work` - grade the artifact you just produced against the rubric.
- `where am I` - what is done, current, and remaining.
- `repeat` - re-teach the current concept a different way.
- `hint` - one more hint on the current task.
- `skip` - move past the current lesson.

The agent reads `progress.md` to know where you are and ticks it off as you finish
lessons. `AGENTS.md` at the project root is what turns your agent into the
instructor - you do not need to read it, but it is there.

### The setup prompt

```text
You are going to set up and then teach me an interactive SQL course. Do this in order, and show me each command before you run it:

1. Check that Git and Bruin are installed (`git --version`, `bruin version`). If Bruin is missing, install it with `curl -LsSf https://getbruin.com/install/cli | sh`, then check the version again.
2. Run `bruin init academy-sql-intermediate` in this folder.
3. Generate the sample data with `bruin run academy-sql-intermediate/pipeline`, then confirm `orders` has 1,212 rows, `order_items` has 2,895 and `fx_rates` has 5,480.
4. Read `academy-sql-intermediate/AGENTS.md` and `academy-sql-intermediate/course/README.md` so you know how to run the course.
5. Greet me, show me the 15-lesson syllabus, and tell me to say "next lesson" to begin and "review my work" whenever I finish a task.

Do not teach lesson one yet - just get set up and hand me the controls. If any command fails, stop and show me the error instead of trying something else.
```

There is no setup lesson in this course. The prompt above is the setup. If it finishes
without an error and the three row counts match, you are ready for lesson one.

## Syllabus

Fifteen lessons in four sections. Each one has a short concept, a quiz, and a
hands-on task you do by hand before the agent grades it.

### From question to specification

- **01 the-question-is-the-hard-part** - the five decisions a vague request leaves open.
- **02 write-the-model-contract** - six fields that fix the answer before the SQL exists.
- **03 profile-before-you-model** - six questions to ask any table before you build on it.

### Design queries that survive review

- **04 stage-the-work** - one CTE per step, named after what it produces.
- **05 window-functions** - LAG, moving averages, ranking within a partition.
- **06 patterns-agents-get-wrong** - dedup, date spines, half-open ranges, aggregate before you join.
- **07 read-a-query-fast** - annotate grain and find a fan-out in ninety seconds.

### Build the model

- **08 layer-the-project** - staging, core, mart, and the rule for each.
- **09 views-and-tables** - which materialization, and the cost of getting it wrong.
- **10 metric-in-the-asset** - put the metric definition where the agent will read it.

### Governance as agent context

- **11 descriptions-and-tags** - descriptions as the input to an agent's accuracy.
- **12 glossary-and-readme** - what belongs in AGENTS.md, the glossary, and the README.
- **13 measure-your-context** - score the agent before and after, and report the delta.
- **14 capstone-defend-the-answer** - build a churn-risk model and survive the objections.
- **15 recap-and-next-steps** - the habits to keep, and where to go next.

Say `next lesson` to begin.
