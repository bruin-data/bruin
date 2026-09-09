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
2. Run `bruin init academy-sql-beginner` in this folder.
3. Generate the sample data with `bruin run academy-sql-beginner/pipeline`, then confirm `orders` has 1,200 rows and `order_items` has 2,880.
4. Read `academy-sql-beginner/AGENTS.md` and `academy-sql-beginner/course/README.md` so you know how to run the course.
5. Greet me, show me the 15-lesson syllabus, and tell me to say "next lesson" to begin and "review my work" whenever I finish a task.

Do not teach lesson one yet - just get set up and hand me the controls. If any command fails, stop and show me the error instead of trying something else.
```

## Syllabus

Fifteen lessons in four sections. Each one has a short concept, a quiz, and a
hands-on task you do by hand before the agent grades it.

### Set up the workspace

- **01 start-here** - table, row, query, warehouse, agent: the words the rest uses.
- **02 what-to-delegate** - loud failures versus silent ones, and which need a human.
- **03 setup** - a working project with the data built, verified by hand.
- **04 meet-the-warehouse** - read the schema and say the grain of each table.

### Write your own SQL

- **05 ask-one-table** - SELECT, WHERE, ORDER BY, LIMIT, and a filter that lies.
- **06 count-sum-group** - COUNT, SUM, AVG, GROUP BY, HAVING, and how NULL skews them.
- **07 join-without-breaking** - INNER versus LEFT JOIN, grain, and the fan-out trap.
- **08 name-your-steps** - CTEs with WITH, and the order the database really runs a query.

### Bring in the agent

- **09 ask-the-agent** - turn a question into a good request, and read the SQL before you run it.
- **10 audit-what-it-wrote** - the seven-point audit checklist, applied to the agent's query.
- **11 interrogate-the-logic** - anchors, second methods, and reconciling a surprising number.
- **12 fix-the-context** - persist a correction in the repo so the mistake does not come back.
- **13 save-a-query-as-an-asset** - promote an audited query to a Bruin asset and run it.

### Make it stick

- **14 capstone-audit-lab** - ten queries, six wrong: find them and explain each fix.
- **15 recap-and-next-steps** - the three habits to keep, and where to go next.

Say `next lesson` to begin.
