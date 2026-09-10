# The course

This is an interactive, agent-led course. Your coding agent is the instructor: it teaches each
lesson, quizzes you, sets a hands-on task, and grades what you actually wrote. You drive it with
six commands:

- `next lesson` - teach the next incomplete lesson and set its task.
- `review my work` - grade the artifact against its rubric.
- `where am I` - report done, skipped, current, and remaining lessons.
- `repeat` - teach the current idea another way.
- `hint` - give one larger hint for the current task.
- `skip` - mark the current lesson skipped and continue.

The project intentionally has one runtime failure. `bruin validate` must pass. The first `bruin run`
must build the upstream assets and fail only on `mart.churn_risk`; that failure is the lesson 12
investigation exercise. If any other asset fails, or the error is different, stop and show the error.
Do not silently continue after an unexpected error.

## The setup prompt

```text
You are going to set up and then teach me an interactive SQL course. Do this in order, and show me each command before you run it:

1. Check that Git and Bruin are installed (`git --version`, `bruin version`). If Bruin is missing, install it with `curl -LsSf https://getbruin.com/install/cli | sh`, then check the version again.
2. From the empty folder where you want the project to live, run `bruin init --in-place academy-sql-advanced`.
3. Run `bruin validate academy-sql-advanced/pipeline`. Then generate the sample data with `bruin run academy-sql-advanced/pipeline`. The shipped project intentionally fails only on `mart.churn_risk`; stop if any other asset fails or if validation fails. After the expected failure, confirm `orders` has 1,212 rows and `order_items` has 2,895 rows.
4. Read `academy-sql-advanced/AGENTS.md` and `academy-sql-advanced/course/README.md` so you know how to run the course.
5. Greet me, show me the 15-lesson syllabus, and tell me to say "next lesson" to begin and "review my work" whenever I finish a task.

After setup, stay in the project root containing `.bruin.yml`; lesson paths such as `pipeline` and
`queries/...` are relative to that directory. Do not teach lesson one yet - just get set up and hand me
the controls. If any command fails unexpectedly, stop and show me the error instead of trying something else.
```

The default environment is local DuckDB and needs no token. MotherDuck is optional: configure
`MOTHERDUCK_TOKEN` and use `--environment cloud`. No lesson requires the cloud path.

## Syllabus

### Build the pipeline

- **01 query-to-pipeline** - what changes when a query becomes a daily obligation (7 min).
- **02 ddl-dml-and-approval** - classify operations by risk and approval (12 min).
- **03 dependencies-and-the-graph** - read the run graph and its dependencies (10 min).

### Make failure loud

- **04 checks-as-automated-audit** - turn an audit checklist into checks (12 min).
- **05 unit-test-the-logic** - test SQL logic with controlled inputs (12 min).
- **06 break-it-on-purpose** - predict, trigger, and repair four failures (10 min).

### Run it repeatedly

- **07 incremental-strategies** - choose strategies and prove idempotence (14 min).
- **08 late-data-and-backfills** - find late rows and backfill their business dates (12 min).
- **09 sargability-and-cost** - compare predicates and label what local DuckDB proves (12 min).
- **10 dev-environments** - experiment safely in a prefixed environment (10 min).

### Operate with an agent

- **11 guardrails** - separate enforceable controls from advice (12 min).
- **12 investigate-a-failure** - diagnose the intentionally broken asset (12 min).
- **13 logs-history-and-the-bill** - use run history and logs for read-only triage (10 min).
- **14 capstone-ship-it** - ship a governed incremental pipeline (30 min).
- **15 recap-and-next-steps** - keep the workflow going (5 min).

Say `next lesson` to begin.
