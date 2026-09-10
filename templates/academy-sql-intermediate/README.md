# academy-sql-intermediate

The starter project for **Design the Model**, the intermediate course in the
**SQL in the Age of AI** series on
[getbruin.com/learn](https://getbruin.com/learn). It gives you a retail dataset that
is deliberately under-documented and deliberately imperfect, and a fifteen-lesson
course your own coding agent teaches from the files in `course/`.

## What this is

A Bruin pipeline that generates its own sample data. There are no data files to
download: seven SQL assets build the tables when you run the pipeline, and they
produce the exact same rows on every machine, every time. That is deliberate - every
number in the course is reproducible and hand-checkable.

The data models an online retailer: orders, the lines on each order, the products
sold, the customers who bought them, the stores, a calendar, and daily exchange
rates.

Two things about this project are unusual, and both are on purpose:

- **Almost nothing is documented.** No column has a description. `docs/schema.md`
  records the grain of each table and stops there. The glossary is a stub with a TODO
  in it. Writing that documentation is coursework, not a chore someone forgot.
- **The staging, core and mart folders are empty.** They are named so you know where
  your work goes before you write a line of it. Lesson 8 fills them.

## Project structure

```text
academy-sql-intermediate/
├─ README.md               # This file: what the project is and how to use it.
├─ AGENTS.md               # Turns a coding agent into your instructor for the course.
├─ .bruin.yml              # Connections and environments. (On init it moves to your repo root.)
├─ .gitignore              # Keeps the generated database and logs out of git.
├─ course/                 # The 15-lesson course the agent teaches from.
│  ├─ README.md            # Syllabus and the setup prompt to paste once.
│  ├─ progress.md          # Your checklist; the agent ticks it off as you go.
│  ├─ answer-key.md        # Capstone key. Instructor-only - do not open it early.
│  └─ lessons/             # One file per lesson, 01-the-question-is-the-hard-part … 15-recap-and-next-steps.
├─ docs/                   # Thin on purpose. You are going to thicken it.
│  ├─ schema.md            # Table names, row counts, and the grain of each. No column meanings.
│  ├─ glossary.md          # A stub with three terms and a TODO. Lesson 12 finishes it.
│  ├─ contracts/
│  │  └─ TEMPLATE.md       # The model-contract template. Lesson 2 fills one in for real.
│  └─ eval/
│     ├─ questions.md      # Eight questions with one defensible answer each.
│     └─ answers.md        # The verified answers, for scoring yourself in lesson 13.
├─ queries/                # Practice SQL. Plain files you run, not pipeline assets.
│  ├─ profiling/           # Six reusable profiling queries - your toolkit after lesson 3.
│  │  ├─ 01-row-counts.sql
│  │  ├─ 02-key-uniqueness.sql
│  │  ├─ 03-null-rates.sql
│  │  ├─ 04-orphan-keys.sql
│  │  ├─ 05-date-coverage.sql
│  │  └─ 06-categorical-values.sql
│  └─ reading-drill/       # Three queries to annotate against the clock (lesson 7).
│     ├─ README.md
│     ├─ drill-1.sql
│     ├─ drill-2.sql
│     └─ drill-3.sql
└─ pipeline/               # The Bruin pipeline that builds the dataset.
   ├─ pipeline.yml         # Pipeline name, schedule, default connection, and shared defaults.
   └─ assets/
      ├─ generate/         # Seven generator assets. Do not edit these - but do read them.
      │  ├─ dates.sql
      │  ├─ stores.sql
      │  ├─ products.sql
      │  ├─ customers.sql
      │  ├─ orders.sql
      │  ├─ order_items.sql
      │  └─ fx_rates.sql
      ├─ staging/          # Empty. Lesson 8: one asset per source table, cleaning only.
      ├─ core/             # Empty. Lesson 8: the entities and facts the business talks about.
      └─ mart/             # Empty. Lesson 8: the shaped answers, one asset per question.
```

## Generate the data

One command, run from the folder where you ran `bruin init` (the path names the
pipeline inside this project):

```bash
bruin run academy-sql-intermediate/pipeline
```

(If `bruin init` told you to "add your connection credentials" - you do not need any.
This project builds a local database file and needs no account and no password.)

That builds all seven tables into a local DuckDB database (`academy.duckdb`) in about
a tenth of a second. Then look around:

```bash
bruin query --connection duckdb-default --description "list the tables" \
  --query "SHOW TABLES"

bruin query --connection duckdb-default --description "count the orders" \
  --query "SELECT COUNT(*) FROM orders"
```

`--description` says why you ran the query. It is the convention in this project, and
the agent working alongside you is asked to use it too.

Prefer a real cloud warehouse? Set `MOTHERDUCK_TOKEN` and add `--environment cloud`
to either command. The generators are plain SQL, so they run unchanged on
MotherDuck. No lesson requires it.

## The tables

| Table | Rows | One row is... |
|---|---|---|
| `dates` | 1,096 | one calendar day, 2023-01-01 to 2025-12-31 |
| `stores` | 6 | one store |
| `products` | 60 | one product |
| `customers` | 510 | source-system rows representing 500 customer IDs, including 10 duplicated IDs |
| `orders` | 1,212 | source-system rows representing 1,200 order IDs; 12 IDs have a changed-status resend |
| `order_items` | 2,895 | source-system rows representing 2,880 distinct order lines, including 15 exact duplicate rows |
| `fx_rates` | 5,480 | one date and currency pair |

[`docs/schema.md`](docs/schema.md) lists the columns. It does not say what they mean.
That is the point - lesson 3 has you find out, and lesson 11 has you write it down.

## What is wrong with this data

Something. Several things. Finding them is lesson 3, and nothing in this repository
tells you what they are before you get there.

Do not fix anything you find before lesson 8. Profile first, decide what a defensible
number is second, clean third. In that order, every time.

## The data is generated and deterministic

Nothing here is random. Every value is plain arithmetic on a row number, so the
tables are identical on every run and every machine, and the exact numbers the course
quotes are numbers you can reproduce. Do not edit the seven files in
`pipeline/assets/generate/`.

## The course

This project is an interactive, agent-led course. `AGENTS.md` in this folder turns a
coding agent into the instructor, and [`course/README.md`](course/README.md) is the
syllabus and explains how the loop works. In short: open this project with a coding
agent, paste the setup prompt from `course/README.md`, then drive with `next lesson`
and `review my work`.

It mirrors the intermediate course at
[getbruin.com/learn](https://getbruin.com/learn), so the two stay in sync. If you
have not written a join or a CTE by hand before, start with the beginner course,
Ask the Data, first.
