# academy-sql-beginner

The starter project for the beginner course in the **Agentic Data Analysis**
series on [getbruin.com/learn](https://getbruin.com/learn). It gives you a small,
realistic retail dataset to learn SQL on - and to learn how to *audit* SQL, which
is the real point of the course.

## What this is

A Bruin pipeline that generates its own sample data. There are no data files to
download: six SQL assets build the tables when you run the pipeline, and they
produce the exact same rows on every machine, every time. That is deliberate -
every number in the course is reproducible and hand-checkable.

The data models an online retailer: orders, the lines on each order, the products
sold, the customers who bought them, the stores, and a calendar.

## Project structure

```text
academy-sql-beginner/
├─ README.md               # This file: what the project is and how to use it.
├─ AGENTS.md               # Turns a coding agent into your SQL instructor for the course.
├─ .bruin.yml              # Connections and environments. (On init it moves to your repo root.)
├─ .gitignore              # Keeps the generated database and logs out of git.
├─ docs/                   # Reference to read to get oriented - cheaper than asking the database.
│  ├─ schema.md            # Every table and column, what "grain" means, and how they relate.
│  ├─ writing-an-asset.md  # The asset file format, for when you save a query (Step 12).
│  ├─ data-design.md       # How the data is generated, and why you must not change it casually.
│  └─ known-defects.md     # The four defects planted in the data, and the lesson each one serves.
├─ queries/                # Practice SQL. Plain files you run, not pipeline assets.
│  ├─ 01-first-look.sql    # Step 4: SELECT, WHERE, ORDER BY, LIMIT.
│  ├─ 02-aggregates.sql    # Step 5: COUNT, SUM, AVG, GROUP BY, HAVING, CASE.
│  ├─ 03-joins.sql         # Step 6: INNER vs LEFT JOIN, grain, and the fan-out trap.
│  ├─ 04-cte.sql           # Step 7: naming a step with WITH (a CTE), and execution order.
│  ├─ anchors.md           # Numbers you have verified, to check later answers against (Step 10).
│  ├─ audit-template.md    # The seven-point audit checklist to fill in (Step 9).
│  └─ audit-lab/           # The signature exercise: ten queries, exactly six of them wrong.
│     ├─ README.md            # The task and the rules.
│     ├─ q01.sql … q10.sql    # One business question each. Run it, decide if the answer is right.
│     ├─ findings-template.md # Where you record your verdict for each of the ten.
│     └─ answer-key.md        # Spoilers: which six are wrong and why. Read this last.
└─ pipeline/               # The Bruin pipeline that builds the dataset.
   ├─ pipeline.yml         # Pipeline name, schedule, and default connection.
   └─ assets/              # Six generator assets. Do not edit these - but do read them.
      ├─ dates.sql             # dates: one row per day, 2023-2025 (1,096 rows).
      ├─ stores.sql            # stores: 6 stores.
      ├─ products.sql          # products: 60 products across 8 categories.
      ├─ customers.sql         # customers: 500 customers (plus 10 duplicates = 510 rows).
      ├─ orders.sql            # orders: 1,200 orders.
      └─ order_items.sql       # order_items: 2,880 order lines (2.4 per order).
```

Your own assets go in `pipeline/assets/` too, alongside those six. Step 12 has you
create your first one; [`docs/writing-an-asset.md`](docs/writing-an-asset.md) is the
format.

## Generate the data

Two commands. The first one matters: every command below is run from inside the
project folder, not from the repository root above it.

```bash
cd academy-sql-beginner
bruin run pipeline/
```

(If `bruin init` told you to "add your connection credentials" - you do not need any.
This project builds a local database file and needs no account and no password.)

That builds all six tables into a local DuckDB database (`academy.duckdb`) in about a
tenth of a second. Then look around:

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
MotherDuck.

## The tables

| Table | Rows | One row is... |
|---|---|---|
| `dates` | 1,096 | one calendar day, 2023-01-01 to 2025-12-31 |
| `stores` | 6 | one store |
| `products` | 60 | one product, across 8 categories and 20 subcategories |
| `customers` | 510 | one customer (500 distinct - see the defects below) |
| `orders` | 1,200 | one order |
| `order_items` | 2,880 | one line on an order (2.4 lines per order on average) |

Full column details are in [`docs/schema.md`](docs/schema.md), and every generator
asset in `pipeline/assets/` documents its own columns.

## What is deliberately wrong with the data

The data carries four planted defects, each one there to teach a specific lesson.
They are documented in full in [`docs/known-defects.md`](docs/known-defects.md):

1. 24 orders have a NULL `order_status`.
2. 57 order lines have a NULL `unit_cost`.
3. 15 order lines point at a `product_id` that does not exist.
4. 10 customers are duplicated in `customers`.

Do not "fix" these - finding them is the exercise.

## The audit lab

[`queries/audit-lab/`](queries/audit-lab/) is the signature exercise: ten queries
that all run cleanly, of which **exactly six return the wrong answer**. Read each
one, decide whether you trust it, and record your verdict. The four correct queries
matter as much as the wrong ones.

The numbered files in [`queries/`](queries/) (`01-first-look.sql` through
`04-cte.sql`) are the guided steps that build up to the lab, and
[`queries/audit-template.md`](queries/audit-template.md) is the seven-point
checklist you fill in when you audit a query of your own.

## The data is generated and deterministic

Nothing here is random. Every value is plain arithmetic on a row number, so the
tables are identical on every run and every machine. That is what lets the course
quote exact numbers you can reproduce. If you are curious how - or you want to
change the generator - read [`docs/data-design.md`](docs/data-design.md) first.

## Working at a bigger scale

This dataset is small on purpose, so you can check a total by hand. When you want
to work at scale, in order of least to most setup:

1. **TPC-H via DuckDB.** `INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);` generates
   6 million line-item rows in seconds, nothing to download. Great for practising
   joins and aggregates at size. It is a clean, uniform benchmark, so it has none
   of the messiness of real data.
2. **BigQuery public datasets.** Real and large, and the first 1 TB of query each
   month is free. Needs a Google Cloud project.
3. **Your own warehouse.** The `/learn/ai-data-analyst` module covers connecting
   Bruin to a real database with `bruin import database`.

## The course

Written to be worked through alongside the beginner course at
[getbruin.com/learn](https://getbruin.com/learn). `AGENTS.md` in this folder turns
a coding agent into a SQL instructor for the course - open the project with your
agent and ask it to help you start.
