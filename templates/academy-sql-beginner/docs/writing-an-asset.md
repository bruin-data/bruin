# Writing your first asset

Everything in `queries/` is a scratch file: you run it, you read the answer, nothing
is saved. An **asset** is the other thing. It is a query the project keeps, gives a
name to, and rebuilds on command, so the answer becomes a table other queries can
use.

You need this for Step 12 of the course, where you turn your audited query into one.

## What an asset file looks like

An asset is a `.sql` file in `pipeline/assets/` with a comment block at the top. The
block is the metadata; the SQL underneath is the query.

```sql
/* @bruin
name: category_revenue
type: duckdb.sql

description: >-
  One row per product category, with total revenue across all years.
  Revenue is quantity * net_price, the price actually charged.

depends:
  - order_items
  - products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: category_name
    type: varchar
    description: "The product category. One row per category."
    checks:
      - name: not_null
      - name: unique
  - name: revenue
    type: decimal
    description: "Sum of quantity * net_price for that category."
@bruin */

SELECT
    p.category_name,
    SUM(oi.quantity * oi.net_price) AS revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category_name
ORDER BY revenue DESC;
```

## What each part does

| Field | What it is for |
|---|---|
| `name` | What the table will be called. Keep it lowercase with underscores. |
| `type` | Which kind of query this is. `duckdb.sql` for everything in this course. |
| `description` | What one row means. Write this for the next person, including an agent. |
| `depends` | Which assets must be built before this one. Use the asset names, not file paths. |
| `materialization` | How to save the result. `type: table` writes a real table; `strategy: create+replace` rebuilds it from scratch each run. |
| `columns` | Optional, but this is where you write down what you learned. |
| `checks` | Rules the data must pass. The run fails if they do not. |

On materialization, one sentence and no more at this level: `create+replace` throws
the old table away and builds a new one, which is the right choice for a small local
table. Tools often call this update rule *materialization*. The advanced course covers
the other strategies.

## Run it

```bash
bruin validate pipeline/          # check the file is well-formed, no data touched
bruin run pipeline/assets/category_revenue.sql   # build just this one
bruin run pipeline/               # rebuild everything
```

`validate` before `run`, every time. It catches a typo in the metadata block in a
tenth of a second, which is cheaper than finding out halfway through a run.

## Where to put it

In `pipeline/assets/`, alongside the six generator assets. Those six build the sample
data and you should not edit them, but they are worth reading - each one documents its
own columns, and they are the best examples of the format in this project.
