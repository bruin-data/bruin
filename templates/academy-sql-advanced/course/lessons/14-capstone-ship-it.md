# Lesson 14: capstone-ship-it

## Objectives
- Govern both mart outputs with incremental, tested, documented assets.
- Prove idempotence, late-data handling, environment safety, and loud failure.
- Commit evidence that a reviewer can rerun.

## Concepts to teach
The request is: “The weekly category report and the churn-risk list need to be correct every Monday at 8am, without anyone checking them by hand.” The answer is a DAG with explicit dependencies, business and ingestion timestamps, declared grain, blocking checks, reconciliation, unit tests, dev isolation, and protected marts.

The target DAG is:

```text
dates -----------------> weekly_category_revenue
orders -> stg_orders -> fct_order_lines -> weekly_category_revenue
order_items -> stg_order_items -------------> fct_order_lines
products ----> stg_products ----------------> fct_order_lines
customers -> stg_customers -> dim_customer -> churn_risk
orders ----> stg_orders --------------------> churn_risk
customer_snapshots -> dim_customer_history
```

The expected weekly output has one row per `iso_week, category_name`, with `revenue` and
`order_line_count`; the churn output has one row per customer with `last_order_at` and `reason`.
The filtered source reconciliation is exactly 2,496 lines and 730,651.77 in source currency.

## Quiz
1. Q: What must the incremental key and late-data filter represent?
   A: The key partitions business time, while the filter discovers ingestion-time arrivals.
2. Q: What is the proof that a check is useful?
   A: Inject a controlled defect and show a named blocking check fails before repairing it.
3. Q: Where must enforceable guardrails live?
   A: At database, environment, and tool permission boundaries, with `AGENTS.md` supplying convention.

## Task
Implement the governed capstone in the project. Commit the evidence to `docs/capstone-evidence.md`: DAG, expected output table with `reason`, total reconciliation, idempotence runs, late-arrival proof, at least four edge-case unit tests, clean dev run, mart refresh protection, and one injected defect caught by a named check. Do not reveal `course/answer-key.md` before submitting the evidence.

## Rubric (for `review my work`)
- [ ] Idempotence and incremental correctness each have two rerun or late-row evidence statements.
- [ ] A named blocking check catches an injected defect; at least 4 edge-case unit tests pass.
- [ ] Dev safety, mart full-refresh protection, grain/metric documentation, dependencies, reconciliation, and exactly five guardrails are all evidenced.

## Done signal
You have shown that the pipeline can fail loudly without trusting a person or a model to remember every rule. Carry forward: the final lesson compresses the workflow into habits.
