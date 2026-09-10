# Known defects - contributor-only

- `stg_orders` filters NULL statuses; this intentionally causes the `churn_risk` check to fail.
- `fct_order_lines` carries a relationships check without declaring the raw `products` dependency.
- Weekly revenue starts with `create+replace`, no grain check, no reconciliation, and no refresh
  restriction. Lessons 4, 7, and 10 address those gaps.
- `dim_customer_history` is an empty student stub. It is kept runnable with an empty projection so
  the shipped pipeline has exactly one intentional runtime failure.
