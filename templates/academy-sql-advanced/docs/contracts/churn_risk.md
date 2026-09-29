# Churn risk contract

- Grain: one row per customer at risk.
- Metric: a customer whose latest usable order is older than `churn_days`, or who has no order.
- Reason: `churned` is a classification, not a probability.
- Required proof: the threshold, customer population, and missing-order population are explicit.
