# Weekly category revenue contract

- Grain: one row per ISO-week start and product category.
- Metric: sum of `quantity * net_price` for non-cancelled order lines.
- Currency: source currency; no FX conversion is applied.
- Required proof: the output reconciles to the same filtered source lines.
