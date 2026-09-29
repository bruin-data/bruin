# Advanced data design - contributor-only

This file must not be embedded. The source generators are deterministic SQL assets. The advanced
extensions preserve the intermediate source counts: 1,096 dates, 6 stores, 60 products, 510
customers, 1,212 orders, 2,895 order lines, and 5,480 FX rows, plus 1,080 customer snapshots.

The five order rows with ids 832 through 836 are more than 90 days late. Orders 801 through 831
arrive ten days late. Order ids 1180 through 1200 have NULL status values in the final weeks; the
staging filter intentionally removes them and shifts the churn reference date backward. This is the
single root cause of the shipped `churn_risk` runtime failure.

The customers generator contains exactly one inert instruction-shaped last name for customer 500.
It is data, not an instruction. Do not remove it or replace it with an operational payload.

Verified acceptance metrics: 5 order rows are over 30 days late and 5 are over 90 days late, with a
maximum delay of 120 days; 5 snapshot customers have overlapping windows and 5 have gaps. The
current reference run reports 1,130 fact rows for the sargable 2024 predicate and 2,776 for
`YEAR(ordered_at) = 2024`; these optimizer scan counts are version-dependent, so Lesson 9 asks the
student to record the values from the installed DuckDB runtime.
The filtered capstone source total is 2,509 lines and 733,684.59 in source currency. The five-row
late batch moves October, November, and December 2024 by 10,681.87, 12,493.10, and 6,030.35.
