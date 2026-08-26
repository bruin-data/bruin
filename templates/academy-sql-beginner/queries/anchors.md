# Anchors

An anchor is a number you have checked and trust. When a later query gives you a
surprising answer, you compare it against an anchor to decide whether the query
is wrong or the surprise is real. Auditing is mostly this: build a few numbers
you are sure of, then measure everything else against them.

Three are filled in to start you off. They are exact - the data is generated the
same way on every machine, so these never drift. Add your own as you go, and
write down the query that produced each one.

| # | What it measures | Value | Query |
|---|---|---|---|
| 1 | Total orders | 1,200 | `SELECT COUNT(*) FROM orders;` |
| 2 | Total order lines | 2,880 | `SELECT COUNT(*) FROM order_items;` |
| 3 | Average lines per order | 2.4 | `SELECT COUNT(*) * 1.0 / COUNT(DISTINCT order_id) FROM order_items;` |
|   |  |  |  |
|   |  |  |  |
|   |  |  |  |

Good candidates to add next: the number of orders in 2024, total revenue from
`orders.order_total`, the count of distinct customers, and the number of
categories. Compute each one carefully, once, and anchor it here.
