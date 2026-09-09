# Lesson 07: join-without-breaking

## Objectives
- Tell an INNER JOIN from a LEFT JOIN and know what each keeps.
- Reproduce the fan-out that inflates an order-header total by about 2.4x.

## Concepts to teach
This is the most important lesson in the course. `queries/03-joins.sql` is the
worksheet. A JOIN lines up rows from two tables on a matching column. `orders` has one
row per order; `order_items` has about 2.4 lines per order; joining them multiplies each
order row by its line count. So summing an order-level column like `order_total` after
that join counts it once per line, not once per order, and the total comes out roughly
2.4 times too big. The fix is to sum `order_total` from `orders` alone, or to aggregate
the lines to one row per order first.

INNER versus LEFT is the other half. An `INNER JOIN` keeps only rows that match on both
sides; a `LEFT JOIN` keeps every left-hand row even when nothing matches. 15 order lines
point at a `product_id` that is not in `products` - an inner join to `products` drops
those lines and their revenue silently; a left join keeps them so you can bucket them.
And `net_price` is the price actually charged; `unit_price` is the catalogue price
before discount. Revenue is `quantity * net_price`.

## Quiz
1. Q: You join `orders` to `order_items` and `SUM(order_total)`. Why is the result wrong?
   A: `order_total` is an order-header value; the join repeats each order once per line, so the sum is inflated by roughly 2.4x. Sum it from `orders` alone.
2. Q: An INNER JOIN to `products` returns fewer lines than the table has. What did it drop?
   A: The 15 order lines whose `product_id` is not in `products` - and their revenue with them. A LEFT JOIN would keep them.

## Task
In `queries/03-joins.sql`, run `SUM(order_total)` two ways: once over `orders` joined to
`order_items`, and once over `orders` alone. Report both numbers and state the roughly
2.4x inflation the join caused.

## Rubric (for `review my work`)
- [ ] Ran both sums; the join version is about 2.4x the `orders`-only version (which is 604,065.00).
- [ ] The student identifies fan-out as the cause, and knows a LEFT JOIN keeps orphan rows an INNER JOIN would silently drop.

## Done signal
Confirm the student can reproduce the fan-out and explain it, not just observe a big number. Carry forward: after any join, ask what one row now represents before you aggregate.
