# Evaluation questions

Eight questions with one defensible answer each. Lesson 13 uses them to measure whether the context
you wrote changed how often your agent gets an answer right.

Every question is scoped precisely on purpose. Where a definition could go two ways, the question
picks one, so that a wrong answer is a wrong answer rather than a different reading. Read the
scoping line before you score anything.

The verified answers are in `answers.md`. Do not open it, and do not let your agent read it, until
you have both scores.

**Shared definitions used below**

- **Line revenue** is `SUM(quantity * net_price)` over rows of `order_items`.
- **In 2023** means the line's order has `ordered_at >= TIMESTAMP '2023-01-01 00:00:00'` and
  `ordered_at < TIMESTAMP '2024-01-01 00:00:00'`.
- **Each order counted once, each line counted once.** Where a table holds the same record more
  than once, the intended figure counts it a single time.

---

**1. What was the total line revenue in 2023?**

Two decimal places.

**2. Which customer country produced the most line revenue in 2023, and how much?**

Fold every spelling of the same country into one. Name the country and give the figure to two
decimal places.

**3. How many orders placed in 2023 finished in a completed state?**

Count orders, not lines. Treat every spelling of the completed state as the completed state.

**4. What was the average order value in 2023?**

Defined as 2023 line revenue divided by the number of distinct orders placed in 2023. Two decimal
places.

**5. Across all three years, how much line revenue sits on order lines whose `product_id` has no
matching row in `products`?**

Two decimal places, and give the number of lines as well.

**6. How many distinct customers placed at least one order in 2023?**

Count only customers that have a row in `customers`.

**7. Which calendar month of 2023 had the highest line revenue, and how much?**

Name the month and give the figure to two decimal places.

**8. What was the total 2023 line revenue converted to USD?**

Convert each order at its own `currency_code` and at the `fx_rates` row for its own `ordered_at`
date. Two decimal places.
