# Lesson 05: unit-test-the-logic

## Objectives
- Explain the difference between a quality check and a unit test.
- Read Bruin mocked inputs and exact expected rows.
- Add edge-case tests without weakening them.

## Concepts to teach
A quality check asks whether the built table is acceptable. A unit test supplies controlled input rows and asks whether the query transforms them correctly. `inputs`, `expected.rows`, `match: exact`, and expected counts make the contract executable.

The useful cases are a customer with no orders, a NULL order status, tied ranking values, a week
with no data, and a negative refund. The two shipped tests demonstrate the pattern; the student
adds all five edge cases.

## Quiz
1. Q: What does `match: exact` require?
   A: The produced rows must equal the expected rows, including values and row count.
2. Q: Which join preserves a customer with no orders?
   A: A left join from customers to aggregated orders.
3. Q: Should a failing test be weakened to make the run pass?
   A: No. Decide whether the test or implementation is wrong, then fix the wrong side.

## Task
Add at least five unit tests to the appropriate asset headers. Cover a no-order customer, a
NULL-status order, a tie with deterministic ordering, a week with no data, and a negative refund.
For each, define the smallest controlled `inputs` and exact expected output or count that proves
the intended behavior. Run `bruin unit-test pipeline` from the project root and record its results
in `docs/unit-tests.md`.

## Rubric (for `review my work`)
- [ ] Adds at least 5 new tests, so the project has at least 7 total.
- [ ] Includes exact mocked inputs and expected outputs for no-order and no-data-week cases.
- [ ] Includes a negative-refund case and explains whether the model preserves or excludes it.
- [ ] Records a passing `bruin unit-test pipeline` run.
- [ ] All shipped tests pass without changing an expected result to hide a failure.

## Done signal
You can make the implementation answer to a test you control. Carry forward: the next lesson turns those assertions into deliberate failures.
