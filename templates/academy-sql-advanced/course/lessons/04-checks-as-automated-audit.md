# Lesson 04: checks-as-automated-audit

## Objectives
- Map audit questions to built-in and custom checks.
- Add the smallest checks that protect weekly revenue.
- Verify a check with `bruin run --only checks`.

## Concepts to teach
A check is an audit question recorded in the asset. `not_null`, `unique`, `relationships`, `accepted_values`, `positive`, and `non_negative` cover common column assumptions. A custom check returns a count or value, so grain and reconciliation can be expressed as blocking assertions.

The built-in checks include `not_null`, `unique`, `relationships`, `accepted_values`, `positive`,
`non_negative`, `min`, `max`, `freshness`, and `row_count`. The shipped mart has easy column checks
but lacks a grain check and source reconciliation. Add checks for assumptions that can break the
metric; more generic checks only add noise. A check should name the failure it catches.

## Quiz
1. Q: Which check tests a claimed one-row-per-key grain?
   A: A custom query grouping by the key and returning duplicate groups, with expected count zero.
2. Q: What does a blocking check do?
   A: It makes a failed assertion fail the run rather than report a warning only.
3. Q: Why is reconciliation valuable?
   A: It compares the output to an independent source total and catches silent loss or duplication.

## Task
Add a grain custom check and a source-reconciliation custom check to `pipeline/assets/mart/weekly_category_revenue.sql`. Run `bruin validate pipeline` and `bruin run --only checks pipeline`, recording the check names and results in `docs/checks.md`.

## Rubric (for `review my work`)
- [ ] The grain check groups by `iso_week, category_name` and expects duplicate-group count `0`.
- [ ] The reconciliation check compares weekly mart revenue to filtered `fct_order_lines` revenue and expects difference `0`.
- [ ] `bruin validate` passes and the two new checks run as blocking checks.

## Done signal
You can turn an audit question into an executable assertion. Carry forward: checks test arrived data, while unit tests test the SQL logic itself.
