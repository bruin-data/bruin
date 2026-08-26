# Audit: <the question you asked>

Copy this file to `queries/audit_v1.md` and fill it in for the query the agent
wrote in Step 8. Work down the list in order - the checks are arranged so that the
cheap ones that invalidate everything else come first.

**Query under audit:** `queries/agent_v1.sql`
**Headline number it returned:**

| # | Check | What you found |
|---|---|---|
| 1 | **Grain.** What does one row of the result represent? Does it match what you asked for? | |
| 2 | **Joins.** For each join: one-to-one or one-to-many? If one-to-many, is anything summed after it that should not be? | |
| 3 | **Filters.** Which rows were excluded? Did you agree to those exclusions? Is there a `!=` or `NOT IN` that will drop NULLs? | |
| 4 | **Columns.** Is every column the one you meant? Check the revenue column and the date column specifically - there is more than one plausible candidate for each. | |
| 5 | **Dates.** Is the range half-open (`>= start AND < end`), or does `BETWEEN` cut a boundary short? Is it the right date column? | |
| 6 | **NULLs.** Any aggregate over a column with NULLs? Any `AVG` whose denominator you have not checked? | |
| 7 | **Second method.** Compute the headline number a different way. Does it agree? | |

## Verdict

- [ ] I trust this number
- [ ] I trust it with the caveats below
- [ ] I do not trust it

**Caveats or corrections:**

**Second-method number:**

**If the two disagree, which is right and why:**

**Anchors this reconciles against** (see `anchors.md`):
