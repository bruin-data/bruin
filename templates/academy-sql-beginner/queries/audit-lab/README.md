# The audit lab

Ten queries, `q01.sql` through `q10.sql`. Every one runs without error and
returns a tidy-looking answer. **Exactly six of them return the wrong answer.**
The other four are correct - and that matters just as much. Someone who flags
all ten has not learned to audit, only to distrust.

Each file starts with the business question it claims to answer. Your job for
each one:

1. Read the question and the query.
2. Predict what a correct answer would look like before you run it.
3. Run it:
   `bruin query --connection duckdb-default --query "$(cat queries/audit-lab/q01.sql)"`
4. Decide: does this query actually answer the question that was asked? If not,
   what is wrong, and roughly how far off is the answer?

Write your verdict for each query in `findings-template.md`. Do not change the
query files - audit them where they sit.

Some things worth checking every time:

- What is the grain of each table, and does a join change it?
- Is the measure being summed at the same grain as the join?
- Could a filter be silently dropping NULLs?
- Are date and timestamp comparisons catching the whole range you meant?
- Does an INNER JOIN drop rows that should have been kept?
- Is there a duplicate row inflating a total?

The numbers you trust in `../anchors.md` are your friend here. So is
`../../docs/known-defects.md`, which lists what is deliberately wrong with the
data - though the bugs in these queries are a separate matter from the defects
in the data.
