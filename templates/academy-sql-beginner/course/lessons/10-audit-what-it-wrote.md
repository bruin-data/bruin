# Lesson 10: audit-what-it-wrote

## Objectives
- Apply the seven-point audit checklist to a query.
- Produce a headline number a genuinely different way.

## Concepts to teach
`queries/audit-template.md` is the seven-point checklist, arranged so the cheap checks
that invalidate everything else come first: grain, joins, filters, columns, dates,
NULLs, and finally a second method. You audit the query you saved in the last lesson
(ask-the-agent), the one in `queries/agent_v1.sql`.

The seventh point is the one people skip and the one that catches the most: compute the
headline number a different way and see if it agrees. A second method only counts if it
does not repeat the first method's assumption. Summing `quantity * net_price` two ways
that both fan out is not a second method - it is the same mistake twice. A real second
method comes at the number from another grain or another table, so a wrong assumption
in the first shows up as a disagreement in the second.

## Quiz
1. Q: Why does the checklist put "grain" first and "second method" last?
   A: Grain is the cheapest check and, if wrong, invalidates everything after it; the second method is the strongest but most work, so you earn it by passing the cheaper checks first.
2. Q: What makes a second method genuine rather than fake?
   A: It reaches the number a different way - another grain, table, or path - so it does not just repeat the first method's assumption. Agreement then means something.

## Task
Copy `queries/audit-template.md` to `queries/audit_v1.md` and fill in all seven points
for your `queries/agent_v1.sql` query. For point seven, actually compute the headline
number a second, independent way and record whether the two agree.

## Rubric (for `review my work`)
- [ ] `queries/audit_v1.md` exists with all seven points filled in for the agent's query.
- [ ] Point seven is a genuine second method (a different grain/table/path), not the same calculation restated, and the student says whether it reconciles.

## Done signal
Confirm every point is answered and the second method is truly independent. Carry forward: "how would I get this a different way?" is the check that survives when everything looks fine.
