# Lesson 03: dependencies-and-the-graph

## Objectives
- Read the full retail DAG from source through marts.
- Distinguish a run dependency from a quality-check relationship.
- Verify the graph with `bruin lineage`.

## Concepts to teach
The pipeline flows from eight generated sources through staging, core, and two marts. A `depends`
edge controls execution order. A `relationships` check validates keys, but it does not create a
run edge or guarantee that the referenced asset was refreshed first. In this pipeline,
`fct_order_lines` has a relationship check to raw `products` but no direct `depends: products` edge;
its query reaches that data transitively through `stg_products`. The check and the query path are
therefore related but not interchangeable.

Use `bruin lineage` to inspect upstream and downstream assets. `mode: symbolic` lets a dependency
stand for a selectable graph edge; `--downstream` and `--selector` with tag or path selectors bound
a run to the affected portion. Parallel branches are safe only when they do not depend on one another.

## Quiz
1. Q: Does a relationships check create a run dependency?
   A: No. Declare `depends` separately so execution order is explicit.
2. Q: Why can staging branches run in parallel?
   A: They read independent generated sources and have no dependency on each other.
3. Q: What should verify a graph you inferred from file names?
   A: `bruin lineage` and the parsed asset definitions.

## Task
Write the target DAG as text in `docs/dag.md`, including every generated table, staging asset, core
asset, and mart. Identify the critical path, parallel branches, and the deliberately missing
explicit `depends: products` edge associated with the `fct_order_lines` relationships check. Explain
why the transitive `stg_products` query dependency does not make the quality-check relationship a
run dependency. Verify your graph with `bruin lineage pipeline/assets/mart/weekly_category_revenue.sql --full`.

## Rubric (for `review my work`)
- [ ] Includes all 8 generated tables, 4 staging assets, 3 core assets, and 2 marts.
- [ ] Shows staging → core → mart direction and both mart branches.
- [ ] States that a relationships check alone does not create a run edge, distinguishes the missing direct `products` edge from the transitive `stg_products` query path, and records the CLI lineage result.

## Done signal
You can tell execution order from validation logic. Carry forward: checks are the automated form of the audit checklist.
