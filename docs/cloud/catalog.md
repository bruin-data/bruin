# Catalog

The **Catalog** dropdown in the top nav covers everything you need to navigate, document, and trace your data: **Pipelines**, **Assets**, **Lineage**, and **Glossary**. **Owners** is a per-owner profile reached by clicking an owner name anywhere in the product.

This page covers Glossary, Owners, and the global Lineage view. For the per-asset and per-pipeline pages, see [Assets](/cloud/assets) and [Pipelines](/cloud/pipelines).

## Glossary

The **Glossary** is the registry of business entities and domains your team agrees on — *Customer*, *Order*, *Subscription*, and so on. It's where you map columns in your warehouse to a shared vocabulary, so dashboards, agents, and reports all use the same definitions.

Open it from **Catalog → Glossary**.

### What lives in the glossary

- **Entities** — named business concepts (Customer, Order). Each entity has **attributes** (id, email, status).
- **Domains** — higher-level groupings (Marketing, Finance) that can contain entities.
- **Asset domains** — a synthetic project group: domains that exist on assets via metadata even without a formal glossary entry.

### How entries get there

Glossary entries are stored as YAML in your Git repo, alongside the rest of your project. You can:

- **Edit in the UI.** Add or update entities, attributes, and domain assignments, then click save. Bruin commits the change back to your repo using the GitHub App (or your project's PAT if no App).
- **Edit in the repo.** Change the YAML directly in your editor and push — Bruin picks it up on the next sync.

### Click-through

Click any entity or domain to see every asset column tagged with it. That makes the glossary a fast way to answer "where is *Customer.email* used?" without grepping a hundred SQL files.

### How agents use it

If an agent has access to your project, it reads the glossary alongside your pipelines and `AGENTS.md` files. Asking *"how many active customers signed up last week?"* makes the agent prefer columns that map to the Customer entity over arbitrary lookalikes.

## Owners

The **Owners** page shows everything one named owner is responsible for. Reach it by clicking an owner name on any asset or pipeline page.

### What's on the page

- Owner name and metadata (email, tags).
- **Owned assets**, grouped by type — SQL, Python, ingestr, dbt, etc.
- **Owned pipelines**.
- **Unique domains** the owner touches.
- **Summary stats** — total assets, total pipelines, total domains.
- A **lineage view** for the owner's assets, so you can see how their work connects to the rest of the warehouse.

### How owners get set

Owners are defined in the asset or pipeline YAML:

```yaml
owner:
  name: Jane
  email: jane@example.com
```

There's no UI to create owners directly — you edit the YAML in your repo. The Owners page just reads what's already there.

## Global lineage

Open **Catalog → Lineage** for the bird's-eye view of every pipeline and every cross-pipeline dependency in your team. The view shows:

- Each pipeline as a node.
- Cross-pipeline dependencies (assets that depend on another pipeline's output via [URI](/cloud/cross-pipeline)) as edges.
- Project boundaries.

Use it to spot orphaned pipelines (nothing depends on them), bottlenecks (everything depends on one upstream), or unexpected reaches across project boundaries.

For lineage scoped to a single asset or pipeline, see the lineage tab on the asset detail page in [Assets](/cloud/assets#lineage), or open a pipeline and look at its lineage panel in [Pipelines](/cloud/pipelines#lineage).

## Related

- [Assets](/cloud/assets) — per-asset detail and the asset catalog itself.
- [Pipelines](/cloud/pipelines) — per-pipeline detail and run controls.
- [Cross-pipeline dependencies](/cloud/cross-pipeline) — how URIs connect assets across pipelines.
- [Configure Agents](/cloud/ai-agents/configure) — agents read the glossary to ground their answers.
- [Glossary (CLI)](/getting-started/glossary) — the YAML schema for entities, attributes, and domains.
- [`bruin lineage`](/commands/lineage) — generate the same lineage graph from your local repo.
