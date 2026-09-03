# Semantic Layers

A **semantic layer** in Bruin Cloud is a reusable model (a source table plus named metrics and dimensions) that your team's [dashboards](/cloud/dashboards) reference by name instead of hand-writing SQL. Define a metric like `total_revenue` once, and every widget that uses it stays consistent.

Semantic layers live under **Catalog → Semantic Layers** in the top nav. They are shared across the team: every dashboard can reference them, so a change affects everything that uses the model.

## Viewing

The Semantic Layers page lists your team's models with their name, owner, and when each was last updated. Open a model to see its full definition as YAML.

The page is **read-only**. You view models here. They are created and edited by an agent while it builds a dashboard, or with the CLI.

## Access

- **View**: anyone on your team can browse the list and open a model to read its full definition. Semantic layers are shared team state, so they are visible to everyone on the team.
- **Create and edit**: team members whose role grants edit permissions can add new models and change existing ones. Members with view-only access cannot.
- **Delete**: only the model's creator or a team admin can delete it. Because a model is shared, deleting one breaks any dashboard that still references it, so check usage first.

## Related

- [Semantic Layer](/core-concepts/semantic-layer): the model definition format (metrics, dimensions, joins).
- [Dashboards](/cloud/dashboards): where semantic models are referenced.
- [`bruin cloud semantic-layers`](/commands/cloud#semantic-layers): the CLI commands.
