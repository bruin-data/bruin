# Semantic Layers

A **semantic layer** in Bruin Cloud is a reusable model (a source table plus named metrics and dimensions) that your team's [dashboards](/cloud/dashboards) reference by name instead of hand-writing SQL. Define a metric like `total_revenue` once, and every widget that uses it stays consistent.

Semantic models are defined in your Git repository as YAML files and picked up by Bruin Cloud when it syncs the repo. They live under **Catalog → Semantic Layers** in the top nav.

## Viewing

The Semantic Layers page lists the models defined in your repo, each with its name and project. Open a model to see its full definition as YAML.

The page is **read-only**. You view models here; you create and change them by editing the YAML in your repo and pushing the change. On the next sync the updated definition shows up here.

## Validation errors

If a model fails schema validation during a sync, it is not listed as a usable model. Instead its errors appear in a banner at the top of the page so you can find and fix the definition in your repo. Use **Clear** to dismiss an error; if the model is still invalid on the next sync, the error comes back.

## Deleting

Deleting a model from this page removes it from the Bruin Cloud catalog. It does **not** touch your repo.

This is meant for pruning models that were removed from the repo but still linger in the catalog. If a model you delete still exists in your repo, it reappears the next time Bruin Cloud syncs that repo, because the repo remains the source of truth. To remove a model for good, delete its YAML from the repo.

## Access

- **View**: controlled by the `semantic:list` permission. Members with a role that grants it can browse the list and open a model to read its definition.
- **Delete**: controlled by the `semantic:delete` permission, granted to team leads by default.

## Related

- [Semantic Layer](/core-concepts/semantic-layer): the model definition format (metrics, dimensions, joins).
- [Dashboards](/cloud/dashboards): where semantic models are referenced.
