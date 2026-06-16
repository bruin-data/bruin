# Quality checks

Quality checks are tests you can perform on your Bruin assets **after** they run to verify that the produced data satisfies your expectations. They are a great way to ensure your data is accurate, complete and consistent.

Quality checks run after the asset has been executed. If a check fails and its
`blocking` attribute is `true`, the asset is marked as failed and downstream
assets are prevented from running. When `blocking` is `false`, the check failure
is recorded but downstream assets are not blocked. `blocking` defaults to `true`.

Below is a short example of attaching checks to columns:

```yaml
columns:
  - name: id
    type: integer
    description: "Primary key"
    checks:            # run built-in checks
      - name: unique
      - name: not_null
```

If any of those checks fails the asset will be marked as failed and any downstream assets will not be executed.

## retries

Both column checks and [custom checks](./custom.md) accept an optional `retries` attribute that controls how many times the check is retried on failure before it is marked as failed. It can be configured independently of the asset- and pipeline-level retries.

```yaml
columns:
  - name: id
    type: integer
    checks:
      - name: unique
        retries: 3   # retry this check up to 3 times
      - name: not_null
```

**Special values:**

- unset: inherit the asset-level [`retries`](../assets/definition-schema.md#retries) (which in turn falls back to the pipeline-level [`retries`](../pipelines/definition.md#retries))
- `0`: no retries
- `> 0`: retry the check up to this many times

Retries are resolved through the chain **check → asset → pipeline**: a check without its own `retries` inherits the asset's, and an asset without its own inherits the pipeline's. An explicit value (including `0`) at any level wins over the inherited default.

Quality checks can also be executed on their own without running the asset again:

```bash
bruin run --only checks assets/my_asset.sql
```
