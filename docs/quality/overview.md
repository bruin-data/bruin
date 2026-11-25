# Quality checks

Quality checks are tests you can perform on your Bruin assets **after** they run to verify that the produced data satisfies your expectations. They are a great way to ensure your data is accurate, complete and consistent.

Quality checks run after the asset has been executed. If a check fails and its `blocking` attribute is `true`, the asset is marked as failed and downstream assets are prevented from running. When `blocking` is `false`, the check failure is recorded but downstream assets are not blocked. `blocking` defaults to `true`.

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

## Showing failing rows

A failed check reports how many rows failed. Add `--show-failing-rows N` to also print up to `N` of those rows under the error:

```bash
bruin run --only checks --show-failing-rows 5 assets/my_asset.sql
```

```text
└── id.not_null - column 'id' has 1 null values
    failing rows:
    +------+------+----------+
    | id   | name | quantity |
    +------+------+----------+
    | NULL | b    | 3        |
    +------+------+----------+
```

For `unique`, Bruin prints the repeated values and how many times each appears. For [custom checks](./custom.md) that use `count`, it prints rows from the check's query; checks that use `value` return a single number and show no rows.

Bruin runs one more query for each failed check, so the option can add cost on warehouses that bill per query, and the printed rows appear in the run logs.

## Semantic layer checks

Semantic models can have their own quality checks on dimensions, metrics, and the model as a whole. `bruin run` does not run these checks. Use `bruin semantic check` instead. See [semantic layer quality checks](../core-concepts/semantic-layer.md#quality-checks) for details.
