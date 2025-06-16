# Quality checks

Quality checks are tests you can perform on your Bruin assets **after** they run to verify that the produced data satisfies your expectations. They are a great way to ensure your data is accurate, complete and consistent.

Quality checks run after the asset has been executed. If a check fails and its
`blocking` attribute is `true`, the rest of the checks are skipped and the asset
is marked as failed. When `blocking` is `false` the asset still fails but the
remaining checks continue to run. `blocking` defaults to `true`.

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

Quality checks can also be executed on their own without running the asset again:

```bash
bruin run --only checks assets/my_asset.sql
```
