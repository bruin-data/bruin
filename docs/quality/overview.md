# Quality checks

Quality checks are tests you can perform on your bruin assets after they run to verify that the asset's data satisfies your expectations. 

You can use quality checks to ensure that your data is accurate, complete, and consistent. Quality checks are a powerful tool for ensuring that your data is reliable and trustworthy.

Quality checks run **after** the asset has been executed. If an asset fails a quality check and the `blocking` attribute of that check is `true` the remaining checks won't be executed. 
If `blocking` is `false` the asset will be marked as failed but the remaining checks will be executed.

See example below:

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
          blocking: true
```

`blocking` is `true` by default.

If any of the checks fails, the asset will be marked as failed and any of the downstream assets won't be executed