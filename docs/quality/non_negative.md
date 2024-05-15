# Non negative

This check will verify that the values of the column are all non negative (positive or zero)

### Example

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: non_negative
```

