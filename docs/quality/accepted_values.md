# Accepted values

This check will verify that the value of the column will be one of the accepted values

### Example

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: accepted_values
        value: [1, 3, 5, 7, 9]
```

