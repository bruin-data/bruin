# Positive

This check will verify that the values of the column are all positive, i.e. greater than zero.

### Example

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: positive
```
