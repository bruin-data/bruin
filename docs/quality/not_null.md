# Not Null

This check will verify that none of the values of the checked column are null.

### Example

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: not_null
```
