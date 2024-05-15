# Unique

This check will verify that no value in the specified column appears more than once

### Example

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: unique
```