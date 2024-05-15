# Pattern

The `pattern` quality check ensures that the values of the column match a specified regular expression.
For most platforms, POSIX regular expressions are the ones meant to be used with this check, but some platforms that don't support it (Synapse, MsSQL) might have other pattern languages (see [Pattern matching in Sql Server](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms187489(v=sql.105)?redirectedfrom=MSDN))

### Example

```yaml
columns:
  - name: name
    type: string
    description: "Just a name"
    checks:
        - name: pattern
          value: "^[A-Z][a-z]*$"
```