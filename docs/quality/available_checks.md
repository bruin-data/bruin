# Available Checks

Bruin provides the following checks to validate assets, ensuring that asset data meets specified quality standards.

- [**Accepted Values**](#accepted-values)
- [**Negative**](#negative)
- [**Non-negative**](#non-negative)
- [**Not Null**](#not-null)
- [**Pattern**](#pattern)
- [**Positive**](#positive)
- [**Unique**](#unique)

You can find a detailed description of each check below.
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
# Negative
This check will verify that the values of the column are all negative
### Example
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: negative
```
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