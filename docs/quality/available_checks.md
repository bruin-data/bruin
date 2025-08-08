# Available Checks

Bruin provides the following checks to validate assets, ensuring that asset data meets specified quality standards.

- [**Accepted Values**](#accepted-values)
- [**Negative**](#negative)
- [**Non-Negative**](#non-negative)
- [**Not-Null**](#not-null)
- [**Pattern**](#pattern)
- [**Positive**](#positive)
- [**Unique**](#unique)
- [**Min**](#min)
- [**Max**](#max)

You can find a detailed description of each check below.
## Accepted values

This check will verify that the value of the column will be one of the accepted values

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: accepted_values
        value: [1, 3, 5, 7, 9]
```
## Negative
This check will verify that the values of the column are all negative

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: negative
```
## Non-Negative
This check will verify that the values of the column are all non negative (positive or zero)
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: non_negative
```

## Not-Null
This check will verify that none of the values of the checked column are null.
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: not_null
```
## Pattern
The `pattern` quality check ensures that the values of the column match a specified regular expression.

> [!WARNING]
> For most platforms, POSIX regular expressions are the ones meant to be used with this check, but some platforms that don't support it (Synapse, MsSQL) might have other pattern languages (see [Pattern matching in SQL Server](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms187489(v=sql.105)?redirectedfrom=MSDN))

```yaml
columns:
  - name: name
    type: string
    description: "Just a name"
    checks:
        - name: pattern
          value: "^[A-Z][a-z]*$"
```


## Positive

This check will verify that the values of the column are all positive, i.e. greater than zero.

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: positive
```


## Unique

This check will verify that no value in the specified column appears more than once

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: unique
```

## Min
This check ensures that all values in the column are greater than or equal to the specified minimum threshold.

```yaml
columns:
  - name: amount
    type: float
    checks:
      - name: min
        value: 0.0
```

You can also use integers or strings where appropriate (e.g., dates):

```yaml
checks:
  - name: min
    value: 10
  - name: min
    value: "2024-01-01"
```

## Max
This check ensures that all values in the column are less than or equal to the specified maximum threshold.

```yaml
columns:
  - name: amount
    type: float
    checks:
      - name: max
        value: 100.0
```

You can also use integers or strings where appropriate (e.g., dates):

```yaml
checks:
  - name: max
    value: 100
  - name: max
    value: "2024-12-31"
```