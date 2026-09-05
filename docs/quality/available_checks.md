# Available Checks

Bruin provides the following checks to validate assets, ensuring that asset data meets specified quality standards.

- [**Accepted Values**](#accepted-values)
- [**Negative**](#negative)
- [**Non-Negative**](#non-negative)
- [**Not-Null**](#not-null)
- [**Pattern**](#pattern)
- [**Positive**](#positive)
- [**Relationships**](#relationships)
- [**Unique**](#unique)
- [**Min**](#min)
- [**Max**](#max)
- [**Freshness**](#freshness)

You can find a detailed description of each check below.

## Freshness

Checks that the latest timestamp in a column is no older than the specified duration.
Bruin queries `MAX(column)` and compares it with the current time on the machine
running Bruin, not the pipeline's start or end date. Older rows do not cause a
failure when the latest row is fresh.

```yaml
columns:
  - name: retrieved_at
    type: timestamp
    checks:
      - name: freshness
        value: 30h
```

`value` must be a positive duration string, such as `30m`, `24h`, or `1h30m`.
Use `24h` for one day; `d` and calendar-based units are not supported.
The check passes at the exact age limit. Future timestamps also pass: this is
a maximum-age check, not a check for clock skew.

Empty tables and columns containing only nulls fail. Nulls alongside valid
timestamps are ignored by `MAX`; add `not_null` to disallow them. Use a timestamp
column, not a string or numeric epoch column. Timestamp offsets are respected;
timestamps without a timezone must represent UTC. The runner's clock must be accurate.

Like other column checks, freshness runs through the normal check lifecycle and
supports `blocking: false`, `retries`, and check notifications. Setting
`blocking: false` lets downstream assets run but does not turn a failed check into
a successful run. Freshness does not run independently as a scheduled monitor or
check materialization time. Each execution issues a `MAX` query, which may scan
the column.

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

## Relationships

This check verifies referential integrity: every non-null value in the checked column must exist in the column identified by its `foreign_key` metadata. Null values are ignored; add a separate `not_null` check when nulls should fail.

```yaml
columns:
  - name: customer_id
    type: integer
    foreign_key:
      table: analytics.customers
      column: id
    checks:
      - name: relationships
```

The referenced table must be another asset in the pipeline. The check runs on the child asset's connection and returns the number of child rows whose keys are missing from the parent. It does not verify that the referenced parent column is unique; add a `unique` check to that column separately when required.

Neither `foreign_key` metadata nor a `relationships` check creates a scheduler dependency. Add the referenced asset to the child asset's `depends` list when it must be refreshed before the relationship check runs.

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
