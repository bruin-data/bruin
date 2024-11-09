# Columns

Bruin supports column definitions inside assets to make them a part of your data pipelines:

- you can document the existing columns in an asset and add further metadata, e.g. `primary_key`
- you can define column-level quality checks
- you can define whether or not a column should be updated as a result of a [
  `merge` materialization](./materialization.md#merge)

## Definition Schema

The top level `columns` key is where you can define your columns. This is a list that contains all the columns defined
with the asset, along with their quality checks and other metadata.

Here's an example column definition:

```yaml
columns:
  - name: col1
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
      - name: unique
      - name: not_null
      - name: positive
  - name: col2
    type: string
    description: |
      some multi-line definition for this column
    update_on_merge: true
    checks:
      - name: not_null
      - name: accepted_values
        value: [ 'value1', 'value2' ]
```

Each column will have the following keys:

| key               | type    | req? | description                                                                     |
|-------------------|---------|------|---------------------------------------------------------------------------------|
| `name`            | String  | yes  | The name of the column                                                          |
| `type`            | String  | no   | The column type in the DB                                                       |
| `description`     | String  | no   | The description for the column                                                  |
| `primary_key`     | Bool    | no   | Whether the column is a primary key                                             |
| `update_on_merge` | Bool    | no   | Whether the column should be updated with [`merge`](./materialization.md#merge) |
| `checks`          | Check[] | no   | The quality checks defined for the column                                       |

### Quality Checks

The structure of the quality checks is rather simple:

| key        | type   | req? | description                                                       |
|------------|--------|------|-------------------------------------------------------------------|
| `name`     | String | yes  | The name of the quality check, see [Quality](../quality/overview) |
| `blocking` | Bool   | no   | Whether the check should block the downstreams, default `true`    |
| `value`    | Any    | no   | Check-specific expected value                                     |                                     
For more details on the quality checks, please refer to the  [Quality](../quality/overview) documentation.