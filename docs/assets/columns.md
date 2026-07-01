# Columns

Bruin supports column definitions inside assets to make them a part of your data pipelines:

- you can document the existing columns in an asset and add further metadata, e.g. `primary_key`
- you can define column-level quality checks
- you can define whether or not a column should be updated as a result of a [`merge` materialization](./materialization.md#merge)

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
| `source_column`   | String  | no   | For ingestr assets, the source column name to map onto `name`. See [Column name mapping](#column-name-mapping-ingestr-assets). |
| `type`            | String  | no   | The column type in the DB                                                       |
| `mask`            | String  | no   | For ingestr assets, a masking rule or method. See [Column masking](#column-masking-ingestr-assets). |
| `description`     | String  | no   | The description for the column                                                  |
| `tags`            | String[]| no   | Tags applied to the column for categorization and filtering                     |
| `primary_key`     | Bool    | no   | Whether the column is a primary key                                             |
| `update_on_merge` | Bool    | no   | Whether the column should be updated with [`merge`](./materialization.md#merge) |
| `merge_sql`       | String  | no   | Expression to compute column on merge; takes precedence over `update_on_merge` |
| `nullable`        | Bool    | no   | Whether the column can contain NULL values                                      |
| `default`         | String  | no   | Default value expression for the column                                         |
| `precision`       | Int     | no   | Total number of digits for numeric types (e.g. `10` in `decimal(10,2)`)         |
| `scale`           | Int     | no   | Number of digits after the decimal point for numeric types                      |
| `length`          | Int     | no   | Maximum length for character types (e.g. `255` in `varchar(255)`)               |
| `collation`       | String  | no   | Collation used for string comparison and sorting                                |
| `foreign_key`     | Object  | no   | A foreign-key reference to a column in another asset, see [Foreign keys](#foreign-keys) |
| `owner`           | String  | no   | The owner of the column for governance and lineage                              |
| `domains`         | String[]| no   | Business domains the column belongs to                                          |
| `meta`            | Map     | no   | Additional metadata for the column                                              |
| `checks`          | Check[] | no   | The quality checks defined for the column                                       |

### Foreign keys

`foreign_key` documents that a column references a column in another asset. It captures
the relationship for documentation and lineage purposes; whether it is enforced depends on
the target platform (most warehouses store it as metadata only).

```yaml
columns:
  - name: customer_id
    type: integer
    foreign_key:
      table: customers   # the name of another asset
      column: id         # the referenced column in that asset
```

| key      | type   | req? | description                                  |
|----------|--------|------|----------------------------------------------|
| `table`  | String | yes  | The name of the referenced asset             |
| `column` | String | yes  | The referenced column within that asset      |

These fields are optional, but when set, `bruin validate` checks that they are well-formed:
a foreign key must name both a `table` and a `column`, the referenced asset must exist in
the pipeline, and the referenced column must exist on it. Numeric type detail is also
checked — `precision`/`length` must be positive, `scale` must not be negative, and `scale`
must not exceed `precision`.

### Type detail

`precision`, `scale`, `length`, and `collation` complement `type` with the structured
detail that databases keep at the column level. They do not replace `type`; you can set
`type: decimal` and add `precision`/`scale` instead of encoding it as `decimal(10,2)`.

```yaml
columns:
  - name: amount
    type: decimal
    precision: 10
    scale: 2
    default: "0"
  - name: name
    type: varchar
    length: 255
    collation: en_US
```

### DDL generation

When an asset uses the `ddl` [materialization](./materialization.md) strategy, these fields
are emitted into the generated `CREATE TABLE` statement: `precision`/`scale`/`length` become
type modifiers (e.g. `decimal(10, 2)`, `varchar(255)`), and `collation`, `default`, and
`foreign_key` become column/table clauses. Foreign keys are emitted as `NOT ENFORCED` on
platforms that only store them as metadata (e.g. BigQuery). Support is currently available
for PostgreSQL, BigQuery, and Snowflake, and is being extended to the other platforms.

### Quality Checks

The structure of the quality checks is rather simple:

| key        | type   | req? | description                                                       |
|------------|--------|------|-------------------------------------------------------------------|
| `name`     | String | yes  | The name of the quality check, see [Quality](../quality/overview) |
| `blocking` | Bool   | no   | Whether the check should block the downstreams, default `true`    |
| `value`    | Any    | no   | Check-specific expected value                                     |

For more details on the quality checks, please refer to the  [Quality](../quality/overview) documentation.

### Column name mapping (ingestr assets)

For ingestr assets, you can rename a column on its way from the source to the destination
by setting `source_column` on the column entry. The `name` field stays the destination
column name; `source_column` is the column that exists on the source.

```yaml
columns:
  - name: first_name
    source_column: fname
    type: string
    primary_key: true
  - name: email
    source_column: eml
    type: string
  - name: created_at
    source_column: crtd_ts
    type: timestamp
```

With the mapping above, the source table's `fname`, `eml`, and `crtd_ts` columns land in
the destination as `first_name`, `email`, and `created_at`. Columns without
`source_column` keep their original source names.

For the mapping to take effect, `enforce_schema: "true"` must be set under the asset's
`parameters` block. 

```yaml
parameters:
  enforce_schema: "true"

columns:
  - name: first_name
    source_column: fname
    type: string
  - name: email
    source_column: eml          # type omitted: ingestr just renames, no type enforcement
```

### Column masking (ingestr assets)

For ingestr assets, you can define a column mask next to the column metadata. If `mask`
contains only the masking method, Bruin qualifies it with the column name before passing
it to ingestr:

```yaml
columns:
  - name: email
    type: string
    mask: hash
```

The example above is passed to ingestr as `--mask email:hash`. You can also provide the
full ingestr mask rule directly:

```yaml
columns:
  - name: email
    mask: email:hash
```
