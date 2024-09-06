# Asset Definition
Assets are defined in a YAML format in the same file as the asset code. 
This enables the metadata to be right next to the code, reducing the friction when things change and encapsulating the relevant details in a single file. 
The definition includes all the details around an asset from its name to the quality checks that will be executed.

Here's an example asset definition:
```sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

depends:
   - hello_python

materialization:
   type: table

tags:
   - dashboard
   - team:xyz
   
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

@bruin */

select 1 as one
union all
select 2 as one
```

::: info
Bruin has [an open-source Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=bruin.bruin) extension that does syntax-highlighting for the definition syntax and more.
:::



## `name`
The name of the asset, used for many things including dependencies, materialiation and more. Corresponds to the `schema.table` convention.
Must consist of letters and dot `.` character.
- **Type:** `String`

## `type`
The type of the asset, determines how the execution will happen. Must be one of the types [here](https://github.com/bruin-data/bruin/blob/main/pkg/executor/defaults.go).
- **Type:** `String` 

## `depends`
The list of assets this asset depends on. This list determines the execution order.
In other words, the asset will be executed only when all of the assets in the `depends` list have succeeded.
- **Type:** `String[]`

## `materialization`
This option determines how the asset will be materialized. Bruin knows about various materialization strategies, refer to the docs on [materialization](./materialization) for more details.

- **Type:** `Object`
- **Keys:** 

Here's an example materialization definition:
```yaml
materialization:
  type: table
  strategy: delete+insert
  incremental_key: dt
  partition_by: dt
  cluster_by:
    - dt
    - user_id
```

The following is the underlying data structure that parses this configuration:
```go
type Materialization struct {
	Type           string
	Strategy       string
	PartitionBy    string
	ClusterBy      []string
	IncrementalKey string
}
```

### `materialization > type`
The type of the materialization, can be one of the following:
- `table`
- `view`

**Default:** none

### `materialization > strategy`
The strategy used for the materialization, can be one of the following:
- `create+replace`: overwrite the existing table with the new version.
- `delete+insert`: incrementally update the table by only refreshing a certain partition.
- `append`: only append the new data to the table, never overwrite.
- `merge`: merge the existing records with the new records, requires a primary key to be set.

### `materialization > partition_by`
Define the column that will be used for the partitioning of the resulting table. This is used to instruct the data warehouse to set the column for the partition key.

- **Type:** `String`
- **Default:** none


### `materialization > cluster_by`
Define the columns that will be used for the clustering of the resulting table. This is used to instruct the data warehouse to set the columns for the clustering.

- **Type:** `String[]`
- **Default:** `[]`

### `materialization > incremental_key`

This is the column of the table that will be used for incremental updates of the table.
- **Type:** `String[]`
- **Default:** `[]`

## `tags`
A list of tags that can be used to categorize the asset. This is useful for searching and filtering assets when running.
- **Type:** `String[]`
- **Default:** `[]`

## `columns`

This is a list that contains all the columns defined with the asset, along with their quality checks and other metadata.

Here's an example column definition:
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]
```

Here's the underlying data type that represents the columns:
```go
type Column struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"`
	Description   string        `json:"description"`
	Checks        []ColumnCheck `json:"checks"`
	PrimaryKey    bool          `json:"primary_key"`
	UpdateOnMerge bool          `json:"update_on_merge"`
}
```