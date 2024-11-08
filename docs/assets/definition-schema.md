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
This option determines how the asset will be materialized. Refer to the docs on [materialization](./materialization) for more details.

## `columns`

This is a list that contains all the columns defined with the asset, along with their quality checks and other metadata. Refer to the [columns](./columns.md) documentation for more details.