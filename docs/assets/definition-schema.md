# Asset Definition
Assets are defined in a YAML format in the same file as the asset code. 
This enables the metadata to be right next to the code, reducing the friction when things change and encapsulating the relevant details in a single file. 
The definition includes all the details around an asset from its name to the quality checks that will be executed.

Here's an example asset definition:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

owner: my-team@acme-corp.com

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

## `owner`
The owner of the asset, has no functional implications on Bruin CLI as of today, allows documenting the ownership information. On [Bruin Cloud](https://getbruin.com), it is used to analyze ownership information, used in governance reports and ownership lineage.  
- **Type:** `String` 

## `tags`
As the name states, tags that are applied to the asset. These tags can then be used while running assets, e.g.:
```bash
bruin run --tags client1
```
- **Type:** `String[]` 

## `depends`
The list of assets this asset depends on. This list determines the execution order.
In other words, the asset will be executed only when all of the assets in the `depends` list have succeeded.
- **Type:** `String[]`

## `materialization`
This option determines how the asset will be materialized. Refer to the docs on [materialization](./materialization) for more details.

## `columns`

This is a list that contains all the columns defined with the asset, along with their quality checks and other metadata. Refer to the [columns](./columns.md) documentation for more details.

## `custom_checks`
This is a list of custom data quality checks that are applied to an asset. These checks allow you to define custom data quality checks in SQL, enabling you to encode any business logic into quality checks that might require more power.

```yaml
custom_checks:
  - name: Client X has 15 credits calculated for June 2024
    description: This client had a problem previously, therefore we want to ensure the numbers make sense, see the ticket ACME-1234 for more details. 
    value: 15
    query: |
      SELECT
        count(*)
      FROM `tier2.client_credits`
      where client="client_x" 
        and date_trunc(StartDateDt, month) = "2024-06-01"
        and credits_spent = 1
```