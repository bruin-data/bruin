# SQL Assets

Bruin supports running SQL assets against a variety of data platforms natively.

A SQL asset is a single file ending in `.sql` that contains **both** the asset definition and the query body. The definition is a YAML block placed at the top of the file between `/* @bruin` and `@bruin */` markers, followed by the SQL query that produces the asset:

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

The `type` key in the configuration defines what platform to run the query against.

You can see the "Data Platforms" on the left sidebar to see supported types.

::: danger
The definition and the query body of a SQL asset must live in the **same `.sql` file**. You cannot split them — for example, you cannot keep the SQL in `hello_world.sql` and the YAML definition in a sibling `hello_world.asset.yml`. Bruin treats `<name>.asset.yml` files as standalone YAML assets (used for types like [ingestr](./ingestr.md), [sensor](./sensor.md), [seed](./seed.md), and [dashboard](./dashboard.md)), not as companion files to a `.sql` query. If you want a SQL asset, put the `/* @bruin ... @bruin */` header at the top of the `.sql` file containing the query.
:::

## Examples

The examples below show how to use SQL assets in your pipeline. Feel free to change them as you wish according to your needs.

### Simplest: run `SELECT 1`

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

@bruin */

select 1
```

This operation does not save the result anywhere, it simply runs the query on BigQuery.

### Materialize the data

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
@bruin */

select 1 as one
union all
select 2 as one
```

This example will save the result of this query into a table called `dashboard.hello_bq`.

### Incremental processing

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: dt
@bruin */

select * from my_upstream
where dt between '{{ start_datetime }}' and '{{ end_datetime }}'
```

This example will incrementally update the data in the destination table using this query. Read more about [materialization here](./materialization.md).

This example also uses Jinja templates, you can read more about [Jinja here](./templating/templating.md).

### Adding quality checks

SQL assets can define quality checks on the columns produced by the query.

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

columns:
  - name: one
    type: integer
    checks:
      - name: positive
      - name: unique
@bruin */

select 1 as one
union all
select -1 as one
```

In this example the `one` column is validated to be positive and unique after the query runs.
