# SQL Assets
Bruin supports running SQL assets against a variety of data platforms natively.

You can define SQL assets in a file ending with `.sql`:
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

##  Examples
The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

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
