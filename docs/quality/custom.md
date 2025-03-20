# Custom Checks

While Bruin supports a [handful of built-in quality checks](./available_checks.md), they have a shortcoming:
- quality checks are not always within the scope of a single column
- some checks are specific to the business, and custom logic is needed

Due to these reasons, Bruin supports defining custom quality checks using SQL. You can define as many of them, they will all run in parallel.

## Definition Schema

You can define custom quality checks under a key called `custom_checks`:
```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks: // [!code ++]  
  - name: row count is greater than zero // [!code ++]  
    description: this check ensures that the table is not empty // [!code ++]  
    query: SELECT count(*) > 1 FROM dataset.player_count // [!code ++]  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

There are a few fields to configure the check behavior:
- `name`: required, give a name to the check.
- `query`: required, the query to run as the quality check
- `description`: optional, add a longer description if needed using Markdown.
- `value`: optional, a value to compare the quality check output
  - if not given, Bruin will try to match the query output to be integer zero.
- `blocking`: optional, whether the test should block running downstreams, default `true`.

## Examples:

### Simple check
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:  
  - name: row count is greater than zero    
    query: SELECT count(*) > 1 FROM dataset.player_count
    value: 1  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

### Run non-blocking checks
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:  
  - name: row count is greater than zero    
    query: SELECT count(*) > 1 FROM dataset.player_count
    value: 1
    blocking: false  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

> [!INFO]
> Non-blocking checks are useful for long-running or expensive quality checks. It means the downstream assets will not be waiting for this quality check to finish.

### Specific value to expect
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:  
  - name: row count is equal to 15    
    query: SELECT count(*)  FROM dataset.player_count
    value: 15
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

> [!INFO]
> Non-blocking checks are useful for long-running or expensive quality checks. It means the downstream assets will not be waiting for this quality check to finish.
