/* @bruin
name: render_hooks.test_hoist
type: bq.sql
hooks:
  pre:
    - query: SELECT 'pre; 雪' AS message
  post:
    - query: DECLARE last_var INT64 DEFAULT 29
    - query: SELECT last_var
@bruin */

DECLARE first_var array<STRING>;
SELECT first_var;
