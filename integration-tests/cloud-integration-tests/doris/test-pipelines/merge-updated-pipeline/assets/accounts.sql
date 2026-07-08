/* @bruin
name: bruin_test.merge_accounts
type: doris.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: account_id
    type: INT
    primary_key: true
  - name: account_status
    type: VARCHAR(32)
    update_on_merge: true
  - name: balance
    type: INT
    update_on_merge: true
  - name: update_count
    type: INT
    merge_sql: target.`update_count` + source.`update_count`
@bruin */

SELECT 2 AS account_id, 'updated' AS account_status, 30 AS balance, 1 AS update_count
UNION ALL
SELECT 3 AS account_id, 'new' AS account_status, 40 AS balance, 1 AS update_count
