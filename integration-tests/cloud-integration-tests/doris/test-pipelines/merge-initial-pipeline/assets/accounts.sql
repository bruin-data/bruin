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

SELECT 1 AS account_id, 'kept' AS account_status, 10 AS balance, 1 AS update_count
UNION ALL
SELECT 2 AS account_id, 'will-update' AS account_status, 20 AS balance, 1 AS update_count
