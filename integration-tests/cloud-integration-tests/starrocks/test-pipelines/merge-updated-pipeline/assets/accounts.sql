/* @bruin
name: bruin_test.merge_accounts
type: starrocks.sql

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
@bruin */

SELECT 2 AS account_id, 'updated' AS account_status, 30 AS balance
UNION ALL
SELECT 3 AS account_id, 'new' AS account_status, 40 AS balance
