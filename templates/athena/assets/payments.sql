/* @bruin
name: payments
materialization:
   type: view
columns:
  - name: amount
    type: integer
    description: "amount of the payment"
    checks:
        - name: positive
@bruin */

SELECT 10 as amount
union all
SELECT 20 as amount
union all
SELECT 30 as amount
