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

SELECT 10
union all
SELECT 20
union all
SELECT 30
