/* @bruin

materialization:
  type: view

columns:
  - name: amount
    type: integer
    description: amount of the payment
    checks:
      - name: positive

@bruin */

SELECT 10
UNION ALL
SELECT 20
UNION ALL
SELECT 30
