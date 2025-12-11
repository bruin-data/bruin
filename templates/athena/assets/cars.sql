/* @bruin
name: cars
materialization:
   type: table
   strategy: create+replace
columns:
  - name: id
    type: integer
    description: "identifier of the car"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
  - name: plate
    type: varchar
    description: "plate number"
    checks:
        - name: not_null
        - name: unique
@bruin */

SELECT 1 as id,  'XWE12312' as plate
union all
SELECT 2 as id, 'TRE34535' as plate
union all
SELECT 3 as id, 'OIY54654' as plate

