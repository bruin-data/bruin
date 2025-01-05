/* @bruin
name: product_categories
type: duckdb.sql
materialization:
  type: table

columns:
  - name: category_id
    type: INTEGER
    description: "Unique identifier for the product category"
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: category_name
    type: VARCHAR
    description: "Name of the product category"
    checks:
      - name: not_null
  - name: description
    type: VARCHAR
    description: "Description of the product category"
@bruin */

SELECT
    1 AS category_id, 'Electronics' AS category_name, 'Devices like phones, laptops, and monitors' AS description
UNION ALL
SELECT
    2 AS category_id, 'Accessories' AS category_name, 'Complementary items like headphones and chargers' AS description
UNION ALL
SELECT
    3 AS category_id, 'Appliances' AS category_name, 'Household devices like refrigerators and microwaves' AS description
UNION ALL
SELECT
    4 AS category_id, 'Furniture' AS category_name, 'Home and office furniture like desks and chairs' AS description;
