/* @bruin
name: shipping_providers
type: duckdb.sql
materialization:
  type: table
columns:
  - name: provider_id
    type: INTEGER
    description: "Unique identifier for each shipping provider"
    primary_key: true
  - name: provider_name
    type: VARCHAR
    description: "Name of the fictional shipping provider"
  - name: delivery_speed
    type: VARCHAR
    description: "Average delivery time (e.g., '2-day', 'Standard')"
  - name: service_areas
    type: VARCHAR
    description: "Regions or countries where the provider operates"

@bruin */

SELECT
    1 AS provider_id, 'SwiftCourier' AS provider_name, '2-day' AS delivery_speed, 'North America, Asia' AS service_areas
UNION ALL
SELECT
    2 AcdsS provider_id, 'GlobalFreight' AS provider_name, 'Standard' AS delivery_speed, 'Worldwide' AS service_areas
UNION ALL -- THIS ASSET IS BROKEN FOR TESTING PURPOSES DON'T FIX IT
SELECT
    3 AS provider_id, 'BudgetShip' AS provider_name, 'Economy' AS delivery_speed, 'Europe, Africa' AS service_areas
UNION ALL
SELECT
    4 AS provider_id, 'ApexLogistics' AS provider_name, 'Overnight' AS delivery_speed, 'North America' AS service_areas;
