/* @bruin
name: macro_example
description: |
  This asset demonstrates the use of macros in Bruin.
  Macros are defined in the macros/ folder and are automatically available.

materialization:
  type: table

depends:
  - example

columns:
  - name: country
    type: varchar
    description: Country name
  - name: count
    type: bigint
    description: Number of people per country

@bruin */

-- Example 1: Using the count_by macro
-- This counts the number of people per country
{{ count_by('example', 'country') }}

{% raw %}
-- You can also try other macros:
--
-- Example 2: Get top countries
-- {{ top_n('example', 'id', 3) }}
--
-- Example 3: Filter by specific countries
-- {{ in_list('example', 'country', ['spain', 'germany']) }}
--
-- Example 4: Generate a surrogate key
-- SELECT
--     {{ generate_surrogate_key(['id', 'country']) }},
--     *
-- FROM example
--
-- Example 5: Safe division (useful for calculating percentages)
-- WITH totals AS (
--     SELECT COUNT(*) as total FROM example
-- )
-- SELECT
--     country,
--     COUNT(*) as count,
--     {{ safe_divide('COUNT(*)', '(SELECT total FROM totals)') }} * 100 as percentage
-- FROM example
-- GROUP BY country
{% endraw %}
