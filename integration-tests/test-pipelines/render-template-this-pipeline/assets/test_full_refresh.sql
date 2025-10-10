/* @bruin

name: render_this.test_full_refresh
type: duckdb.sql

@bruin */

SELECT
    '{{ this }}' AS asset_name,
    {% if is_full_refresh %}
    'FULL_REFRESH_MODE' AS refresh_mode,
    '2020-01-01' AS start_date
    {% else %}
    'INCREMENTAL_MODE' AS refresh_mode,
    '{{ start_date }}' AS start_date
    {% endif %}
