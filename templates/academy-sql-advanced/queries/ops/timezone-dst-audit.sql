-- Run with: bruin query --connection duckdb-default --description "compare local and UTC timestamps around DST transitions" --limit 100 --query "$(cat queries/ops/timezone-dst-audit.sql)"
SELECT
    o.order_id,
    s.city,
    s.timezone,
    o.ordered_at,
    CAST(o.ordered_at_utc AT TIME ZONE 'UTC' AS TIMESTAMP) AS ordered_at_utc,
    CASE
        WHEN o.ordered_at < TIMESTAMP '2024-04-01' THEN 'spring edge window'
        ELSE 'autumn edge window'
    END AS edge_window
FROM orders AS o
JOIN stores AS s ON o.store_id = s.store_id
WHERE ((o.ordered_at >= TIMESTAMP '2024-03-30' AND o.ordered_at < TIMESTAMP '2024-04-01')
    OR (o.ordered_at >= TIMESTAMP '2024-10-26' AND o.ordered_at < TIMESTAMP '2024-10-29'))
  AND o.ordered_at_utc IS NOT NULL
ORDER BY o.ordered_at, o.order_id;
