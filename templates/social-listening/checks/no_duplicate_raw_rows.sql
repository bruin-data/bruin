-- Raw tables are keyed by event_key (source + external ID + content hash; engagement counters excluded).
SELECT 'reddit' AS source_table, event_key, COUNT(*) AS copies
FROM raw.raw_reddit_content GROUP BY event_key HAVING COUNT(*) > 1
UNION ALL
SELECT 'hackernews' AS source_table, event_key, COUNT(*) AS copies
FROM raw.raw_hackernews_content GROUP BY event_key HAVING COUNT(*) > 1
UNION ALL
SELECT 'github' AS source_table, event_key, COUNT(*) AS copies
FROM raw.raw_github_content GROUP BY event_key HAVING COUNT(*) > 1
UNION ALL
SELECT 'stackoverflow' AS source_table, event_key, COUNT(*) AS copies
FROM raw.raw_stackoverflow_content GROUP BY event_key HAVING COUNT(*) > 1
UNION ALL
SELECT 'authorised_export' AS source_table, event_key, COUNT(*) AS copies
FROM raw.raw_authorised_export_content GROUP BY event_key HAVING COUNT(*) > 1
