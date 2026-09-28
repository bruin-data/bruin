-- Redaction requests remove the record from staging and everything built on it.
SELECT r.source, r.external_id, 'staging' AS found_in
FROM config.redaction_request r
JOIN staging.stg_content_item s ON s.source = r.source AND s.external_id = r.external_id
UNION ALL
SELECT r.source, r.external_id, 'mart_mention_queue'
FROM config.redaction_request r
JOIN marts.mart_mention_queue q ON q.source = r.source AND q.external_id = r.external_id
