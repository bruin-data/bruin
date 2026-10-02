-- Every review-queue row carries its evidence, rule, reasons and each score component.
SELECT content_id
FROM marts.mart_mention_queue
WHERE source_url IS NULL OR matched_text IS NULL OR rule_ids IS NULL OR reasons_json IS NULL
   OR relevance IS NULL OR intent_score IS NULL OR fit IS NULL OR engagement IS NULL
   OR freshness IS NULL OR authenticity IS NULL OR priority IS NULL OR queue_state IS NULL
