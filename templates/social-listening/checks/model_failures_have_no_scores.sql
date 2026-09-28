-- A model error or invalid answer stores NULL scores, and the assessment keeps them NULL.
SELECT 'llm_assessment_result' AS table_name, result_key AS row_key, status
FROM enrichment.llm_assessment_result
WHERE record_kind = 'result' AND status <> 'ok'
  AND (llm_relevance IS NOT NULL OR llm_fit IS NOT NULL OR llm_confidence IS NOT NULL OR llm_intent IS NOT NULL)
UNION ALL
SELECT 'fct_mention_assessment', assessment_id, assessment_status
FROM enrichment.fct_mention_assessment
WHERE assessment_status IN ('llm_error', 'llm_invalid_response', 'llm_pending')
  AND (model_generated OR llm_relevance IS NOT NULL OR llm_intent IS NOT NULL)
