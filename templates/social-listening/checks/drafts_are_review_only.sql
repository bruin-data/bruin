-- Drafts always require human approval, and the table has no publishing fields.
SELECT draft_id AS problem, 'draft without human approval flag' AS reason
FROM operations.fct_reply_draft
WHERE NOT requires_human_approval
UNION ALL
SELECT column_name, 'publishing column present'
FROM information_schema.columns
WHERE table_schema = 'operations' AND table_name = 'fct_reply_draft'
  AND (column_name LIKE '%posted%' OR column_name LIKE '%published%' OR column_name LIKE '%permalink_out%' OR column_name LIKE '%sent%')
