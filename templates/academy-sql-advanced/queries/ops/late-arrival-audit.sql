-- Run with: bruin query --description "find late-arriving order lines" --query "$(cat queries/ops/late-arrival-audit.sql)"
SELECT order_id,
       CAST(_loaded_at AS DATE) - CAST(ordered_at AS DATE) AS delay_days,
       CAST(ordered_at AS DATE) AS business_date,
       CAST(_loaded_at AS DATE) AS ingestion_date
FROM fct_order_lines
WHERE CAST(_loaded_at AS DATE) - CAST(ordered_at AS DATE) > 30
ORDER BY delay_days DESC, order_id
LIMIT 100;
