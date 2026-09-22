/* @bruin
name: marts.mart_share_of_voice_monthly
type: duckdb.sql
materialization: {type: table}
depends: [staging.stg_content_item, enrichment.fct_term_match, config.dim_term]
columns:
  - name: month_start
    type: date
    checks: [{name: not_null}]
  - name: term_category
    type: string
    checks: [{name: not_null}]
@bruin */
WITH matched AS (
  SELECT date_trunc('month', item.published_at)::DATE AS month_start, term.category AS term_category,
    item.content_id, item.author,
    item.author IN ({% for author in var.team_authors %}'{{ author | replace("'", "''") }}'{% if not loop.last %}, {% endif %}{% endfor %} '') AS is_team_content
  FROM staging.stg_content_item item
  JOIN enrichment.fct_term_match match USING (content_id)
  JOIN config.dim_term term USING (term_id)
), totals AS (SELECT month_start, COUNT(DISTINCT content_id) AS denominator FROM matched GROUP BY 1)
SELECT matched.month_start, matched.term_category, COUNT(DISTINCT matched.content_id) AS mention_count,
  COUNT(DISTINCT matched.author) FILTER (WHERE NOT is_team_content) AS organic_authors,
  COUNT(DISTINCT matched.content_id) FILTER (WHERE is_team_content) AS team_content_count,
  totals.denominator, COUNT(DISTINCT matched.content_id)::DOUBLE / NULLIF(totals.denominator, 0) AS share_of_voice,
  CASE WHEN totals.denominator > 0 THEN 1.0 ELSE 0.0 END AS coverage
FROM matched JOIN totals USING (month_start)
GROUP BY 1, 2, totals.denominator;
