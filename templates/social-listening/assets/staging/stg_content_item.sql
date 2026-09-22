/* @bruin
name: staging.stg_content_item
type: duckdb.sql
materialization: {type: table}
depends: [raw.raw_reddit_content, raw.raw_hackernews_content]
columns:
  - name: content_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: source
    type: string
    checks: [{name: accepted_values, value: [reddit, hackernews]}]
  - name: source_url
    type: string
    checks: [{name: not_null}]
  - name: published_at
    type: timestamp
    checks: [{name: not_null}]
@bruin */

WITH raw_content AS (
  SELECT * FROM raw.raw_reddit_content UNION ALL SELECT * FROM raw.raw_hackernews_content
), parsed AS (
  SELECT source, external_id, payload_json::JSON AS payload, published_at, collected_at, source_cursor, ingest_run_id
  FROM raw_content
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY source, external_id ORDER BY collected_at DESC) AS row_num
  FROM parsed
)
SELECT
  sha256(source || ':' || external_id) AS content_id,
  source,
  external_id,
  payload->>'type' AS content_type,
  payload->>'url' AS source_url,
  payload->>'author' AS author,
  payload->>'parent_id' AS parent_external_id,
  payload->>'title' AS title,
  payload->>'body' AS body,
  published_at,
  collected_at,
  json_object('score', COALESCE(payload->>'score', payload->>'points')) AS metrics_json,
  json_object('community', payload->>'community', 'source_cursor', source_cursor, 'ingest_run_id', ingest_run_id) AS metadata_json
FROM ranked
WHERE row_num = 1;
