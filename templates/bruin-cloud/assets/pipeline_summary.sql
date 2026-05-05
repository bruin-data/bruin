/* @bruin

name: bruin_cloud_logs.pipeline_summary
type: duckdb.sql
materialization:
  type: table

description: |
  One row per pipeline in the Bruin Cloud workspace, joined to its assets to expose aggregate counts.
  Produced by joining `bruin_cloud_logs.pipelines` to `bruin_cloud_logs.assets` on (`name`, `project`) so each pipeline's identity is co-located with its asset count and the variety of asset types it uses.
  Useful as a quick health/inventory dashboard for a Bruin Cloud workspace — e.g. spotting pipelines without assets or pipelines with a heterogeneous mix of asset types.

  Note: this summary intentionally selects only the pipeline columns (`name`, `project`) that are guaranteed to be populated in every Bruin Cloud workspace. Other pipeline fields (`owner`, `description`, `schedule`, `start_date`, `commit`, `default_connections`) are loaded by `bruin_cloud_logs.pipelines` but may be dropped from the destination by dlt's schema inference when every row in the snapshot has a NULL value for them — referencing them here would cause workspace-specific failures. Extend the SELECT below if your workspace populates those fields.

depends:
  - bruin_cloud_logs.pipelines
  - bruin_cloud_logs.assets

columns:
  - name: pipeline_name
    type: varchar
    description: "Pipeline name, copied from `bruin_cloud_logs.pipelines.name`. Not unique on its own — combine with `project` to identify a pipeline."
    checks:
      - name: not_null
  - name: project
    type: varchar
    description: "Bruin Cloud project (workspace folder) the pipeline belongs to. Combined with `pipeline_name` it forms the unique key for this table."
  - name: total_assets
    type: integer
    description: "Total number of assets attached to this pipeline. Zero indicates a pipeline with no assets defined yet."
    checks:
      - name: non_negative
  - name: distinct_asset_types
    type: integer
    description: "Number of distinct asset types in the pipeline (e.g. an `ingestr` + `duckdb.sql` mix counts as 2). Higher values indicate more heterogeneous pipelines."
    checks:
      - name: non_negative

@bruin */

SELECT
    p.name AS pipeline_name,
    p.project,
    COUNT(a.name) AS total_assets,
    COUNT(DISTINCT a.type) AS distinct_asset_types
FROM bruin_cloud_logs.pipelines p
LEFT JOIN bruin_cloud_logs.assets a
       ON a.pipeline = p.name
      AND a.project  = p.project
GROUP BY p.name, p.project
ORDER BY total_assets DESC
