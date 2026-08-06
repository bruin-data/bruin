/* @bruin
name: web_stage.gsc_export_log
type: bq.sql
description: >
  Publication log of the Search Console bulk export, read from the ExportLog
  table. Search Console only logs successful exports and revises history in
  place, so this is the one place that reveals which dates were republished and
  how stale the newest data is. Check it before explaining a metric change: a
  bumped epoch_version means Google restated the numbers rather than the site
  changing.

materialization:
  type: table
  strategy: truncate+insert

tags:
  - web_stage
  - search_console
  - freshness

columns:
  - name: agenda
    type: STRING
    description: Export agenda, currently always SEARCHDATA.
  - name: namespace
    type: STRING
    description: Table the log entry refers to, such as searchdata_url_impression.
    primary_key: true
    checks:
      - name: not_null
  - name: data_date
    type: DATE
    description: Reporting date the export covers, in Pacific Time.
    primary_key: true
    checks:
      - name: not_null
  - name: epoch_version
    type: INT64
    description: >
      Increments each time Google republishes the same reporting date. Any value
      above 0 means the date was restated after its first publication.
  - name: publish_time
    type: TIMESTAMP
    description: When the export for this reporting date completed.
  - name: is_latest_epoch
    type: BOOL
    description: Whether this row is the most recent publication of the reporting date.
  - name: publication_lag_days
    type: INT64
    description: >
      Days between the reporting date and the publication date. Both are read in
      Pacific Time because that is the basis Search Console reports on.

custom_checks:
  - name: exactly one latest epoch per table and date
    description: >
      Each table and reporting date must resolve to a single newest publication,
      otherwise freshness monitoring reads the wrong row.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          namespace,
          data_date
        FROM {{ this }}
        WHERE is_latest_epoch
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
@bruin */

SELECT
  agenda,
  namespace,
  data_date,
  epoch_version,
  publish_time,
  epoch_version = MAX(epoch_version) OVER (PARTITION BY namespace, data_date) AS is_latest_epoch,
  DATE_DIFF(DATE(publish_time, 'America/Los_Angeles'), data_date, DAY) AS publication_lag_days
FROM `{{ var.search_console_dataset }}.ExportLog`;
