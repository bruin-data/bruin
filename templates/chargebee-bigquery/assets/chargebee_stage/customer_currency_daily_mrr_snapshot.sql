/* @bruin
name: chargebee_stage.customer_currency_daily_mrr_snapshot
type: bq.sql
description: >
  Daily MRR observation history at Chargebee customer and native-currency grain,
  and the snapshot every monthly report reads. Chargebee subscription records
  are mutable and overwrite their own past, so a repriced or cancelled
  subscription reports only its current state and prior daily MRR cannot be
  recovered from Chargebee; this asset records that state while it is still
  current. Each run observes the pipeline end date and adds that day, replacing
  only the rows for the date being run, so re-running a date is idempotent.
  History accrues from the first run onward and cannot be backfilled, and the
  movement and retention reports need two contiguous monthly observations before
  they classify anything. It sums the Chargebee-computed subscription MRR and
  retains a zero-MRR row when a customer has only ineligible (e.g. cancelled)
  subscriptions, so a churned customer stays visible at zero instead of
  disappearing.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: snapshot_date
  partition_by: snapshot_date

depends:
  - chargebee_stage.subscriptions

tags:
  - chargebee_stage
  - chargebee
  - billing
  - snapshot
  - mrr

custom_checks:
  - name: snapshot keys are unique
    description: >
      Each daily reporting snapshot must contain at most one row per snapshot
      date, Chargebee customer, and native currency.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          snapshot_date,
          chargebee_customer_id,
          currency_code
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: current customer currencies are covered by this snapshot
    description: >
      Every current subscription customer/currency pair must be represented for
      the pipeline end date. An empty Chargebee account passes this check.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT DISTINCT
          chargebee_customer_id,
          currency_code
        FROM {{ schema_prefix }}chargebee_stage.subscriptions
        WHERE chargebee_customer_id IS NOT NULL
          AND currency_code IS NOT NULL
      ) AS source
      LEFT JOIN {{ this }} AS snapshot
        ON snapshot.snapshot_date = DATE('{{ end_date }}')
        AND snapshot.chargebee_customer_id = source.chargebee_customer_id
        AND snapshot.currency_code = source.currency_code
      WHERE snapshot.chargebee_customer_id IS NULL
    value: 0

columns:
  - name: snapshot_date
    type: DATE
    description: >
      UTC date this row observed. It is the incremental key and the partition
      key, so a re-run replaces only its own day and leaves the rest of the
      history in place.
    primary_key: true
    checks:
      - name: not_null
  - name: chargebee_customer_id
    type: STRING
    description: Chargebee customer identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native Chargebee currency.
    primary_key: true
    checks:
      - name: not_null
  - name: active_subscription_count
    type: INT64
    description: Count of MRR-eligible subscriptions for the customer/currency.
  - name: ending_mrr_minor
    type: NUMERIC
    description: Ending MRR in native-currency minor units.
@bruin */

SELECT
  DATE('{{ end_date }}') AS snapshot_date,
  chargebee_customer_id,
  currency_code,
  COUNTIF(COALESCE(is_mrr_eligible, FALSE)) AS active_subscription_count,
  COALESCE(
    SUM(IF(
      COALESCE(is_mrr_eligible, FALSE),
      subscription_mrr_minor,
      CAST(0 AS NUMERIC)
    )),
    CAST(0 AS NUMERIC)
  ) AS ending_mrr_minor
FROM chargebee_stage.subscriptions
WHERE chargebee_customer_id IS NOT NULL
  AND currency_code IS NOT NULL
GROUP BY 1, 2, 3;
