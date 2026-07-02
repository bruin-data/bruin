/* @bruin

name: silver.salesforce_activity_timeline
type: sf.sql
description: |
  Unified Salesforce Task and Event activity timeline for credit union banker
  follow-up, appointments, document collection, underwriting callbacks, and
  financial wellness outreach.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - salesforce
  - silver
  - activity
domains:
  - crm
  - member_relationships
meta:
  asset_grain: One row per current Salesforce Task or Event activity.
  load_pattern: Full rebuild over current bronze activity and opportunity objects.
  refresh_cadence: Daily batch pipeline.
  source_system: Salesforce Sales Cloud via bronze ingestr assets.

materialization:
  type: table
  strategy: create+replace
  cluster_by:
    - activity_date
    - activity_type

depends:
  - bronze.salesforce_tasks
  - bronze.salesforce_events
  - bronze.salesforce_opportunities
  - bronze.salesforce_users

columns:
  - name: activity_id
    type: VARCHAR
    description: Salesforce Task or Event identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: activity_type
    type: VARCHAR
    description: Task or Event.
  - name: opportunity_id
    type: VARCHAR
    description: Related Opportunity identifier.
  - name: account_id
    type: VARCHAR
    description: Related Account identifier.
  - name: owner_id
    type: VARCHAR
    description: Salesforce User identifier for the activity owner.
  - name: owner_name
    type: VARCHAR
    description: Activity owner name.
  - name: subject
    type: VARCHAR
    description: Activity subject.
  - name: activity_date
    type: DATE
    description: Activity date.
  - name: status
    type: VARCHAR
    description: Task status, or Completed for events.
  - name: priority
    type: VARCHAR
    description: Task priority when applicable.
  - name: duration_in_minutes
    type: INTEGER
    description: Event duration when applicable.
  - name: completed_flag
    type: BOOLEAN
    description: Whether the activity is completed.
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest Salesforce SystemModstamp for the activity row.
    checks:
      - name: not_null

@bruin */

WITH tasks AS (
    SELECT
        id AS activity_id,
        'Task' AS activity_type,
        what_id AS opportunity_id,
        owner_id,
        subject,
        activity_date::DATE AS activity_date,
        status,
        priority,
        NULL::INTEGER AS duration_in_minutes,
        LOWER(COALESCE(status, '')) = 'completed' AS completed_flag,
        system_modstamp
    FROM bronze.salesforce_tasks
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

events AS (
    SELECT
        id AS activity_id,
        'Event' AS activity_type,
        what_id AS opportunity_id,
        owner_id,
        subject,
        activity_date::DATE AS activity_date,
        'Completed' AS status,
        NULL::VARCHAR AS priority,
        duration_in_minutes,
        TRUE AS completed_flag,
        system_modstamp
    FROM bronze.salesforce_events
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

activities AS (
    SELECT * FROM tasks
    UNION ALL
    SELECT * FROM events
),

opportunities AS (
    SELECT *
    FROM bronze.salesforce_opportunities
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

users AS (
    SELECT *
    FROM bronze.salesforce_users
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY name) = 1
)

SELECT
    a.activity_id,
    a.activity_type,
    a.opportunity_id,
    o.account_id,
    a.owner_id,
    u.name AS owner_name,
    a.subject,
    a.activity_date,
    a.status,
    a.priority,
    a.duration_in_minutes,
    a.completed_flag,
    GREATEST(
        COALESCE(a.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(o.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01'))
    ) AS source_system_modstamp
FROM activities AS a
LEFT JOIN opportunities AS o
    ON o.id = a.opportunity_id
LEFT JOIN users AS u
    ON u.id = a.owner_id
