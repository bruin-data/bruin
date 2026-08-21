/* @bruin
name: qa.integrity
description: |
  SPOILER. The check that actually matters. Reads the generation scaffolding to
  learn who the generator chose, then asserts that the four deduction threads
  converge on exactly those four residents.

  Cardinality checks alone can pass while the stages lead somewhere else entirely.
  This is the only assertion that the case has the answer it is supposed to have.

  It also asserts the things that make the case fair: that every decoy has a fact
  in the data that clears it, that no case-file row leaks a name, a plate or a
  number, and that no single-column filter isolates anybody.
materialization:
  type: table
depends:
  - qa.shooter_stages
  - qa.driver_stages
  - qa.handler_stages
  - qa.partner_stages
  - town.citizens
  - town.vehicles
  - town.devices
  - town.clinic_visits
  - casefile.witness_statements
  - casefile.forensic_findings
  - casefile.interview_notes
custom_checks:
  - name: all four threads converge on the residents the generator chose
    query: SELECT count(*) FROM qa.integrity WHERE NOT holds
    value: 0
  - name: no case-file row contains any resident's full name
    query: |
      SELECT count(*) FROM (
        SELECT 1 FROM (
          SELECT statement AS body FROM casefile.witness_statements
          UNION ALL SELECT finding FROM casefile.forensic_findings
          UNION ALL SELECT note FROM casefile.interview_notes
        ) t
        JOIN town.citizens c
          ON contains(t.body, c.first_name || ' ' || c.last_name))
    value: 0
  - name: no case-file row contains a full registration mark
    query: |
      SELECT count(*) FROM (
        SELECT 1 FROM (
          SELECT statement AS body FROM casefile.witness_statements
          UNION ALL SELECT finding FROM casefile.forensic_findings
          UNION ALL SELECT note FROM casefile.interview_notes
        ) t
        JOIN town.vehicles v ON contains(t.body, v.plate))
    value: 0
  - name: no case-file row contains a subscriber number
    query: |
      SELECT count(*) FROM (
        SELECT 1 FROM (
          SELECT statement AS body FROM casefile.witness_statements
          UNION ALL SELECT finding FROM casefile.forensic_findings
          UNION ALL SELECT note FROM casefile.interview_notes
        ) t
        JOIN town.devices d ON contains(t.body, d.msisdn))
    value: 0
  - name: the badge decoy is cleared by a hospital stay spanning the rally
    query: |
      SELECT count(*) FROM town.clinic_visits v
      WHERE v.citizen_id = (
          SELECT b.citizen_id FROM town.building_access_events e
          JOIN town.building_readers r USING (reader_id)
          JOIN town.badges b USING (badge_id)
          WHERE r.building = 'Loma House' AND r.zone = 'stairwell'
            AND e.ts BETWEEN TIMESTAMP '2026-05-14 18:00:00' AND TIMESTAMP '2026-05-14 18:20:00')
        AND v.admitted < TIMESTAMP '2026-05-14 18:47:00'
        AND v.discharged > TIMESTAMP '2026-05-14 18:47:00'
    value: 1
  - name: the hospital-route driver decoy is cleared by an admission that evening
    query: |
      SELECT count(*) FROM qa.driver_stages d
      JOIN town.clinic_visits v ON v.citizen_id = d.owner_citizen_id
      WHERE d.in_d3 AND NOT d.in_d4
        AND v.admitted BETWEEN TIMESTAMP '2026-05-14 19:00:00' AND TIMESTAMP '2026-05-14 20:00:00'
    count: 1
@bruin */

WITH truth AS (SELECT * FROM _gen.actor_assignments),
combined AS (
SELECT * FROM (
    VALUES
        ('the one who fired',
         (SELECT citizen_id FROM qa.shooter_stages WHERE in_s8),
         (SELECT actor_a FROM truth)),
        ('the one who drove',
         (SELECT named_driver_citizen_id FROM qa.driver_stages WHERE in_d4),
         (SELECT actor_b FROM truth)),
        ('the one who arranged it',
         (SELECT d.citizen_id FROM qa.handler_stages h JOIN town.devices d ON d.device_id = h.subject WHERE h.stage = 'H3'),
         (SELECT actor_c FROM truth)),
        ('the partner',
         (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1),
         (SELECT actor_d FROM truth))
) AS t(part, found, expected)
UNION ALL
SELECT 'the registered keeper',
       (SELECT owner_citizen_id FROM qa.driver_stages WHERE in_d4),
       (SELECT actor_e FROM truth)
), resolved AS (
    SELECT part, found, expected, found IS NOT NULL AND found = expected AS holds FROM combined
)
SELECT * FROM resolved
