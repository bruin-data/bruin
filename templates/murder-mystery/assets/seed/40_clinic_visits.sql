/* @bruin
name: town.clinic_visits
description: |
  Attendances at Yorkville General and the two branch surgeries over the last year.
  An admission with no discharge date was still open when the extract was taken.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.access_scene
  - _gen.square_traffic
  - _gen.vehicle_plan
columns:
  - name: visit_id
    type: varchar
    description: Attendance identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Patient
    checks:
      - name: not_null
  - name: clinic_id
    type: varchar
    description: Site attended
  - name: admitted
    type: timestamp
    description: When the patient was seen
  - name: discharged
    type: timestamp
    description: When the patient left, null if still on the ward
  - name: presenting_note
    type: varchar
    description: Reason for attendance as recorded at the desk
  - name: outcome
    type: varchar
    description: treated and discharged, admitted, referred or did not attend
@bruin */

WITH patients AS (
    SELECT citizen_id, seq, age_years FROM _gen.citizen_base
),
spine AS (
    SELECT p.citizen_id, p.seq, p.age_years, v.visit
    FROM patients p
    CROSS JOIN generate_series(1, 6) v(visit)
    WHERE {{ rnd('p.seq * 10 + v.visit', 6901) }} <
        list_extract([0.74, 0.52, 0.36, 0.22, 0.12, 0.05], v.visit)
        * CASE WHEN p.age_years >= 65 THEN 1.35 WHEN p.age_years < 12 THEN 1.20 ELSE 1.0 END
),
drawn AS (
    SELECT
        s.citizen_id,
        s.seq,
        s.visit,
        ({{ rally_date() }} - INTERVAL ({{ rnd_int('s.seq * 10 + s.visit', 6910, 1, 364) }}) DAY
            + INTERVAL ({{ rnd_int('s.seq * 10 + s.visit', 6911, 8, 21) }}) HOUR
            + INTERVAL ({{ rnd_int('s.seq * 10 + s.visit', 6912, 0, 59) }}) MINUTE)::TIMESTAMP AS admitted,
        {{ weighted('s.seq * 10 + s.visit', 6913, [['CL-GENERAL', 58], ['CL-DAVENPORT', 80], ['CL-HILLCREST', 100]]) }} AS clinic_id,
        {{ pick('s.seq * 10 + s.visit', 6914, ['chest pain', 'persistent cough', 'sprained ankle', 'laceration to hand', 'migraine', 'abdominal pain', 'back pain', 'dizziness', 'rash', 'fracture, suspected', 'ear infection', 'shortness of breath', 'burn to forearm', 'eye injury', 'routine review', 'blood pressure check', 'wound dressing', 'vomiting', 'fall at home', 'dental abscess']) }} AS presenting_note,
        {{ weighted('s.seq * 10 + s.visit', 6915, [['treated and discharged', 74], ['referred', 87], ['admitted', 95], ['did not attend', 100]]) }} AS outcome,
        {{ rnd_int('s.seq * 10 + s.visit', 6916, 1, 96) }} AS stay_hours
    FROM spine s
),
-- Attendances the case timeline fixes: a contractor on the ward across the
-- rally, and the drivers who took someone to the apron that evening.
pinned AS (
    SELECT
        sc.citizen_id,
        TIMESTAMP '2026-05-13 07:40:00'         AS admitted,
        TIMESTAMP '2026-05-16 11:15:00'         AS discharged,
        'CL-GENERAL'                            AS clinic_id,
        'acute appendicitis'                    AS presenting_note,
        'admitted'                              AS outcome
    FROM _gen.access_scene sc
    WHERE sc.part = 'contractor'
    UNION ALL
    SELECT
        v.owner_citizen_id,
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 19 HOUR
            + INTERVAL ({{ rnd_int('v.n', 6920, 8, 46) }}) MINUTE),
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 22 HOUR
            + INTERVAL ({{ rnd_int('v.n', 6921, 0, 110) }}) MINUTE),
        'CL-GENERAL',
        'fall at home, head injury',
        'treated and discharged'
    FROM _gen.square_traffic t
    JOIN _gen.vehicle_plan v USING (plate)
    WHERE t.route = 'hospital' AND v.owner_citizen_id IS NOT NULL
)
SELECT
    'CV-' || lpad(row_number() OVER (ORDER BY admitted, citizen_id, clinic_id, presenting_note, outcome, discharged NULLS LAST)::VARCHAR, 6, '0') AS visit_id,
    citizen_id,
    clinic_id,
    admitted,
    discharged,
    presenting_note,
    outcome
FROM (
    SELECT
        citizen_id,
        clinic_id,
        admitted,
        CASE
            WHEN outcome = 'did not attend' THEN NULL
            WHEN outcome = 'admitted'       THEN (admitted + INTERVAL (stay_hours) HOUR)::TIMESTAMP
            ELSE (admitted + INTERVAL ({{ rnd_int('seq * 10 + visit', 6917, 20, 260) }}) MINUTE)::TIMESTAMP
        END AS discharged,
        presenting_note,
        outcome
    FROM drawn
    UNION ALL
    SELECT citizen_id, clinic_id, admitted, discharged, presenting_note, outcome FROM pinned
)
