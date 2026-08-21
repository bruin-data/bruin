/* @bruin
name: town.library_loans
description: |
  Loans from Yorkville's public library over the last year, including the ones
  still out.
materialization:
  type: table
depends:
  - _gen.citizen_base
columns:
  - name: loan_id
    type: varchar
    description: Loan identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Borrower
    checks:
      - name: not_null
  - name: title
    type: varchar
    description: Title borrowed
  - name: subject
    type: varchar
    description: Shelf the title sits on
  - name: borrowed_on
    type: date
    description: Date the loan was taken out
  - name: returned_on
    type: date
    description: Date the title came back, null if still out
@bruin */

WITH members AS (
    SELECT citizen_id, seq FROM _gen.citizen_base WHERE age_years >= 8
),
-- The field sports shelf is small and its borrowers are few, which is what makes
-- it tempting. None of them is in the ordinary draw below.
field_sports_readers AS (
    SELECT citizen_id, seq, rk FROM (
        SELECT m.citizen_id, m.seq,
               row_number() OVER (ORDER BY {{ rnd('m.seq', 7002) }}, m.citizen_id) AS rk
        FROM members m
        JOIN _gen.citizen_base b USING (citizen_id)
        WHERE b.age_years BETWEEN 22 AND 70
    ) WHERE rk <= 14
),
spine AS (
    SELECT m.citizen_id, m.seq, l.loan
    FROM members m
    CROSS JOIN generate_series(1, 12) l(loan)
    WHERE {{ rnd('m.seq * 100 + l.loan', 7001) }} < 0.42 / sqrt(l.loan)
),
titled AS (
    SELECT
        s.citizen_id,
        s.seq,
        s.loan,
        list_extract([0,1,2,3,4,5,6,7,8,9,10,11,16,17,18,19,20,21,22,23,24,25,26,27],
                     1 + {{ rnd_int('s.seq * 100 + s.loan', 7010, 0, 23) }}) AS title_idx,
        ({{ rally_date() }} - INTERVAL ({{ rnd_int('s.seq * 100 + s.loan', 7011, 1, 364) }}) DAY)::DATE AS borrowed_on
    FROM spine s
)
SELECT
    'LN-' || lpad(row_number() OVER (ORDER BY borrowed_on, citizen_id, title_idx, loan)::VARCHAR, 6, '0') AS loan_id,
    citizen_id,
    list_extract([
        'The Ravine in Winter', 'Yorkville: A Short History', 'Kitchen Garden Almanac',
        'Bridge Endings', 'The Davenport Road Murders', 'Ironwork of the Old Brickyards',
        'Rivers and Their Making', 'Small Boat Handling', 'Bread, Daily',
        'Birds of the Ravine', 'The Cabinetmaker''s Notebook', 'Winter Pruning',
        'Interior Ballistics: An Introduction', 'Rifle Marksmanship and the Long Shot',
        'Exterior Ballistics for Sporting Rifles', 'Reloading by the Book',
        'Watercolour Without Fear', 'The Walmer Road Letters', 'Coastal Walks',
        'Beekeeping Year One', 'A Field Guide to Fungi', 'Chess for the Impatient',
        'The Seaton Green Papers', 'Repairing Old Clocks', 'Preserving and Pickling',
        'Steam on the Northern Line', 'The Nordheimer Poems', 'Knots and Splices'
    ], title_idx + 1)                                                       AS title,
    CASE
        WHEN title_idx BETWEEN 12 AND 15 THEN 'field sports'
        WHEN title_idx IN (0, 1, 5, 22, 25) THEN 'local history'
        WHEN title_idx IN (4, 17, 21, 26)   THEN 'fiction and poetry'
        WHEN title_idx IN (2, 11, 19, 20, 24) THEN 'gardening and food'
        ELSE 'general'
    END                                                                     AS subject,
    borrowed_on,
    CASE
        WHEN {{ rnd('seq * 100 + loan', 7012) }} < 0.94
            THEN (borrowed_on + INTERVAL ({{ rnd_int('seq * 100 + loan', 7013, 3, 42) }}) DAY)::DATE
    END                                                                     AS returned_on
FROM (
    SELECT * FROM titled
    UNION ALL
    SELECT
        r.citizen_id,
        r.seq,
        90 + r.rk                                       AS loan,
        12 + (r.rk % 4)                                 AS title_idx,
        ({{ rally_date() }} - INTERVAL ({{ rnd_int('r.seq', 7020, 20, 340) }}) DAY)::DATE AS borrowed_on
    FROM field_sports_readers r
)
