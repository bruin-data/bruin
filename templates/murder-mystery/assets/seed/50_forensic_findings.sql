/* @bruin
name: casefile.forensic_findings
description: |
  Findings filed by the scene examiners and the regional laboratory. Each finding
  narrows what kind of person and what kind of weapon, and nothing further.
materialization:
  type: table
columns:
  - name: finding_id
    type: varchar
    description: Finding identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: filed_on
    type: date
    description: Date the finding was filed
  - name: discipline
    type: varchar
    description: Which examiner filed it
  - name: finding
    type: varchar
    description: The finding as written
@bruin */

SELECT
    'FF-' || lpad(n::VARCHAR, 3, '0') AS finding_id,
    filed_on,
    discipline,
    finding
FROM (VALUES
    ( 1, DATE '2026-05-15', 'ballistics',   'One round recovered. Calibre 7.62x51. Full metal jacket, commercial sporting manufacture, widely sold in this region.'),
    ( 2, DATE '2026-05-15', 'ballistics',   'Rifling marks are consistent with a bolt-action hunting rifle. Twist rate and land count rule out a service pattern semi-automatic.'),
    ( 3, DATE '2026-05-16', 'ballistics',   'Single shot. No second impact anywhere on the platform or the hoardings behind it.'),
    ( 4, DATE '2026-05-16', 'trajectory',   'Entry and exit geometry place the firing point above and to the north, at an elevation of between eighteen and twenty-two metres.'),
    ( 5, DATE '2026-05-16', 'trajectory',   'Range from the firing point to the platform is a little over two hundred and ten metres.'),
    ( 6, DATE '2026-05-17', 'trajectory',   'Shot placement at this range and elevation, unsupported, is beyond a casual shooter. The examiner notes it as competent rather than exceptional.'),
    ( 7, DATE '2026-05-17', 'scene',        'The Corvid Building roof parapet shows fresh disturbance to the gravel over a span of about one metre.'),
    ( 8, DATE '2026-05-17', 'scene',        'A single boot impression in the gravel, partial but measurable. Continental size 46. Tread is a common industrial pattern.'),
    ( 9, DATE '2026-05-17', 'scene',        'Standing height at the parapet consistent with the impression and the sight line puts the shooter between 186 and 194 centimetres.'),
    (10, DATE '2026-05-18', 'scene',        'Two fibres recovered from the parapet edge. Dark navy, heavy cotton drill, consistent with workwear. No manufacturer identifiable.'),
    (11, DATE '2026-05-18', 'scene',        'No cartridge case recovered. No prints on the parapet, the roof door or the stairwell handrail.'),
    (12, DATE '2026-05-18', 'trajectory',   'The rifle was rested on the parapet with a soft pad. The gravel shows a rectangular compression, not a bipod.'),
    (13, DATE '2026-05-19', 'ballistics',   'Impact velocity indicates a barrel length in the sporting range. Not a short barrelled weapon.'),
    (14, DATE '2026-05-20', 'scene',        'The roof access door was not forced. It was opened from the stairwell side.')
) AS t(n, filed_on, discipline, finding)
