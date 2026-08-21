/* @bruin
name: casefile.interview_notes
description: |
  Notes from follow-up interviews in the fortnight after the shooting. These are
  the officers' summaries, not verbatim records, and no interviewee is named.
materialization:
  type: table
columns:
  - name: note_id
    type: varchar
    description: Note identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: interviewed_on
    type: date
    description: Date of the interview
  - name: interviewee_role
    type: varchar
    description: How the interviewee came to be relevant
  - name: note
    type: varchar
    description: The officer's summary
@bruin */

SELECT
    'IN-' || lpad(n::VARCHAR, 3, '0') AS note_id,
    interviewed_on,
    interviewee_role,
    note
FROM (VALUES
    ( 1, DATE '2026-05-15', 'council clerk',            'Confirms the deceased chaired the planning committee for the last four years. Where a committee vote tied, the chair decided it. The minutes are a public record and we have taken a copy.'),
    ( 2, DATE '2026-05-15', 'council clerk',            'Says the ravine rezoning was the most contested item of the spring. It had been brought forward twice before and pulled both times.'),
    ( 3, DATE '2026-05-16', 'rally organiser',          'Attendance was around eight hundred. The route and the platform position were published a fortnight in advance on the council website.'),
    ( 4, DATE '2026-05-16', 'rally organiser',          'No security sweep of the surrounding buildings was requested or carried out. There had been no threats.'),
    ( 5, DATE '2026-05-16', 'building manager',         'The Loma House is let to eleven tenants. Cleaning and maintenance are contracted out, not employed directly.'),
    ( 6, DATE '2026-05-16', 'building manager',         'Contractor staff are issued badges by their own employer, not by us. We do not hold a list of who those people are day to day.'),
    ( 7, DATE '2026-05-17', 'building manager',         'The roof door alarm has been faulty since February. It was reported and not fixed. Anyone who had been in the building recently would know.'),
    ( 8, DATE '2026-05-17', 'caretaker',                'Is in every weekday evening at about the same time to lock up. Saw nobody on the stairs that he did not expect. Did not go above the second floor.'),
    ( 9, DATE '2026-05-17', 'tenant, second floor',     'Was working late. Heard someone on the stairwell but assumed it was the caretaker. Did not look out.'),
    (10, DATE '2026-05-18', 'range secretary',          'The club has around sixteen hundred members. Most shoot short lanes at the two town ranges. Only the Bracondale site has butts beyond three hundred metres.'),
    (11, DATE '2026-05-18', 'range secretary',          'A small competition squad shoots the long butts. They are the only members who book those lanes regularly and their scores are recorded.'),
    (12, DATE '2026-05-18', 'range secretary',          'Members drift away all the time, usually gradually. Somebody stopping dead after years of weekly attendance would be unusual but not unheard of.'),
    (13, DATE '2026-05-19', 'firearms licensing',       'There are a little under a thousand live certificates in the town. Rifle certificates outnumber shotguns here because of the deer.'),
    (14, DATE '2026-05-19', 'firearms licensing',       'Holding a certificate for that chambering is entirely ordinary in Yorkville. It tells you almost nothing on its own.'),
    (15, DATE '2026-05-19', 'network operator',         'Handsets re-register roughly every two hours while they are being carried. For a public event we keep every fifteen minute registration for the whole evening.'),
    (16, DATE '2026-05-19', 'network operator',         'A handset lying still re-registers on a timer instead of on movement, so it reports far more often and always from the same site. A carried handset wanders across neighbouring sites.'),
    (17, DATE '2026-05-20', 'network operator',         'Prepaid handsets carry no subscriber record. Around fourteen hundred people in the town use one as their only phone. There is nothing irregular about it.'),
    (18, DATE '2026-05-20', 'network operator',         'The Loma House has an operator microcell on its own roof serving the north block. It is a separate site from the one covering the square.'),
    (19, DATE '2026-05-20', 'traffic office',           'Twenty-four cameras. Six of them cover the approaches to the square. A vehicle is only read when it passes one, so gaps in a route are normal.'),
    (20, DATE '2026-05-21', 'traffic office',           'The T letter series was issued here over about a decade, so it is common on older small cars. It narrows things far less than people expect.'),
    (21, DATE '2026-05-21', 'motor insurance',          'A policy can name an additional driver. Where the registered keeper holds no licence of their own, there will always be one.'),
    (22, DATE '2026-05-21', 'land registry',            'The ravine parcels are held by a company rather than an individual. The filing names a principal.'),
    (23, DATE '2026-05-22', 'land registry',            'A rejected rezoning leaves undeveloped land at undeveloped value. Consent would have multiplied it several times over.'),
    (24, DATE '2026-05-22', 'bank compliance',          'Around two hundred and thirty accounts in the town took in a sum over two thousand in the last ninety days that their ordinary pattern does not explain. Most have innocent explanations.'),
    (25, DATE '2026-05-23', 'deceased''s office',       'Says the deceased had received no threats and had made no changes to his routine. He had chaired that committee without incident for years.'),
    (26, DATE '2026-05-23', 'senior investigating officer', 'No arrest. The file is left open. My view is that this was arranged rather than personal, and that at least three people were involved besides whoever fired.')
) AS t(n, interviewed_on, interviewee_role, note)
