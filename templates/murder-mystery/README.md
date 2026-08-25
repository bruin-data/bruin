# The Yorkville Case

A SQL detective game you play with Bruin.

`bruin run` builds a whole fictional town in DuckDB — 12,400 residents, their phones,
their cars, their bank accounts, **4.9 million cell tower registrations** — and one
unsolved assassination. Name the four people who did it.

Nobody gives you a suspect list. Every clue is a query.

---

## 1. Set it up

No credentials, no cloud account, no configuration.

```bash
bruin init murder-mystery yorkville-case
cd yorkville-case
bruin run .
```

About **10 seconds**. You get a 74 MB `yorkville.db`, written next to your `.bruin.yml`
at the repo root — one level up from the pipeline folder.

Every install builds the same town with the same four culprits, so you can compare
answers with someone else.

## 2. Read your briefing

| Read this | For |
|---|---|
| **`CASE_FILE.md`** | What happened, and what the officers logged |
| **`DATA_DICTIONARY.md`** | Every table and column. Skim it — don't guess the schema |
| `AGENTS.md` | Rules, if an AI assistant is running your queries |
| `PLAY_WITH_AN_AGENT.md` | Hand this to an agent and it sets everything up for you |

## 3. Start asking questions

One command does everything:

```bash
bruin query -c duckdb-yorkville -q "SELECT * FROM town.citizens LIMIT 5"
```

Begin with the case papers. They're small — read all of them.

```bash
# 38 witnesses, in their own words
bruin query -c duckdb-yorkville -q "SELECT witness_ref, location, statement FROM casefile.witness_statements"

# 14 laboratory and scene findings
bruin query -c duckdb-yorkville -q "SELECT discipline, finding FROM casefile.forensic_findings"

# 26 follow-up interviews — these tell you how the town works
bruin query -c duckdb-yorkville -q "SELECT interviewee_role, note FROM casefile.interview_notes"
```

Between them you'll learn roughly what the shooter looked like, what he fired, and what
was waiting for him outside. No names.

## 4. Understand the shape of it

The forensics give you a height band. Try it on its own:

```bash
bruin query -c duckdb-yorkville -q "
  SELECT count(*) FROM town.citizens
  WHERE sex = 'M' AND height_cm BETWEEN 186 AND 194"
```

**403.** And 940 people hold a live firearm certificate. And 1,800 use a prepaid phone.

That's the whole design: **no single filter isolates anyone.** Every incriminating
attribute has a crowd standing in it. You reach one person by intersecting several — and
by clearing the ones who look guilty and aren't.

## 5. Keep your notes as assets

This is why the game is a Bruin project and not a `.sql` file. You'll build candidate
sets you need again three steps later. Don't retype them — materialize them.

`assets/notebook/rally_window_plate_reads.sql` is a worked example. Copy its shape:

```bash
bruin run assets/notebook/my_candidates.sql
bruin query -c duckdb-yorkville -q "SELECT * FROM notebook.my_candidates"
```

Anything under `assets/notebook/` is yours, and a note can build on a note — Bruin works
out the order.

---

## Where to look for what

| Question | Tables |
|---|---|
| Who is this person? | `citizens`, `addresses`, `employment` |
| Where were they? | `device_pings`, `devices`, `cell_towers` |
| Who did they talk to? | `call_records`, `devices` |
| What did they drive? | `vehicles`, `vehicle_insurance`, `plate_reads`, `cameras` |
| What did they spend? | `card_transactions`, `bank_accounts`, `merchants` |
| Who got into a building? | `building_access_events`, `badges`, `building_readers` |
| Can they shoot? | `firearm_licences`, `range_visits` |
| Who benefits? | `property_records`, `council_decisions`, `businesses` |
| Where did they go after? | `travel_records`, `hotel_stays` |
| Alibis | `clinic_visits` |

All in the `town` schema. The case papers are in `casefile`.

## Good to know

- **The shot was fired at 18:47 on 2026-05-14.** Most of your filtering starts there.
- **Tower registrations are 15-minute buckets, and every phone shares the same grid.**
  Two phones in the same place at the same time share an exact `(cell_id, ts)` pair — so
  you can find out who was travelling with whom.
- Phones normally report every couple of hours. The six hours around the rally are
  recorded at full 15-minute resolution for every phone in town.
- All timestamps are plain local time. One zone, no daylight saving, no traps.
- Add `-o csv` or `-o json` to any query if the table output gets unwieldy.

## What a finished answer looks like

Four people. For each: a name, what they did, and the evidence chain behind it.

- the one who **fired the shot**
- the one who **drove**
- the one who **arranged and paid** for it
- the shooter's **partner**, who knew

## House rules

**Don't read `assets/seed/`.** Those files build the town. They hold no names and no
citizen IDs by design, but they'll spoil the *shape* of the case, which is most of the
fun. There's no answer key here and nothing will validate an accusation — the only thing
stopping you is this line.

**Playing with an AI assistant?** It works well: point it at `AGENTS.md`, which keeps it
running one step at a time instead of solving the case for you. An assistant that just
hands you four names has taken the game away.

## How long

**One to two hours** the first time. Four threads, and they cross — so if one stalls,
switch. Solving any one person gives you a way into the others.
