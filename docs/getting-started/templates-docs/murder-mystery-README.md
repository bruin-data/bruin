# The Yorkville Case

A SQL detective game you play with Bruin.

`bruin run` seeds a whole fictional town into DuckDB — 12,400 residents, their phones,
their cars, their bank accounts, four and a half million cell tower registrations — and
one unsolved assassination. Your job is to name the four people who carried it out.

Nobody hands you a suspect list. Every clue is a query.

---

## Setup

You need Bruin and nothing else. No credentials, no cloud account, no configuration.

```bash
bruin init murder-mystery yorkville-case
cd yorkville-case
bruin run .
```

The run takes about ten seconds and writes a 74 MB DuckDB file called `yorkville.db`
next to your `.bruin.yml`. Every install produces the same town and the same four
culprits, so you and a friend can compare answers.

Then read **`CASE_FILE.md`**. That is your briefing.

## Playing

Everything you need is `bruin query`:

```bash
# the briefing, in the witnesses' own words
bruin query -c duckdb-yorkville -q "SELECT witness_ref, location, statement FROM casefile.witness_statements"

# what the laboratory found
bruin query -c duckdb-yorkville -q "SELECT discipline, finding FROM casefile.forensic_findings"

# how many residents match a description on its own
bruin query -c duckdb-yorkville -q "
  SELECT count(*) FROM town.citizens
  WHERE sex = 'M' AND height_cm BETWEEN 186 AND 194"
```

That last one returns 403. That is the shape of this case: **no single filter isolates
anybody**. Every incriminating attribute has a crowd standing in it. You get to one
person by intersecting several, and by ruling out the people who look guilty and are not.

`DATA_DICTIONARY.md` lists every table and column. Read it early — the case is not more
interesting for making you guess the schema.

## Keeping notes as assets

This is the part that matters, and it is why the game is a Bruin project rather than a
SQL file. You will build up candidate sets that you need again three steps later. Do not
keep re-running the query — materialize it.

`assets/notebook/rally_window_plate_reads.sql` is a worked example: every camera read
near the square while the rally was breaking up. Copy its shape.

```bash
# after adding a file under assets/notebook/
bruin run assets/notebook/my_candidates.sql
bruin query -c duckdb-yorkville -q "SELECT * FROM notebook.my_candidates"
```

Assets under `assets/notebook/` are yours. Add as many as you like. Bruin works out the
dependency order, so a note can build on a note.

## Playing with an AI assistant

The game works just as well if you direct an agent — Claude Code, Codex, Cursor — with
Bruin MCP configured, and let it run the queries for you.

`AGENTS.md` in this folder constrains the assistant so that it *assists* rather than
solves: it runs the step you ask for, reports what came back, and stops. If you ask it
to solve the case it will decline. That is deliberate. An assistant that hands you the
answer has taken the game away.

If you want to hand the whole thing to an agent from a standing start, give it
**`PLAY_WITH_AN_AGENT.md`**. That file walks it through installing Bruin, initializing
the case and building the town, then stops and waits for you to start asking questions.
You can pass it to an agent before you have Bruin installed at all.

## Two requests

**Do not read `assets/seed/`.** Those files build the town. They contain no names and no
citizen IDs — that was a design constraint — but they will spoil the *shape* of the case,
which is most of the fun. There is no answer key in this project and nothing here will
validate an accusation. The only thing stopping you is this paragraph.

**Do not look for a shortcut in the generator.** Recovering the culprits from the seed
logic is possible if you are determined; it is also unambiguously cheating, and it takes
longer than solving it properly.

## How long it takes

Somewhere between one and two hours if you have not seen it before. There are four
threads and they cross, so if you get stuck on one, switch. Solving any one member gives
you a way into the others.
