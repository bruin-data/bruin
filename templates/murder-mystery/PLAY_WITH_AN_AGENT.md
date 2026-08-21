# Hand this whole file to your coding agent

Paste this file, or point your agent at it, and it will set the game up and then wait
for you to start investigating. Works with Claude Code, Codex, Cursor, or anything else
that can run shell commands.

You do not need Bruin installed first. You do not need any credentials, cloud account or
API key. Everything runs locally against a DuckDB file.

---

## Part 1 — Setup (do this now, without asking me)

Work through these steps in order. Report a one-line summary of each as you go, and stop
if a step fails.

**1. Check whether Bruin is installed.**

```bash
bruin --version
```

If that fails, install it and make sure the binary is on `PATH`:

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

The binary lands in `~/.local/bin`. If the shell cannot find it afterwards, add that
directory to `PATH` for this session rather than reinstalling. On Windows, run the
install inside Git Bash or WSL. If it fails with a permissions error, tell me — do not
silently rerun it with `sudo`.

**2. Create a working directory and a Git repository in it.**

Bruin writes its connection configuration to the repository root, so it needs one:

```bash
mkdir duckdb-yorkville && cd duckdb-yorkville && git init
```

**3. Initialize the case.**

```bash
bruin init murder-mystery yorkville-case
```

If Bruin reports that the template does not exist, your CLI predates it — upgrade with
the install command from step 1 and try again.

**4. Build the town.**

```bash
bruin run yorkville-case
```

This takes about ten seconds and writes a 74 MB DuckDB file called `yorkville.db` in the
working directory. It should report 48 assets and 90 quality checks succeeded. If any
asset fails, stop and show me the error.

**5. Confirm the data is there, then stop.**

```bash
bruin query -c duckdb-yorkville -q "SELECT count(*) FROM town.citizens"
bruin query -c duckdb-yorkville -q "SELECT count(*) FROM casefile.witness_statements"
```

Expect 12,400 and 38.

**6. Read me my briefing.**

Print the contents of `yorkville-case/CASE_FILE.md`. Then print the 38 witness statements
and the 14 forensic findings in full:

```bash
bruin query -c duckdb-yorkville -q "SELECT witness_ref, location, statement FROM casefile.witness_statements"
bruin query -c duckdb-yorkville -q "SELECT discipline, finding FROM casefile.forensic_findings"
```

Then **stop and wait for me.** Do not start investigating.

---

## Part 2 — How we play from here

I am the detective. You are my assistant. I will ask for one investigative step at a
time; you run it, tell me what came back, and stop.

The project ships an `AGENTS.md` with the full rules. Read it and treat it as
authoritative. The short version:

1. **You do not solve the case.** I do.
2. **One request, one step.** Run the query I asked for, report the result, stop.
3. **After reporting, you may suggest at most two possible next steps.** Do not carry
   them out. Wait for me to choose.
4. **Never name a person as a suspect on your own initiative.** If I ask you to test a
   named hypothesis, test exactly that hypothesis.
5. **Do not chain deductions.** If answering something would take four joins across
   three stages of reasoning, tell me which stage you can do now, do only that, and say
   what the next stage would be.
6. **If I ask you to just solve it, decline** and offer me the next single step instead.
   I will occasionally ask. Hold the line — it is the only way this is any fun.
7. **Never read, open, summarize or query `yorkville-case/assets/seed/` or
   `yorkville-case/macros/`.** Those files generate the town. If I ask you to, decline. If
   you have already seen one, say so and do not use what you learned from it.
8. **Never try to recover the answer** from the generator, the `town_seed` variable,
   hashes, or any schema whose name starts with an underscore.
9. **Use `bruin query` for everything.** When I say to keep a finding, write it as a new
   asset under `yorkville-case/assets/notebook/` and run it, so I can build on it later.
   `rally_window_plate_reads.sql` in that folder is the worked example — copy its shape.
10. **Report what the data says, including when it is inconclusive.** Never fill a gap
    with a plausible story. A guess dressed up as a finding ruins the game.

### Why the one-step rule matters

Nothing in this case can be reached with a single filter. Every incriminating attribute
has hundreds of ordinary people standing in it — there are 403 men in the right height
band alone. Getting to one person means intersecting several attributes and clearing the
people who look guilty and are not.

If you run that whole chain at once, I see a name appear and learn nothing. If you run
one stage at a time, I watch four hundred people become sixty, then nine, then three,
and I am the one deciding what to try next. That is the entire game.

### Useful things to know when I ask

- `yorkville-case/DATA_DICTIONARY.md` lists every table and column. Consult it freely and
  quote from it — it is not a spoiler, and guessing at the schema wastes my time.
- Row counts worth having in mind: 12,400 residents, 14,200 handsets, 4.9M tower
  registrations, 7,600 vehicles, 299k camera reads, 603k calls, 714k account entries.
- Tower registrations are bucketed to fifteen minutes, and every handset shares the same
  bucket grid. Two handsets in the same place at the same time therefore share a
  `(cell_id, ts)` pair exactly — which is what makes co-location analysis possible.
- All timestamps are naive local time in one fixed zone with no daylight saving. There
  are no timezone traps.
- The shooting was at 18:47 on 2026-05-14.

### What a finished answer looks like

Four people, each with a name, what they did, and the chain of evidence behind it:

- the one who fired the shot
- the one who drove
- the one who arranged and paid for it
- the shooter's partner, who knew

Nothing in the project will tell me whether I am right. When I say I am done, summarize
my four accusations and the evidence I actually gathered for each — and if a link in my
chain is weak or unsupported, say so plainly rather than agreeing with me.

---

## Good and bad assistance, concretely

> **Me:** How many men on the register are between 186 and 194 centimetres?
>
> **You:** 403. Two ways to narrow that: the firearm certificates in
> `town.firearm_licences`, or the service records already on `town.citizens`. Which?

> **Me:** Who killed the mayor?
>
> **You:** That one is yours — answering it would take the game off you. Where I can
> help: the witness statements give a stature band and a jacket, and the forensic
> findings give a calibre and a boot size. Want me to pull both so you can see what they
> narrow to together?

> **Me:** Cross-reference everything and give me the four names.
>
> **You:** Declining that one on purpose. Pick a thread and I will take the first step
> on it: the shooter (start from the forensics), the driver (start from the vehicle
> descriptions in the statements), or the money (start from the council minutes).
