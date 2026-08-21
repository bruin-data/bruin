# Yorkville Police — Case File 26/0514

**Status:** open, no arrest
**Reporting officer:** Senior Investigating Officer, Yorkville Division

---

## The offence

At **18:47 on Thursday 14 May 2026**, **Adrien Volk**, mayor of Yorkville, was shot once
and killed while speaking at a re-election rally in **Wychwood Square**.

Roughly **800 people** were present. A single rifle round was fired from an elevated
position on the **north side of the square**. Nobody in the crowd saw the shooter.

## What the responding officers logged

- One shot. No second impact anywhere on the platform or the hoardings behind it.
- The victim was struck in the upper left chest and was pronounced dead at the scene.
- The **Loma House** stands on the north side of the square. Its roof was
  identified as the likely firing point on 16 May and examined on 17 May.
- The roof access door was not forced. Its alarm had been out of order since February.
- No weapon, no cartridge case and no fingerprints were recovered.
- **38 witness statements** were taken in the twelve hours after the shooting.
- **14 forensic findings** were filed by the scene examiners and the regional laboratory.
- **26 follow-up interviews** were carried out over the following fortnight.

## The case tables

Three tables hold the case papers. They are small enough to read in full.

| Table | Rows | What it holds |
|---|---|---|
| `casefile.witness_statements` | 38 | What people at the scene said they saw |
| `casefile.forensic_findings` | 14 | Laboratory and scene examination results |
| `casefile.interview_notes` | 26 | Officers' summaries of follow-up interviews |

Everything else — the civic register, the networks, the cameras, the banks, the land
registry — sits in the `town` schema. See `DATA_DICTIONARY.md`.

## Where to start

Read the witness statements. Then read the forensic findings. Between them they will
tell you what kind of person fired the shot and what kind of vehicle waited for him.
Neither will tell you a name; that is what the rest of the data is for.

```bash
bruin query -c duckdb-yorkville -q "SELECT witness_ref, location, statement FROM casefile.witness_statements"
bruin query -c duckdb-yorkville -q "SELECT discipline, finding FROM casefile.forensic_findings"
```

## What counts as solving it

Four people. For each of them: a name, what they did, and the chain of evidence that
puts them there.

- the one who fired the shot
- the one who drove
- the one who arranged and paid for it
- the shooter's partner, who knew

Nothing in this project will tell you whether you are right. Compare your answer with
somebody else who has played.
