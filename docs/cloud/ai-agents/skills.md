# Agent Skills

Skills teach a Bruin Cloud agent how to do a specific task in your repo — a
reusable set of instructions the agent loads only when it is relevant. A skill
is just a Markdown file that lives in your project repo, so it is versioned with
your code and shared by everyone who uses the agent on that repo.

Bruin ships a few built-in skills automatically (for example, querying Bruin
Cloud, or working with a dbt/MetricFlow project when it detects one). This page
is about adding your **own** skills.

## Where skills live

A skill is a directory under `.agents/skills/` in your project repo, containing
a single `SKILL.md` file:

```
your-repo/
├── .agents/
│   └── skills/
│       └── weekly-revenue-report/
│           └── SKILL.md
├── pipelines/
├── .bruin.yml
└── AGENTS.md
```

Commit `.agents/skills/` like any other file. When an agent runs on the repo,
it picks up every skill it finds there. Both the Codex and OpenCode agent
backends read the same folder, so you write a skill once.

## The `SKILL.md` format

Each `SKILL.md` starts with YAML frontmatter and is followed by the
instructions in Markdown:

```markdown
---
name: weekly-revenue-report
description: >
  Use this skill when the user asks for the weekly revenue report — building the
  numbers, formatting the summary, or scheduling it. Do NOT use for ad-hoc
  revenue questions that don't need the full report.
---

# Weekly Revenue Report

Steps to produce the report:

1. Query `analytics.revenue_daily` for the last 7 completed days.
2. Group by `country` and sum `revenue_usd`.
3. Format as a Markdown table, largest first.
4. Add a one-line week-over-week comparison at the top.
```

Two frontmatter fields matter:

| Field | Required | Purpose |
|-------|----------|---------|
| `name` | yes | Identifier for the skill. Match the folder name. |
| `description` | yes | **When** to use the skill. This is the only part the agent sees up front — it decides whether to load the full `SKILL.md` based on this text. |

The body below the frontmatter is the actual playbook: commands to run, rules to
follow, pitfalls to avoid. It is loaded on demand, so it can be as long and
detailed as the task needs without bloating every prompt.

### Writing a good `description`

The `description` is what makes a skill fire at the right time. Be explicit
about both when to use it and when not to:

- **Do:** "Use when the user wants to create a Google Slides deck from an
  existing template. Do NOT use for editing slides in place."
- **Avoid:** "Google Slides helper." (too vague — the agent can't tell when it
  applies)

## Referencing other files

Anything else the skill needs (SQL files, templates, scripts) can live in the
same skill directory and be referenced by relative path from `SKILL.md`:

```
.agents/skills/weekly-revenue-report/
├── SKILL.md
└── revenue_query.sql
```

## Skills managed in Bruin Cloud

You can also attach skills to an agent from the Bruin Cloud UI instead of
committing them to the repo. These are fetched at run time and materialized
alongside the repo skills, so a Cloud-managed skill and a committed skill behave
identically to the agent. Repo-committed skills are the right default when the
skill is tied to a specific project; Cloud-managed skills are useful when you
want the same skill across several projects without editing each repo.

If a Cloud-managed skill and a repo skill share the same `name`, the
Cloud-managed one wins — it is applied last and overrides the repo skill, so the
agent loads a single skill under that name. Give a skill a distinct `name` if
you want it to stand on its own rather than replace a repo skill.

## Tips

- One skill, one job. A focused skill with a sharp `description` beats a
  catch-all.
- Keep instructions imperative and concrete — the body is a runbook, not prose.
- Version them with the repo so changes are reviewed like code.
- Test by asking the agent to do the task and confirming it loads the skill.
