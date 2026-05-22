# Governance

Bruin Cloud scores every asset against a fixed set of **governance rules** and aggregates the results into a **quality score**. Those scores drive the [Risk Report](/cloud/insights#risk-report) and the **Governance** tab on every [asset detail page](/cloud/assets#governance).

The rules are the same for every team — there's no per-team toggle. The goal is a consistent baseline of what "well-documented, well-owned, well-checked" means across teams.

## The rules

Seven rules, each contributing a fixed maximum score. The asset's quality score is `sum(actual_scores) / sum(max_scores) * 100`.

| Rule | What it checks | Max score |
|---|---|---|
| **Has description** | Asset has a description of more than 3 characters | 10 |
| **Has owner** | An owner is defined on the asset | 10 |
| **Has columns defined** | At least one column is declared | 5 |
| **Columns have descriptions** | 90%+ of columns have descriptions | 5 |
| **Has custom check** | At least one custom check is defined | 5 |
| **Has primary key** | At least one column is marked as primary key | 5 |
| **Columns have quality checks** | 90%+ of columns have at least one check | 5 |

Two of the rules — "Columns have descriptions" and "Columns have quality checks" — give partial credit. The score scales linearly with the coverage ratio, capped at the max once you hit 90%.

## Quality score buckets

The final percentage maps to one of four buckets that you'll see in the UI:

| Bucket | Range | Colour |
|---|---|---|
| **Poor** | < 45% | Red |
| **Average** | 45 – 65% | Yellow |
| **Good** | 65 – 85% | Blue |
| **Excellent** | ≥ 85% | Green |

These are the colours you see on asset rows in the catalog, on the Risk Report donut, and on the per-asset Governance tab.

## Where scores show up

- **Asset → Governance tab.** A list of every rule for that asset with either a green checkmark (full score) or a progress bar showing actual vs max.
- **Asset catalog.** The **Quality score** filter and column on the [Assets page](/cloud/assets#filters) come from the rolled-up percentage.
- **Risk Report.** Aggregates orphan assets (no owner) and assets without descriptions, sorted by downstream impact. See [Risk Report](/cloud/insights#risk-report).
- **Pipeline detail.** Pipeline-level aggregates show up alongside other pipeline stats.

## Acting on a low score

The fastest way to improve a score is to edit the asset's YAML in Git:

- Add or expand the `description` field.
- Set `owner: { name: ..., email: ... }`.
- Declare `columns:` with names, types, and per-column `description` fields.
- Mark identifying columns with `primary_key: true`.
- Add at least one `custom_check:` on the asset and column-level `checks:` on individual columns.

Or let the agent do it for you — open an [AI agent](/cloud/ai-agents/configure) with Cloud CLI access and ask:

> Add descriptions and the most useful checks to `analytics.orders`.

The agent reads the schema, drafts the additions, and applies them as [asset suggestions](/cloud/assets#ai-suggestions) you can review.

## How rollups work

Each rule evaluates to a `Result` with an actual score and a max. The asset's quality score is the sum across rules. Bruin computes this on the fly every time an asset is rendered — there's no scheduled "score job," and there's no need to wait for re-evaluation after a YAML change.

Pipeline and organisation rollups are derived from the per-asset scores at render time too. The Risk Report depth-orders by downstream count so high-impact assets float to the top.

## Limitations

- **Rules are not configurable.** You can't disable a rule or change the weights per team. If you have specific compliance needs that the built-in rules don't capture, talk to your account manager.
- **Scores aren't versioned.** What you see is "right now"; there's no historical chart of score over time per asset. (The [Risk Report](/cloud/insights#risk-report) is the closest thing — it changes as you fix things.)
- **No per-rule notifications.** You can't get pinged when an asset's score drops; for alerting, use [pipeline notifications](/cloud/notifications) on failures.

## Related

- [Risk Report](/cloud/insights#risk-report) — aggregate view of governance gaps.
- [Assets → Governance tab](/cloud/assets#governance) — per-asset breakdown.
- [AI Suggestions](/cloud/assets#ai-suggestions) — let the agent draft the missing metadata.
- [Asset definition schema](/assets/definition-schema) — the `description`, `owner`, `columns`, and `primary_key` fields the rules check.
- [Custom quality checks](/quality/custom) — what powers "Has custom check" and "Columns have quality checks".
- [`bruin ai-enhance`](/commands/ai-enhance) — local CLI equivalent of cloud asset suggestions.
- [FAQ → Can I skip a single asset from scheduled runs?](/cloud/faq#can-i-skip-a-single-asset-from-scheduled-runs) — related patterns for asset-level overrides.
