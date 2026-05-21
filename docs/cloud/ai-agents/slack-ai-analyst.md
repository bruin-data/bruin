# Slack AI Analyst Tutorial

End-to-end walkthrough: build a stock-market analyst pipeline with the Bruin CLI, deploy it to Bruin Cloud, and expose it as a Slack agent your team can query from any channel.

**Audience** — data professionals deploying an AI analyst to Bruin Cloud and Slack.

**Prerequisites**

- [Bruin CLI](/getting-started/introduction/installation) installed and authenticated.
- Claude Code available for pipeline generation and `bruin ai enhance`.
- A Bruin Cloud account with access to Team settings and Projects.
- A Git repo containing your Bruin project.
- Slack workspace with bot credentials and channel access.

## 1. Initialise the Bruin project

Run `bruin init empty <pipeline-name>`. If the current folder is already git-initialised, this creates `<pipeline-name>` unless you pass `--in-place`. If the current folder is not a Git repo, Bruin creates a `bruin/` folder first and then creates the project and pipeline inside it.

See [Project](/core-concepts/project) for context on Bruin projects.

## 2. Build the pipeline

Use Claude to extract stock data from Yahoo Finance and Wikipedia. Build assets that clean and join the data into something useful for an analyst — daily price tables, market-cap rankings, revenue and free-cash-flow rollups, and so on.

## 3. Enhance metadata

Run `bruin ai enhance` across the assets — this adds descriptions, column metadata, quality checks, and lineage. See [`ai enhance`](/commands/ai-enhance) for flag options. Review the output before committing.

## 4. Add the repo to Bruin Cloud

- Open Bruin Cloud → **Team settings → Projects** and add the repo to your workspace. See [Projects](/cloud/projects).
- Enable the pipeline and trigger the first run. See [Pipelines](/cloud/pipelines).
- Confirm backfills and the daily schedule run as expected.

## 5. Create the AI agent

- Open **AI → Agents** and create a new agent. See [Configure Agents](/cloud/ai-agents/configure).
- Select the project (the repo you just added).
- Attach the [connection set](/cloud/connections#connection-sets-for-ai-agents) the agent should query against.
- Add the Slack integration and pick the target channel. See [Slack](/cloud/integrations/slack).
- Name the agent and save.

## 6. Add agent instructions

Create an `AGENTS.md` file in the project root with the pretext, context, rules, and instructions for the analyst. A good `AGENTS.md` should:

- Describe what the analyst is for, who uses it, and what kinds of questions to expect.
- Tell the agent which assets to prefer for which question types.
- Require `bruin query` for all data access, and use `--dry-run` while testing.
- List any business rules or definitions (revenue growth, free cash flow margin, ticker matching) that the agent needs to apply.

## 7. Test in Bruin Cloud

- Open the agent's chat in Bruin Cloud and ask a few questions.
- Confirm it can query the data and self-correct when its first query is wrong.

## 8. Test in Slack

- Mention the agent in a Slack channel and ask a stock-market question.
- Open the generated SQL to validate the logic.
- Request a PDF report and confirm it lands in the channel.

## Sample prompts

- *"Which companies had their free cash flow margin improve in the past 4 quarters but saw their stock price decrease more than 10% during the same period?"*
- *"Summarize the top 10 tickers by revenue growth and generate a PDF report."*

## Helpful links

- [Bruin installation](/getting-started/introduction/installation)
- [`bruin ai enhance`](/commands/ai-enhance)
- [Projects in Bruin Cloud](/cloud/projects)
- [Configure Agents](/cloud/ai-agents/configure)
- [Slack](/cloud/integrations/slack)
