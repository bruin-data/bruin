---
outline: deep
---

# What is Bruin?

End-to-end data framework: data ingestion + transformations + quality.

If dbt, Airbyte, and Great Expectations had a lovechild, it would be Bruin.

<img alt="Bruin - Demo" src="/demo.gif" />

Bruin is packed with features:

- 📥 ingest from [70+ sources](ingestion/overview) with [ingestr](https://github.com/bruin-data/ingestr) / Python
- ✨ run SQL, Python & R transformations on [many platforms](#supported-platforms)
- 📐 table/view [materializations](assets/materialization.md), incremental tables
- 🏔️ [lakehouse support](getting-started/lakehouse.md) with Iceberg & Delta Lake
- 🐍 run Python in isolated environments using [uv](https://github.com/astral-sh/uv)
- 💅 built-in [data quality checks](quality/overview.md), [glossaries](getting-started/glossary.md), and [policies](getting-started/policies.md)
- 🔗 visualize dependencies with [lineage](commands/lineage.md)
- 🔍 compare tables across connections with [data-diff](commands/data-diff.md)
- 🚀 [Jinja templating](assets/templating/templating.md) to avoid repetition
- ⚡ [concurrent](getting-started/concurrency.md) pipeline execution
- ✅ validate pipelines end-to-end via dry-run
- 🤖 [MCP support](getting-started/bruin-mcp.md) for AI/LLM integration
- 📋 [project templates](getting-started/templates.md) for quick starts
- 👷 run on your local machine, an EC2 instance, or [GitHub Actions](cicd/github-action.md)
- 🔒 [secrets management](secrets/overview.md) via environment variables & vaults
- 💻 [VS Code extension](vscode-extension/overview.md) for a better developer experience
- 📦 [easy to install](getting-started/introduction/installation.md) and use

I know, that's a lot. Let's dive into the details.

You can get started with Bruin [via installing it](getting-started/introduction/installation.md) with a single command.

## Supported Platforms

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const connectionsGroup = sidebarGroups.value.find(group => group.text === 'Connections & Platforms')
const platformsGroup = connectionsGroup?.items?.find(item => item.text === 'Warehouses & Lakes')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">

Bruin supports many data platforms out-of-the-box and as a first-class citizen. Feel free to get started with your favorite platform:

<ul>
<li v-for="platform in platformsGroup.items.filter(p => p.link?.startsWith('/platforms/'))" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>
