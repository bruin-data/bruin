---
outline: deep
---

# What is Bruin?

End-to-end data framework: data ingestion + transformations + quality. 

If dbt, Airbyte, and Great Expectations had a lovechild, it would be Bruin.

<img alt="Bruin - Demo" src="/demo.gif" />

Bruin is packed with features:
- 📥 ingest data with [ingestr](https://github.com/bruin-data/ingestr) / Python
- ✨ run SQL & Python transformations on [many platforms](#supported-platforms)
- 📐 table/view [materializations](assets/materialization.md), incremental tables
- 🐍 run Python in isolated environments using [uv](https://github.com/astral-sh/uv)
- 💅 built-in data quality checks
- 🚀 Jinja templating to avoid repetition
- ✅ validate pipelines end-to-end via dry-run
- 👷 run on your local machine, an EC2 instance, or [GitHub Actions](cicd/github-action.md)
- 🔒 secrets injection via environment variables
- [VS Code extension](vscode-extension/overview.md) for a better developer experience
- ⚡ written in Golang
- 📦 [easy to install](getting-started/introduction/installation.md) and use

I know, that's a lot. Let's dive into the details.

You can get started with Bruin [via installing it](getting-started/introduction/installation.md) with a single command.


## Supported Platforms

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const platformsGroup = sidebarGroups.value.find(group => group.text === 'Data Platforms')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">

Bruin supports many data platforms out-of-the-box and as a first-class citizen. Feel free to get started with your favorite platform:

<ul>
<li v-for="platform in platformsGroup.items" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>
