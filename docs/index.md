---
outline: deep
---

# What is Bruin?

Bruin is a data framework that consists of a command-line application & VS Code extension. It allows you to
- ingest data with pre-built connectors / Python
- transform data using Python and SQL
- add quality checks to your pipelines

You can use Bruin to build your data pipelines inside your data warehouse. It is a single-binary, which allows you to run it anywhere, e,g. your computer, GitHub Actions, or in an EC2 instance.

You can get started with Bruin [via installing it](getting-started/introduction/installation.md) with a single command.

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const platformsGroup = sidebarGroups.value.find(group => group.text === 'Data Platforms')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">
<h2>Supported Platforms</h2>

Bruin supports many data platforms out-of-the-box and as a first-class citizen. Feel free to get started with your favorite platform:

<ul>
<li v-for="platform in platformsGroup.items" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>
