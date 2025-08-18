# Destinations

Bruin supports all the data platforms mentioned in the Data Platforms section as destinations for your ingested data. This means you can configure Bruin to load data into these platforms using [Ingestr Assets](../assets/ingestr.md).


## Supported Platforms

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const platformsGroup = sidebarGroups.value.find(group => group.text === 'Data Platforms')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">


<ul>
<li v-for="platform in platformsGroup.items" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>
