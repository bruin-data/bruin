<script setup>
import DefaultTheme from 'vitepress/theme'
import CopyPageButton from './CopyPageButton.vue'
import { useData, useRoute } from 'vitepress'
import { nextTick, ref, watch } from 'vue'

const { Layout } = DefaultTheme
const { frontmatter } = useData()
const route = useRoute()

// Tracks whether the next click should expand or collapse every section.
const allExpanded = ref(true)

// Identifies the current toggle run. A new click or page navigation bumps this,
// so any earlier run that is still awaiting a render bails out instead of
// clobbering the newer state or the label.
let runId = 0

function collapsibleItems() {
  return Array.from(
    document.querySelectorAll('.VPSidebar .VPSidebarItem.collapsible')
  )
}

// Wait for Vue to flush the re-render so the collapsed class reflects reality
// before the next pass reads it (otherwise a pass toggles items back).
function afterRender() {
  return nextTick().then(
    () => new Promise((resolve) => requestAnimationFrame(resolve))
  )
}

async function setAll(expand, myRun) {
  // Loop until stable: expanding a parent can reveal nested collapsible items,
  // and each pass must wait for the DOM to update before re-reading state.
  for (let pass = 0; pass < 20; pass++) {
    const targets = collapsibleItems().filter(
      (item) => item.classList.contains('collapsed') === expand
    )
    if (targets.length === 0) break
    for (const item of targets) {
      item.querySelector(':scope > .item > .caret')?.click()
    }
    await afterRender()
    // A newer toggle or a navigation happened while we waited — stop and let it win.
    if (myRun !== runId) return false
  }
  return true
}

async function toggleAll() {
  const expand = !allExpanded.value
  const myRun = ++runId
  const completed = await setAll(expand, myRun)
  if (completed && myRun === runId) {
    allExpanded.value = expand
  }
}

// Cancel any in-flight run and reset the label when navigating to a new page
// (the sidebar re-renders on navigation).
watch(() => route.path, () => {
  runId++
  allExpanded.value = true
})
</script>

<template>
  <Layout>
    <template #doc-before>
      <div class="copy-page-wrapper" v-if="frontmatter.layout !== 'home'">
        <CopyPageButton />
      </div>
    </template>
    <template #sidebar-nav-before>
      <button
        type="button"
        class="collapse-all-button"
        @click="toggleAll"
      >
        <span class="vpi-chevron-right collapse-all-icon" :class="{ expanded: allExpanded }" />
        {{ allExpanded ? 'Collapse all' : 'Expand all' }}
      </button>
    </template>
  </Layout>
</template>

<style>
.copy-page-wrapper {
  float: right;
  margin-top: 4px;
  position: relative;
  z-index: 10;
}

.collapse-all-button {
  display: flex;
  align-items: center;
  gap: 4px;
  width: 100%;
  margin-bottom: 8px;
  padding: 4px 0;
  font-size: 12px;
  font-weight: 500;
  color: var(--vp-c-text-2);
  transition: color 0.25s;
}

.collapse-all-button:hover {
  color: var(--vp-c-text-1);
}

.collapse-all-icon {
  width: 14px;
  height: 14px;
  transition: transform 0.25s;
}

.collapse-all-icon.expanded {
  transform: rotate(90deg);
}
</style>
