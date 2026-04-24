<template>
  <div class="code-viewer">
    <div class="cv-header" v-if="title">
      <span class="cv-title">{{ title }}</span>
    </div>
    <div class="cv-body">
      <div class="cv-sidebar">
        <div class="cv-tree">
          <template v-for="node in fileTree" :key="node.path">
            <TreeItem
              :node="node"
              :depth="0"
              :selected-path="selectedPath"
              :collapsed-folders="collapsedFolders"
              @select="selectFile"
            />
          </template>
        </div>
      </div>
      <div class="cv-content">
        <div class="cv-file-header">
          <span class="cv-file-path">{{ selectedFile?.path || '' }}</span>
          <button class="cv-copy-btn" @click="copyContent">
            <span v-if="copied">Copied!</span>
            <span v-else>Copy</span>
          </button>
        </div>
        <div class="cv-code-wrapper">
          <pre class="cv-pre"><code class="hljs" v-html="highlightedContent"></code></pre>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, h, defineComponent } from 'vue'
import hljs from 'highlight.js/lib/core'
import yaml from 'highlight.js/lib/languages/yaml'
import sql from 'highlight.js/lib/languages/sql'
import python from 'highlight.js/lib/languages/python'
import json from 'highlight.js/lib/languages/json'
import bash from 'highlight.js/lib/languages/bash'
import plaintext from 'highlight.js/lib/languages/plaintext'

hljs.registerLanguage('yaml', yaml)
hljs.registerLanguage('yml', yaml)
hljs.registerLanguage('sql', sql)
hljs.registerLanguage('python', python)
hljs.registerLanguage('py', python)
hljs.registerLanguage('json', json)
hljs.registerLanguage('bash', bash)
hljs.registerLanguage('shell', bash)
hljs.registerLanguage('sh', bash)
hljs.registerLanguage('plaintext', plaintext)
hljs.registerLanguage('text', plaintext)

const props = defineProps({
  files: {
    type: Array,
    required: true
  },
  title: {
    type: String,
    default: ''
  },
  collapsedFolders: {
    type: Array,
    default: () => []
  }
})

const selectedPath = ref('')
const copied = ref(false)

function buildTree(files) {
  const root = []
  const folderMap = {}

  const sortedFiles = [...files].sort((a, b) => {
    const aParts = a.path.split('/')
    const bParts = b.path.split('/')
    if (aParts.length !== bParts.length) return aParts.length - bParts.length
    return a.path.localeCompare(b.path)
  })

  for (const file of sortedFiles) {
    const parts = file.path.split('/')
    let currentLevel = root

    for (let i = 0; i < parts.length - 1; i++) {
      const folderPath = parts.slice(0, i + 1).join('/')
      if (!folderMap[folderPath]) {
        const folder = {
          name: parts[i],
          path: folderPath,
          type: 'folder',
          children: []
        }
        folderMap[folderPath] = folder
        currentLevel.push(folder)
      }
      currentLevel = folderMap[folderPath].children
    }

    currentLevel.push({
      name: parts[parts.length - 1],
      path: file.path,
      type: 'file',
      content: file.content,
      language: file.language
    })
  }

  return root
}

const fileTree = computed(() => buildTree(props.files))

const flatFiles = computed(() => {
  const result = []
  function collect(nodes) {
    for (const node of nodes) {
      if (node.type === 'file') result.push(node)
      if (node.children) collect(node.children)
    }
  }
  collect(fileTree.value)
  return result
})

const selectedFile = computed(() =>
  flatFiles.value.find(f => f.path === selectedPath.value)
)

function inferLanguage(path) {
  const ext = path.split('.').pop().toLowerCase()
  const map = {
    yml: 'yaml', yaml: 'yaml',
    sql: 'sql',
    py: 'python',
    json: 'json',
    sh: 'bash', bash: 'bash',
  }
  return map[ext] || 'plaintext'
}

const highlightedContent = computed(() => {
  const file = selectedFile.value
  if (!file) return ''
  const lang = file.language && hljs.getLanguage(file.language)
    ? file.language
    : inferLanguage(file.path)
  try {
    return hljs.highlight(file.content, { language: lang, ignoreIllegals: true }).value
  } catch {
    return hljs.highlight(file.content, { language: 'plaintext', ignoreIllegals: true }).value
  }
})

function selectFile(path) {
  selectedPath.value = path
}

async function copyContent() {
  if (!selectedFile.value) return
  try {
    await navigator.clipboard.writeText(selectedFile.value.content)
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  } catch {
    // Clipboard API unavailable (non-HTTPS or denied permission)
  }
}

onMounted(() => {
  if (flatFiles.value.length > 0) {
    selectedPath.value = flatFiles.value[0].path
  }
})

// Icon factory functions — must create fresh VNodes per render call
function makeChevronDown() {
  return h('svg', { width: 12, height: 12, viewBox: '0 0 12 12', fill: 'none' }, [
    h('path', { d: 'M3 4.5L6 7.5L9 4.5', stroke: 'currentColor', 'stroke-width': '1.5', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' })
  ])
}
function makeChevronRight() {
  return h('svg', { width: 12, height: 12, viewBox: '0 0 12 12', fill: 'none' }, [
    h('path', { d: 'M4.5 3L7.5 6L4.5 9', stroke: 'currentColor', 'stroke-width': '1.5', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' })
  ])
}
function makeFileIcon() {
  return h('svg', { width: 12, height: 12, viewBox: '0 0 12 12', fill: 'none' }, [
    h('path', { d: 'M7 1H3C2.44772 1 2 1.44772 2 2V10C2 10.5523 2.44772 11 3 11H9C9.55228 11 10 10.5523 10 10V4L7 1Z', stroke: 'currentColor', 'stroke-width': '1', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' }),
    h('path', { d: 'M7 1V4H10', stroke: 'currentColor', 'stroke-width': '1', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' })
  ])
}

const TreeItem = defineComponent({
  name: 'TreeItem',
  props: {
    node: { type: Object, required: true },
    depth: { type: Number, default: 0 },
    selectedPath: { type: String, default: '' },
    collapsedFolders: { type: Array, default: () => [] }
  },
  emits: ['select'],
  setup(props, { emit }) {
    const expanded = ref(
      !(props.node.type === 'folder' && props.collapsedFolders.includes(props.node.path))
    )

    return () => {
      const isFolder = props.node.type === 'folder'
      const isSelected = props.selectedPath === props.node.path

      const itemClasses = ['cv-tree-item']
      if (isSelected) itemClasses.push('is-selected')
      if (isFolder) itemClasses.push('is-folder')

      const icon = isFolder
        ? h('span', { class: 'cv-tree-icon' }, [expanded.value ? makeChevronDown() : makeChevronRight()])
        : h('span', { class: 'cv-tree-icon cv-file-icon' }, [makeFileIcon()])

      const label = h('span', { class: 'cv-tree-label' }, props.node.name)

      const item = h('div', {
        class: itemClasses.join(' '),
        style: { paddingLeft: `${props.depth * 16 + 12}px` },
        onClick: () => {
          if (isFolder) {
            expanded.value = !expanded.value
          } else {
            emit('select', props.node.path)
          }
        }
      }, [icon, label])

      const children = isFolder && expanded.value && props.node.children
        ? props.node.children.map(child =>
            h(TreeItem, {
              key: child.path,
              node: child,
              depth: props.depth + 1,
              selectedPath: props.selectedPath,
              collapsedFolders: props.collapsedFolders,
              onSelect: (path) => emit('select', path)
            })
          )
        : []

      return h('div', { class: 'cv-tree-node' }, [item, ...children])
    }
  }
})
</script>

<style>
.code-viewer {
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  overflow: hidden;
  margin: 16px 0;
  background: var(--vp-c-bg);
  font-family: var(--vp-font-family-base);
}

.cv-header {
  padding: 10px 16px;
  border-bottom: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
}

.cv-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--vp-c-text-1);
}

.cv-body {
  display: flex;
  min-height: 400px;
  max-height: 600px;
}

.cv-sidebar {
  width: 240px;
  min-width: 240px;
  border-right: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
  overflow-y: auto;
  padding: 8px 0;
}

.cv-tree-item {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 12px;
  cursor: pointer;
  font-size: 13px;
  color: var(--vp-c-text-2);
  line-height: 1.6;
  user-select: none;
  transition: background 0.15s, color 0.15s;
}

.cv-tree-item:hover {
  background: var(--vp-c-bg-elv);
  color: var(--vp-c-text-1);
}

.cv-tree-item.is-selected {
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  font-weight: 500;
}

.cv-tree-icon {
  display: flex;
  align-items: center;
  flex-shrink: 0;
  color: var(--vp-c-text-3);
}

.cv-tree-item.is-selected .cv-tree-icon {
  color: var(--vp-c-brand-1);
}

.cv-tree-label {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cv-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
  overflow: hidden;
}

.cv-file-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 16px;
  border-bottom: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
}

.cv-file-path {
  font-size: 12px;
  font-family: var(--vp-font-family-mono);
  color: var(--vp-c-text-2);
}

.cv-copy-btn {
  font-size: 12px;
  padding: 2px 10px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 4px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-2);
  cursor: pointer;
  transition: border-color 0.2s, color 0.2s;
}

.cv-copy-btn:hover {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
}

.cv-code-wrapper {
  flex: 1;
  overflow: auto;
  background: var(--vp-code-block-bg);
}

.cv-pre {
  margin: 0;
  padding: 16px;
  font-size: 13px;
  line-height: 1.6;
  font-family: var(--vp-font-family-mono);
  color: var(--vp-code-block-color, var(--vp-c-text-1));
  white-space: pre;
  tab-size: 2;
}

.cv-pre code {
  font-family: inherit;
  background: transparent;
  padding: 0;
}

.code-viewer .hljs {
  background: transparent;
  color: inherit;
  padding: 0;
}

.code-viewer .hljs-comment,
.code-viewer .hljs-quote {
  color: #8e908c;
  font-style: italic;
}
.code-viewer .hljs-keyword,
.code-viewer .hljs-selector-tag,
.code-viewer .hljs-section,
.code-viewer .hljs-doctag {
  color: #8959a8;
}
.code-viewer .hljs-string,
.code-viewer .hljs-regexp,
.code-viewer .hljs-addition {
  color: #4f8a10;
}
.code-viewer .hljs-number,
.code-viewer .hljs-literal,
.code-viewer .hljs-meta {
  color: #c18401;
}
.code-viewer .hljs-built_in,
.code-viewer .hljs-type,
.code-viewer .hljs-class .hljs-title {
  color: #0086b3;
}
.code-viewer .hljs-title,
.code-viewer .hljs-name,
.code-viewer .hljs-selector-id,
.code-viewer .hljs-selector-class {
  color: #4271ae;
}
.code-viewer .hljs-attr,
.code-viewer .hljs-attribute,
.code-viewer .hljs-variable,
.code-viewer .hljs-template-variable,
.code-viewer .hljs-tag {
  color: #c82829;
}
.code-viewer .hljs-deletion {
  color: #c82829;
}

.dark .code-viewer .hljs-comment,
.dark .code-viewer .hljs-quote {
  color: #8b949e;
}
.dark .code-viewer .hljs-keyword,
.dark .code-viewer .hljs-selector-tag,
.dark .code-viewer .hljs-section,
.dark .code-viewer .hljs-doctag {
  color: #c678dd;
}
.dark .code-viewer .hljs-string,
.dark .code-viewer .hljs-regexp,
.dark .code-viewer .hljs-addition {
  color: #98c379;
}
.dark .code-viewer .hljs-number,
.dark .code-viewer .hljs-literal,
.dark .code-viewer .hljs-meta {
  color: #d19a66;
}
.dark .code-viewer .hljs-built_in,
.dark .code-viewer .hljs-type,
.dark .code-viewer .hljs-class .hljs-title {
  color: #56b6c2;
}
.dark .code-viewer .hljs-title,
.dark .code-viewer .hljs-name,
.dark .code-viewer .hljs-selector-id,
.dark .code-viewer .hljs-selector-class {
  color: #61afef;
}
.dark .code-viewer .hljs-attr,
.dark .code-viewer .hljs-attribute,
.dark .code-viewer .hljs-variable,
.dark .code-viewer .hljs-template-variable,
.dark .code-viewer .hljs-tag {
  color: #e06c75;
}
.dark .code-viewer .hljs-deletion {
  color: #e06c75;
}
</style>
