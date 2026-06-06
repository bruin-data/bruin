import { computed, createApp, inject, onMounted, provide, reactive, ref, watch } from "vue/dist/vue.esm-bundler.js";
import hljs from "highlight.js/lib/core";
import sql from "highlight.js/lib/languages/sql";
import python from "highlight.js/lib/languages/python";
import yaml from "highlight.js/lib/languages/yaml";

hljs.registerLanguage("sql", sql);
hljs.registerLanguage("python", python);
hljs.registerLanguage("yaml", yaml);

const rawData = JSON.parse(document.getElementById("bruin-docs-data")?.textContent || "{}");

/* ------------------------------------------------------------------ helpers */

function pipelineKey(pipeline, index) {
  return `${pipeline.definition_file?.path || index}::${pipeline.selected_variant || ""}::${pipeline.name || "pipeline"}`;
}

function assetKey(pipeline, asset) {
  return `${pipeline.__key}::${asset.name || asset.id || asset.definition_file?.path}`;
}

function normalize(value) {
  return String(value || "").toLowerCase();
}

function listValue(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function typeLabel(value) {
  return value ? String(value).replaceAll(".", " ") : "unknown";
}

function isDefaultTrue(value) {
  if (value === undefined || value === null || value === "") return true;
  if (typeof value === "boolean") return value;
  return String(value).toLowerCase() !== "false";
}

function isSeed(type) {
  return String(type || "").toLowerCase().includes("seed");
}

function materializationLabel(asset) {
  const m = asset?.materialization;
  if (m && typeof m === "object" && m.type) {
    const parts = [m.type];
    if (m.strategy) parts.push(m.strategy);
    return parts.join(" · ");
  }
  if (typeof m === "string" && m) return m;
  if (isSeed(asset?.type)) return "table";
  return "None";
}

function columnCheckCount(asset) {
  return (asset.columns || []).reduce((n, c) => n + (c.checks || []).length, 0);
}

function checkCount(asset) {
  return columnCheckCount(asset) + (asset.custom_checks || []).length;
}

function describeSchedule(schedule) {
  if (!schedule) return "";
  if (typeof schedule === "object") return schedule.template || "";
  return String(schedule);
}

function formatGeneratedAt(value) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

/* -------- type → colour mapping ---- */

const TYPE_COLORS = {
  bigquery: "#3b82f6",
  bq: "#3b82f6",
  snowflake: "#38bdf8",
  sf: "#38bdf8",
  duckdb: "#d4a017",
  postgres: "#3f6ea5",
  pg: "#3f6ea5",
  redshift: "#5b6ee8",
  athena: "#9a6ef0",
  databricks: "#ef5b3c",
  mssql: "#5b8def",
  synapse: "#5b8def",
  clickhouse: "#e0a030",
  trino: "#6d6df0",
  python: "#4b8bbe",
  ingestr: "#16a34a",
  s3: "#22a06b",
  gcs: "#2f7be0",
};

function platformOf(type) {
  return String(type || "").split(".")[0];
}

function categoryOf(type) {
  const t = String(type || "").toLowerCase();
  if (t.includes("python") || t === "python") return "python";
  if (t.includes("ingestr") || t.startsWith("ingestr")) return "ingest";
  if (t.includes("sql") || t.includes("query") || t.includes("seed")) return "sql";
  return "box";
}

function typeMeta(type) {
  const color = TYPE_COLORS[platformOf(type)?.toLowerCase()] || "#d23f3f";
  return { color, category: categoryOf(type), label: typeLabel(type) };
}

const EXT_LANGUAGES = { sql: "sql", py: "python", python: "python", yml: "yaml", yaml: "yaml" };

function codeLanguage(asset) {
  const path = asset?.executable_file?.path || asset?.definition_file?.path || "";
  const ext = path.includes(".") ? path.split(".").pop().toLowerCase() : "";
  if (EXT_LANGUAGES[ext]) return EXT_LANGUAGES[ext];
  const category = categoryOf(asset?.type);
  if (category === "python") return "python";
  if (category === "sql") return "sql";
  return null;
}

function escapeHTML(value) {
  return String(value).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
}

function highlight(content, language) {
  if (!content) return "";
  try {
    if (language && hljs.getLanguage(language)) {
      return hljs.highlight(String(content), { language, ignoreIllegal: true }).value;
    }
  } catch (_) {}
  return escapeHTML(content);
}

/* ----------------------------------------------------------- data shaping */

const pipelines = (rawData.pipelines || []).map((pipeline, index) => ({
  ...pipeline,
  __key: pipelineKey(pipeline, index),
  __index: index,
  assets: pipeline.assets || [],
}));

/* ----------------------------------------------------------- tree builders */

function assetLeaf(pipeline, asset) {
  return {
    type: "asset",
    name: asset.name,
    asset,
    key: assetKey(pipeline, asset),
    pipelineKey: pipeline.__key,
  };
}

function projectChildren(pipeline) {
  const roots = [];
  for (const asset of pipeline.assets) {
    const path = asset.definition_file?.path || asset.name || "";
    const folders = path.split(/[\\/]/).slice(0, -1);
    let level = roots;
    let keyPrefix = pipeline.__key;
    for (const folder of folders) {
      keyPrefix += "/" + folder;
      let node = level.find((n) => n.type === "folder" && n.name === folder);
      if (!node) {
        node = { type: "folder", name: folder, key: keyPrefix, children: [] };
        level.push(node);
      }
      level = node.children;
    }
    level.push(assetLeaf(pipeline, asset));
  }
  return roots;
}

function typeChildren(pipeline) {
  const groups = new Map();
  for (const asset of pipeline.assets) {
    const t = asset.type || "unknown";
    if (!groups.has(t)) {
      groups.set(t, {
        type: "folder",
        kind: "type",
        refType: t,
        name: typeLabel(t),
        key: `${pipeline.__key}::type::${t}`,
        children: [],
      });
    }
    groups.get(t).children.push(assetLeaf(pipeline, asset));
  }
  return [...groups.values()];
}

function sortNodes(nodes) {
  nodes.sort((a, b) => {
    const ra = a.type === "asset" ? 1 : 0;
    const rb = b.type === "asset" ? 1 : 0;
    if (ra !== rb) return ra - rb;
    return normalize(a.name).localeCompare(normalize(b.name));
  });
  for (const n of nodes) if (n.children) sortNodes(n.children);
}

function leafMatches(leaf, term) {
  const a = leaf.asset;
  return (
    normalize(a.name).includes(term) ||
    normalize(a.type).includes(term) ||
    normalize(a.description).includes(term) ||
    normalize(a.definition_file?.path).includes(term) ||
    listValue(a.tags).some((t) => normalize(t).includes(term)) ||
    (a.columns || []).some((c) => normalize(c.name).includes(term))
  );
}

function filterNode(node, term) {
  if (node.type === "asset") return leafMatches(node, term) ? node : null;
  const kids = (node.children || []).map((c) => filterNode(c, term)).filter(Boolean);
  if (!kids.length) return null;
  return { ...node, children: kids };
}

/* ============================ TypeIcon component ============================ */

const TypeIcon = {
  props: { type: { type: String, default: "" }, size: { type: Number, default: 30 } },
  setup(props) {
    const meta = computed(() => typeMeta(props.type));
    const inner = computed(() => Math.round(props.size * 0.6));
    return { meta, inner };
  },
  template: `
    <span class="type-icon" :style="{ width: size + 'px', height: size + 'px', '--ti-color': meta.color, '--ti-bg': meta.color + '1f' }">
      <svg :width="inner" :height="inner" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <template v-if="meta.category === 'sql'">
          <ellipse cx="12" cy="5" rx="8" ry="3"></ellipse>
          <path d="M4 5v14c0 1.66 3.58 3 8 3s8-1.34 8-3V5"></path>
          <path d="M4 12c0 1.66 3.58 3 8 3s8-1.34 8-3"></path>
        </template>
        <template v-else-if="meta.category === 'python'">
          <polyline points="16 18 22 12 16 6"></polyline>
          <polyline points="8 6 2 12 8 18"></polyline>
        </template>
        <template v-else-if="meta.category === 'ingest'">
          <path d="M12 3v12"></path>
          <polyline points="7 10 12 15 17 10"></polyline>
          <path d="M4 21h16"></path>
        </template>
        <template v-else>
          <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
          <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
        </template>
      </svg>
    </span>
  `,
};

/* ============================ TreeNode (recursive) ============================ */

const TreeNode = {
  name: "tree-node",
  props: { node: { type: Object, required: true }, depth: { type: Number, default: 0 } },
  setup() {
    const ctx = inject("treeCtx");
    return { ctx };
  },
  template: `
    <div class="tree-node">
      <button
        v-if="node.type !== 'asset'"
        class="tree-row"
        :class="{ root: node.type === 'pipeline' }"
        :style="{ paddingLeft: (8 + depth * 13) + 'px' }"
        type="button"
        @click="ctx.toggle(node.key)"
      >
        <svg class="tree-chevron" :class="{ open: ctx.isExpanded(node.key) }" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="m9 6 6 6-6 6"></path></svg>
        <svg v-if="node.type === 'pipeline'" class="tree-folder-ic" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h7l2 2h9v9a2 2 0 0 1-2 2H3z"></path></svg>
        <type-icon v-else-if="node.kind === 'type'" :type="node.refType" :size="18"></type-icon>
        <svg v-else class="tree-folder-ic" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h7l2 2h9v9a2 2 0 0 1-2 2H3z"></path></svg>
        <span class="tree-label">{{ node.name }}</span>
      </button>

      <button
        v-else
        class="tree-row asset"
        :class="{ active: ctx.isActive(node.key) }"
        :style="{ paddingLeft: (8 + depth * 13 + 16) + 'px' }"
        type="button"
        @click="ctx.selectLeaf(node)"
      >
        <type-icon :type="node.asset.type" :size="18"></type-icon>
        <span class="tree-label">{{ node.name }}</span>
      </button>

      <div v-if="node.children && ctx.isExpanded(node.key)">
        <tree-node v-for="child in node.children" :key="child.key" :node="child" :depth="depth + 1"></tree-node>
      </div>
    </div>
  `,
};

/* ============================ root app ============================ */

const app = createApp({
  setup() {
    const firstPipeline = pipelines[0];
    const firstAsset = firstPipeline?.assets?.[0];

    const selectedPipelineKey = ref(firstPipeline?.__key || "");
    const selectedAssetKey = ref(firstAsset ? assetKey(firstPipeline, firstAsset) : "");
    const search = ref("");
    const treeMode = ref("project");
    const showLineage = ref(false);
    const collapsedNodes = reactive(new Set());
    const collapsedSections = reactive(new Set());
    const theme = ref(initTheme());

    function initTheme() {
      try {
        const saved = localStorage.getItem("bruin-docs-theme");
        if (saved === "dark" || saved === "light") return saved;
      } catch (_) {}
      return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }

    watch(
      theme,
      (value) => {
        document.documentElement.setAttribute("data-theme", value);
        try {
          localStorage.setItem("bruin-docs-theme", value);
        } catch (_) {}
      },
      { immediate: true },
    );

    function toggleTheme() {
      theme.value = theme.value === "dark" ? "light" : "dark";
    }

    /* ---- tree ---- */

    const tree = computed(() => {
      const roots = pipelines.map((p) => {
        const children = treeMode.value === "type" ? typeChildren(p) : projectChildren(p);
        const node = {
          type: "pipeline",
          name: p.name + (p.selected_variant ? " · " + p.selected_variant : ""),
          key: p.__key,
          children,
        };
        sortNodes(node.children);
        return node;
      });
      const term = normalize(search.value);
      if (!term) return roots;
      return roots.map((r) => filterNode(r, term)).filter(Boolean);
    });

    provide("treeCtx", {
      isExpanded: (key) => (normalize(search.value) ? true : !collapsedNodes.has(key)),
      toggle: (key) => {
        if (collapsedNodes.has(key)) collapsedNodes.delete(key);
        else collapsedNodes.add(key);
      },
      isActive: (key) => selectedAssetKey.value === key,
      selectLeaf: (leaf) => {
        selectedPipelineKey.value = leaf.pipelineKey;
        selectedAssetKey.value = leaf.key;
      },
    });

    /* ---- selection ---- */

    const selectedPipeline = computed(
      () => pipelines.find((p) => p.__key === selectedPipelineKey.value) || pipelines[0],
    );

    const pipelineAssets = computed(() =>
      (selectedPipeline.value?.assets || []).map((a) => ({ ...a, __key: assetKey(selectedPipeline.value, a) })),
    );

    const assetByName = computed(() => {
      const map = new Map();
      for (const a of pipelineAssets.value) map.set(a.name, a);
      return map;
    });

    const selectedAsset = computed(() => {
      const found = pipelineAssets.value.find((a) => a.__key === selectedAssetKey.value);
      return found || pipelineAssets.value[0] || null;
    });

    const upstreams = computed(() => {
      const asset = selectedAsset.value;
      if (!asset) return [];
      return (asset.upstreams || []).map((u) => {
        const match = assetByName.value.get(u.value);
        return { ...u, asset: match, key: `${u.type || "asset"}:${u.value}` };
      });
    });

    const downstreams = computed(() => {
      const asset = selectedAsset.value;
      if (!asset) return [];
      return pipelineAssets.value.filter((c) =>
        (c.upstreams || []).some((u) => (u.type || "asset") === "asset" && u.value === asset.name),
      );
    });

    const detailRows = computed(() => {
      const a = selectedAsset.value;
      if (!a) return [];
      const rows = [];
      rows.push({ k: "Type", v: typeLabel(a.type) });
      rows.push({ k: "Pipeline", v: selectedPipeline.value?.name });
      rows.push({ k: "Materialized", v: materializationLabel(a) });
      const m = a.materialization;
      if (m && typeof m === "object") {
        if (m.incremental_key) rows.push({ k: "Incremental key", v: m.incremental_key, mono: true });
        if (m.partition_by) rows.push({ k: "Partition by", v: m.partition_by, mono: true });
        if (listValue(m.cluster_by).length) rows.push({ k: "Cluster by", v: m.cluster_by.join(", "), mono: true });
        if (m.time_granularity) rows.push({ k: "Time granularity", v: m.time_granularity });
      }
      if (a.connection) rows.push({ k: "Connection", v: a.connection });
      const owner = a.owner || selectedPipeline.value?.owner;
      if (owner) rows.push({ k: "Owner", v: owner });
      const schedule = describeSchedule(selectedPipeline.value?.schedule);
      if (schedule) rows.push({ k: "Schedule", v: schedule });
      return rows;
    });

    /* ---- lineage graph layout ---- */

    const lineage = computed(() => {
      const assets = selectedPipeline.value?.assets || [];
      const names = new Set(assets.map((a) => a.name));
      const parents = new Map();
      for (const a of assets) parents.set(a.name, []);
      for (const a of assets) {
        for (const u of a.upstreams || []) {
          if ((u.type || "asset") === "asset" && names.has(u.value) && u.value !== a.name) {
            parents.get(a.name).push(u.value);
          }
        }
      }

      const layer = new Map(assets.map((a) => [a.name, 0]));
      for (let i = 0; i < assets.length; i++) {
        let changed = false;
        for (const a of assets) {
          for (const p of parents.get(a.name)) {
            const candidate = layer.get(p) + 1;
            if (candidate > layer.get(a.name)) {
              layer.set(a.name, candidate);
              changed = true;
            }
          }
        }
        if (!changed) break;
      }

      const byLayer = new Map();
      for (const a of assets) {
        const l = layer.get(a.name);
        if (!byLayer.has(l)) byLayer.set(l, []);
        byLayer.get(l).push(a);
      }
      const layerKeys = [...byLayer.keys()].sort((x, y) => x - y);

      const NODE_W = 184;
      const NODE_H = 50;
      const GAP_X = 80;
      const GAP_Y = 22;
      const PAD = 28;
      const colStride = NODE_W + GAP_X;
      const rowStride = NODE_H + GAP_Y;
      const maxRows = Math.max(1, ...layerKeys.map((l) => byLayer.get(l).length));
      const height = PAD * 2 + maxRows * rowStride - GAP_Y;

      const pos = new Map();
      const nodes = [];
      layerKeys.forEach((l, colIndex) => {
        const col = byLayer.get(l).slice().sort((a, b) => normalize(a.name).localeCompare(normalize(b.name)));
        const colHeight = col.length * rowStride - GAP_Y;
        const offsetY = PAD + (height - PAD * 2 - colHeight) / 2;
        col.forEach((a, rowIndex) => {
          const x = PAD + colIndex * colStride;
          const y = offsetY + rowIndex * rowStride;
          pos.set(a.name, { x, y });
          nodes.push({
            name: a.name,
            type: a.type,
            color: typeMeta(a.type).color,
            x,
            y,
            w: NODE_W,
            h: NODE_H,
            active: selectedAsset.value && a.name === selectedAsset.value.name,
            key: assetKey(selectedPipeline.value, a),
          });
        });
      });

      const activeName = selectedAsset.value?.name;
      const edges = [];
      for (const a of assets) {
        for (const p of parents.get(a.name)) {
          const from = pos.get(p);
          const to = pos.get(a.name);
          if (!from || !to) continue;
          const x1 = from.x + NODE_W;
          const y1 = from.y + NODE_H / 2;
          const x2 = to.x;
          const y2 = to.y + NODE_H / 2;
          const mid = (x1 + x2) / 2;
          edges.push({
            key: `${p}->${a.name}`,
            d: `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`,
            hl: activeName && (p === activeName || a.name === activeName),
          });
        }
      }

      const width = PAD * 2 + (layerKeys.length || 1) * colStride - GAP_X;
      return { nodes, edges, width: Math.max(width, 320), height: Math.max(height, 200), empty: nodes.length === 0 };
    });

    /* ---- section + actions ---- */

    function isOpen(id) {
      return !collapsedSections.has(id);
    }

    function toggleSection(id) {
      if (collapsedSections.has(id)) collapsedSections.delete(id);
      else collapsedSections.add(id);
    }

    const highlightedCode = computed(() => {
      const asset = selectedAsset.value;
      const content = asset?.executable_file?.content || "";
      return highlight(content, codeLanguage(asset));
    });

    function highlightSQL(query) {
      return highlight(query, "sql");
    }

    const codeCopied = ref(false);
    const canCopy = typeof navigator !== "undefined" && !!navigator.clipboard;
    let copyTimer = null;

    function copyCode() {
      const content = selectedAsset.value?.executable_file?.content || "";
      if (!content || !navigator.clipboard) return;
      navigator.clipboard.writeText(content).then(() => {
        codeCopied.value = true;
        if (copyTimer) clearTimeout(copyTimer);
        copyTimer = setTimeout(() => {
          codeCopied.value = false;
        }, 1500);
      });
    }

    function selectAsset(asset) {
      selectedAssetKey.value = asset.__key || assetKey(selectedPipeline.value, asset);
    }

    function openAssetByName(name) {
      const match = assetByName.value.get(name);
      if (match) {
        selectAsset(match);
        showLineage.value = false;
      }
    }

    onMounted(() => {
      window.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && showLineage.value) showLineage.value = false;
      });
    });

    return {
      rawData,
      pipelines,
      search,
      treeMode,
      theme,
      showLineage,
      tree,
      selectedPipeline,
      selectedAsset,
      upstreams,
      downstreams,
      detailRows,
      lineage,
      typeLabel,
      describeSchedule,
      formatGeneratedAt,
      listValue,
      checkCount,
      columnCheckCount,
      isDefaultTrue,
      isOpen,
      toggleSection,
      toggleTheme,
      selectAsset,
      openAssetByName,
      codeCopied,
      canCopy,
      copyCode,
      highlightedCode,
      highlightSQL,
    };
  },
  template: `
    <div class="app">
      <header class="topbar">
        <div class="brand">
          <span class="brand-mark" aria-hidden="true">
            <svg width="28" height="28" viewBox="40 40 470 470" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M452.178 157.461C451.231 154.857 451.902 151.937 453.954 150.082L490.613 116.028C495.191 111.766 492.191 104.11 485.956 104.11H64.0437C57.8089 104.11 54.8098 111.766 59.3873 116.028L96.0068 150.043C98.0588 151.937 98.7294 154.857 97.7431 157.461L57.8483 240.29C56.5461 242.973 57.1775 246.169 59.3478 248.182L270.265 444.143C272.79 446.472 276.657 446.472 279.183 444.143L490.415 247.787C492.705 245.656 493.374 242.223 491.993 239.421L452.219 157.501L452.178 157.461ZM186.45 188.083L166.128 164.406C162.34 159.987 165.497 153.2 171.298 153.2H211.705C217.506 153.2 220.623 159.987 216.875 164.406L196.552 188.083C193.909 191.161 189.134 191.161 186.45 188.083ZM350.922 316.33H291.929C285.773 316.33 282.813 308.793 287.312 304.61L304.951 288.234H304.872L308.265 285.077C312.804 280.855 309.804 273.279 303.649 273.279H245.681C239.486 273.279 236.526 280.855 241.064 285.077L244.497 288.274H244.418L261.978 304.531C266.516 308.754 263.557 316.37 257.322 316.37H198.447C193.238 316.37 189.883 310.806 192.37 306.189L268.608 164.485C271.212 159.632 278.157 159.632 280.761 164.485L357.038 306.189C359.526 310.806 356.171 316.37 350.962 316.37L350.922 316.33ZM383.202 164.406L362.88 188.083C360.235 191.161 355.459 191.161 352.776 188.083L332.454 164.406C328.667 159.987 331.824 153.2 337.623 153.2H378.031C383.833 153.2 386.949 159.987 383.202 164.406Z" fill="#FF5569"></path>
            </svg>
          </span>
          <span class="brand-text">
            <span class="brand-title">{{ rawData.title || "Bruin Docs" }}</span>
            <span class="brand-sub" v-if="rawData.repository?.name">{{ rawData.repository.name }}</span>
            <span class="brand-sub" v-else>Pipeline documentation</span>
          </span>
        </div>

        <div class="topbar-spacer"></div>

        <span class="gen-time" v-if="rawData.generated_at">Generated {{ formatGeneratedAt(rawData.generated_at) }}</span>

        <button class="icon-btn" type="button" @click="toggleTheme" :title="theme === 'dark' ? 'Switch to light' : 'Switch to dark'" aria-label="Toggle theme">
          <svg v-if="theme === 'dark'" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="4"></circle>
            <path d="M12 2v2M12 20v2M2 12h2M20 12h2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M19.1 4.9l-1.4 1.4M6.3 17.7l-1.4 1.4"></path>
          </svg>
          <svg v-else width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"></path>
          </svg>
        </button>
      </header>

      <div class="body">
        <!-- ======================= resource tree ======================= -->
        <aside class="sidebar">
          <div class="sidebar-top">
            <div class="search">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="11" cy="11" r="7"></circle>
                <path d="m21 21-4.3-4.3"></path>
              </svg>
              <input v-model="search" type="search" placeholder="Search…" spellcheck="false" />
              <button v-if="search" class="search-clear" type="button" @click="search = ''" aria-label="Clear search">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"></path></svg>
              </button>
            </div>

            <div class="seg">
              <button :class="{ active: treeMode === 'project' }" type="button" @click="treeMode = 'project'">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h7l2 2h9v9a2 2 0 0 1-2 2H3z"></path></svg>
                Project
              </button>
              <button :class="{ active: treeMode === 'type' }" type="button" @click="treeMode = 'type'">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="4" rx="1"></rect><rect x="3" y="10" width="18" height="4" rx="1"></rect><rect x="3" y="16" width="18" height="4" rx="1"></rect></svg>
                By type
              </button>
            </div>
          </div>

          <div class="tree">
            <tree-node v-for="root in tree" :key="root.key" :node="root" :depth="0"></tree-node>
            <p v-if="!tree.length" class="empty">No assets match your search.</p>
          </div>
        </aside>

        <!-- ======================= main ======================= -->
        <main class="main">
          <div class="scroll-area" v-if="selectedAsset">
            <div class="page">
              <nav class="crumb">
                <span>{{ selectedPipeline?.name }}</span>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 6 6 6-6 6"></path></svg>
                <span>{{ selectedAsset.name }}</span>
              </nav>

              <div class="asset-title">
                <type-icon :type="selectedAsset.type" :size="44"></type-icon>
                <div>
                  <h1>{{ selectedAsset.name }}</h1>
                  <div class="asset-subtype">{{ typeLabel(selectedAsset.type) }}</div>
                </div>
              </div>

              <!-- Details -->
              <section class="dsection">
                <button class="dsection-head" type="button" @click="toggleSection('details')">
                  <svg class="chev" :class="{ collapsed: !isOpen('details') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Details
                </button>
                <div class="dsection-body" v-show="isOpen('details')">
                  <div class="detail-grid">
                    <div class="drow" v-for="row in detailRows" :key="row.k">
                      <div class="dk">{{ row.k }}</div>
                      <div class="dv" :class="{ mono: row.mono }">{{ row.v }}</div>
                    </div>
                    <div class="drow" v-if="listValue(selectedAsset.tags).length">
                      <div class="dk">Tags</div>
                      <div class="dv">
                        <div class="tag-row">
                          <span v-for="tag in selectedAsset.tags" :key="tag" class="tag">{{ tag }}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <!-- Description -->
              <section class="dsection" v-if="selectedAsset.description">
                <button class="dsection-head" type="button" @click="toggleSection('description')">
                  <svg class="chev" :class="{ collapsed: !isOpen('description') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Description
                </button>
                <div class="dsection-body" v-show="isOpen('description')">
                  <p class="description">{{ selectedAsset.description }}</p>
                </div>
              </section>

              <!-- Columns -->
              <section class="dsection" v-if="(selectedAsset.columns || []).length">
                <button class="dsection-head" type="button" @click="toggleSection('columns')">
                  <svg class="chev" :class="{ collapsed: !isOpen('columns') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Columns <span class="dcount">{{ selectedAsset.columns.length }}</span>
                </button>
                <div class="dsection-body" v-show="isOpen('columns')">
                  <div class="table-wrap">
                    <table>
                      <thead>
                        <tr><th>Column</th><th>Type</th><th>Description</th><th>Checks</th></tr>
                      </thead>
                      <tbody>
                        <tr v-for="col in selectedAsset.columns" :key="col.name">
                          <td>
                            <span class="col-name">
                              {{ col.name }}
                              <span v-if="col.primary_key" class="pk-badge">
                                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="15" r="4"></circle><path d="M10.85 12.15 19 4M18 5l2 2M15 8l2 2"></path></svg>
                                PK
                              </span>
                            </span>
                            <span v-if="!isDefaultTrue(col.nullable)" class="flag-badge" style="margin-top:5px">NOT NULL</span>
                          </td>
                          <td><span v-if="col.type" class="col-type">{{ col.type }}</span></td>
                          <td class="col-desc">{{ col.description || "—" }}</td>
                          <td>
                            <div class="checks-cell" v-if="(col.checks || []).length">
                              <span v-for="check in col.checks" :key="check.name" class="check-badge" :class="{ nonblocking: !isDefaultTrue(check.blocking) }">
                                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"></path></svg>
                                {{ check.name }}
                              </span>
                            </div>
                            <span v-else class="muted">—</span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </section>

              <!-- Custom checks -->
              <section class="dsection" v-if="(selectedAsset.custom_checks || []).length">
                <button class="dsection-head" type="button" @click="toggleSection('checks')">
                  <svg class="chev" :class="{ collapsed: !isOpen('checks') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Custom checks <span class="dcount">{{ selectedAsset.custom_checks.length }}</span>
                </button>
                <div class="dsection-body" v-show="isOpen('checks')">
                  <div class="check-list">
                    <div class="check-card" v-for="check in selectedAsset.custom_checks" :key="check.name || check.query">
                      <div class="check-card-head">
                        <span class="check-badge"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"></path></svg>check</span>
                        <strong>{{ check.name || "custom check" }}</strong>
                      </div>
                      <p v-if="check.description" class="muted" style="margin-bottom:9px">{{ check.description }}</p>
                      <pre class="code hljs"><code v-html="highlightSQL(check.query)"></code></pre>
                    </div>
                  </div>
                </div>
              </section>

              <!-- Referenced By -->
              <section class="dsection">
                <button class="dsection-head" type="button" @click="toggleSection('referenced')">
                  <svg class="chev" :class="{ collapsed: !isOpen('referenced') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Referenced by <span class="dcount">{{ downstreams.length }}</span>
                </button>
                <div class="dsection-body" v-show="isOpen('referenced')">
                  <div class="dep-list" v-if="downstreams.length">
                    <button v-for="d in downstreams" :key="d.__key" class="dep-node" type="button" @click="selectAsset(d)">
                      <type-icon :type="d.type" :size="26"></type-icon>
                      <span class="dep-node-body">
                        <span class="dep-node-name">{{ d.name }}</span>
                        <span class="dep-node-type">{{ typeLabel(d.type) }}</span>
                      </span>
                    </button>
                  </div>
                  <p v-else class="muted">No assets reference this one.</p>
                </div>
              </section>

              <!-- Depends On -->
              <section class="dsection">
                <button class="dsection-head" type="button" @click="toggleSection('depends')">
                  <svg class="chev" :class="{ collapsed: !isOpen('depends') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Depends on <span class="dcount">{{ upstreams.length }}</span>
                </button>
                <div class="dsection-body" v-show="isOpen('depends')">
                  <div class="dep-list" v-if="upstreams.length">
                    <button v-for="u in upstreams" :key="u.key" class="dep-node" type="button" :disabled="!u.asset" @click="openAssetByName(u.value)">
                      <type-icon v-if="u.asset" :type="u.asset.type" :size="26"></type-icon>
                      <span v-else class="type-icon" style="width:26px;height:26px;--ti-color:#858d99;--ti-bg:#858d9920">
                        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h18M12 3v18"></path></svg>
                      </span>
                      <span class="dep-node-body">
                        <span class="dep-node-name">{{ u.value }}</span>
                        <span class="dep-node-type">{{ u.asset ? typeLabel(u.asset.type) : (u.type || "external") }}</span>
                      </span>
                      <span v-if="!u.asset" class="dep-external">ext</span>
                    </button>
                  </div>
                  <p v-else class="muted">No upstream dependencies.</p>
                </div>
              </section>

              <!-- Code -->
              <section class="dsection" v-if="selectedAsset.executable_file?.content">
                <div class="dsection-head-row">
                  <button class="dsection-head" type="button" @click="toggleSection('code')">
                    <svg class="chev" :class="{ collapsed: !isOpen('code') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                    Code
                  </button>
                  <button v-if="canCopy" class="copy-btn" :class="{ copied: codeCopied }" type="button" @click="copyCode" v-show="isOpen('code')">
                    <svg v-if="codeCopied" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"></path></svg>
                    <svg v-else width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15V5a2 2 0 0 1 2-2h10"></path></svg>
                    {{ codeCopied ? "Copied" : "Copy" }}
                  </button>
                </div>
                <div class="dsection-body" v-show="isOpen('code')">
                  <pre class="code hljs"><code v-html="highlightedCode"></code></pre>
                </div>
              </section>

              <!-- Files -->
              <section class="dsection">
                <button class="dsection-head" type="button" @click="toggleSection('files')">
                  <svg class="chev" :class="{ collapsed: !isOpen('files') }" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"></path></svg>
                  Files
                </button>
                <div class="dsection-body" v-show="isOpen('files')">
                  <div class="kv-grid">
                    <div v-if="selectedAsset.definition_file?.path">
                      <div class="k">Definition</div>
                      <div class="v mono">{{ selectedAsset.definition_file.path }}</div>
                    </div>
                    <div v-if="selectedAsset.executable_file?.path">
                      <div class="k">Executable</div>
                      <div class="v mono">{{ selectedAsset.executable_file.path }}</div>
                    </div>
                  </div>
                </div>
              </section>
            </div>
          </div>

          <div v-else class="center-empty">
            <span class="big">No assets found</span>
            <p>This documentation has no assets to display.</p>
          </div>

          <!-- floating lineage graph button -->
          <button v-if="selectedAsset && !showLineage" class="lineage-fab" type="button" @click="showLineage = true">
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="6" r="2"></circle><circle cx="5" cy="18" r="2"></circle><circle cx="19" cy="12" r="2"></circle><path d="M7 6.5 17 11M7 17.5 17 13"></path></svg>
            Lineage graph
          </button>

          <!-- lineage overlay -->
          <div v-if="showLineage" class="lineage-overlay">
            <div class="lineage-toolbar">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="6" r="2"></circle><circle cx="5" cy="18" r="2"></circle><circle cx="19" cy="12" r="2"></circle><path d="M7 6.5 17 11M7 17.5 17 13"></path></svg>
              <span class="title">{{ selectedPipeline?.name }}</span>
              <span class="muted">· {{ lineage.nodes.length }} assets</span>
              <span class="muted" style="margin-left:auto">Click a node to open its docs</span>
              <button class="icon-btn" type="button" @click="showLineage = false" aria-label="Close lineage graph" style="margin-left:12px">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"></path></svg>
              </button>
            </div>
            <div class="lineage-canvas">
              <svg v-if="!lineage.empty" class="lineage-svg" :width="lineage.width" :height="lineage.height" :viewBox="'0 0 ' + lineage.width + ' ' + lineage.height">
                <path v-for="edge in lineage.edges" :key="edge.key" class="edge" :class="{ hl: edge.hl }" :d="edge.d"></path>
                <g v-for="node in lineage.nodes" :key="node.key" class="gnode" :class="{ active: node.active }" @click="openAssetByName(node.name)">
                  <rect class="gnode-box" :x="node.x" :y="node.y" :width="node.w" :height="node.h" rx="4"></rect>
                  <circle :cx="node.x + 15" :cy="node.y + 24" r="3.5" :fill="node.color"></circle>
                  <text class="gnode-label" :x="node.x + 27" :y="node.y + 21">{{ node.name.length > 19 ? node.name.slice(0, 18) + '…' : node.name }}</text>
                  <text class="gnode-type" :x="node.x + 27" :y="node.y + 36">{{ typeLabel(node.type) }}</text>
                </g>
              </svg>
              <div v-else class="center-empty">
                <span class="big">No lineage to show</span>
                <p>Assets in this pipeline have no dependencies between them.</p>
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  `,
});

app.component("type-icon", TypeIcon);
app.component("tree-node", TreeNode);
app.mount("#app");
