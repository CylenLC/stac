"use strict";

const state = {
  route: location.hash.slice(1) || "overview",
  summary: null,
  products: [],
  assets: [],
  runs: [],
  tasks: [],
  arrays: [],
  resources: {},
  protocol: {},
  selectedAssetId: null,
  spatialFeatures: [],
  exploreMap: null,
  registryTable: "sources",
  pollTick: 0,
};

const main = document.querySelector("#mainContent");
const navItems = [...document.querySelectorAll(".nav-item")];

const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");

const formatBytes = (bytes) => {
  const value = Number(bytes || 0);
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index > 1 ? 1 : 0)} ${units[index]}`;
};

const formatDate = (value) => {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? String(value) : new Intl.DateTimeFormat("zh-CN", {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
  }).format(date);
};

const formatDuration = (seconds) => {
  if (seconds == null) return "—";
  const value = Math.max(0, Math.round(Number(seconds)));
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m ${value % 60}s`;
  return `${Math.floor(value / 3600)}h ${Math.floor(value % 3600 / 60)}m`;
};

const shortId = (value, length = 12) => value && value.length > length ? `${value.slice(0, length)}…` : value || "—";
const badge = (status) => `<span class="badge ${escapeHtml(status || "reserved")}">${escapeHtml(status || "reserved")}</span>`;

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {"Content-Type": "application/json"}, ...options});
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try { detail = (await response.json()).detail || detail; } catch (_) { /* no JSON body */ }
    throw new Error(detail);
  }
  return response.json();
}

function toast(message, type = "info") {
  const element = document.createElement("div");
  element.className = `toast ${type}`;
  element.textContent = message;
  document.querySelector("#toastRegion").append(element);
  setTimeout(() => element.remove(), 4200);
}

async function loadBaseData() {
  const [summary, products, assets, runs, tasks, arrays] = await Promise.all([
    api("/lake/summary"), api("/lake/products"), api("/lake/assets?limit=500"),
    api("/lake/runs?limit=200"), api("/stac/tasks"), api("/lake/arrays"),
  ]);
  Object.assign(state, {summary, products, assets: assets.items, runs: runs.items, tasks, arrays});
  document.querySelector("#apiPulse").className = "pulse ok";
  document.querySelector("#apiState").textContent = "API 已连接";
  document.querySelector("#protocolVersion").textContent = summary.protocol?.version || "0.1";
  updateTransferDock();
}

function pageHeader(eyebrow, title, description, meta = "") {
  return `<header class="page-header"><div><p class="eyebrow">${eyebrow}</p><h1>${title}</h1><p>${description}</p></div><div class="header-meta">${meta}</div></header>`;
}

function renderOverview() {
  const summary = state.summary;
  const counts = summary.registry_counts;
  const active = state.tasks.filter((task) => !["completed", "partial", "failed"].includes(task.status));
  const issues = summary.missing_assets + summary.unregistered_source_files + state.runs.filter((run) => ["failed", "partial"].includes(run.status)).length;
  const layers = summary.layer_stats;
  const maxBytes = Math.max(...layers.map((layer) => layer.byte_size), 1);
  main.innerHTML = pageHeader(
    "Lake Overview", "数据湖运行概览", "查看当前资产规模、协议层状态、下载活动和需要处理的问题。",
    `更新于 ${formatDate(summary.generated_at)}`,
  ) + `
    <section class="metric-grid">
      ${metric("本地资产", counts.assets, `${counts.products} 个产品 · ${counts.variables} 个变量`)}
      ${metric("已登记数据量", formatBytes(summary.asset_bytes), `${summary.available_assets} 个文件可用`)}
      ${metric("活动下载", active.length, active.length ? "任务仍在运行" : "当前没有后台下载")}
      ${metric("需要关注", issues, issues ? "存在失败运行或目录不一致" : "未发现目录一致性问题")}
    </section>
    <section class="content-grid">
      <div>
        <article class="panel">
          <header class="panel-header"><div><h2>协议层占用</h2><p>按 Earth Zarr Protocol 物理层统计</p></div><span class="mono muted">${formatBytes(layers.reduce((sum, layer) => sum + layer.byte_size, 0))}</span></header>
          <div class="panel-body layer-bars">
            ${layers.map((layer) => `<div class="layer-row"><strong>${layer.layer}</strong><div class="bar-track"><span style="width:${Math.max(1, layer.byte_size / maxBytes * 100)}%"></span></div><small>${formatBytes(layer.byte_size)} · ${layer.file_count}</small></div>`).join("")}
          </div>
        </article>
        <article class="panel">
          <header class="panel-header"><div><h2>最近运行</h2><p>持久化 processing_runs 记录</p></div><button class="quiet-button" data-route-target="downloads">查看全部</button></header>
          ${runsTable(state.runs.slice(0, 6), false)}
        </article>
      </div>
      <div>
        <article class="panel">
          <header class="panel-header"><div><h2>状态事实</h2><p>不使用合成健康分数</p></div></header>
          <div class="panel-body status-list">
            ${statusFact("最近成功下载", summary.last_successful_run ? formatDate(summary.last_successful_run) : "暂无成功记录", summary.last_successful_run ? "completed" : "reserved")}
            ${statusFact("登记文件缺失", `${summary.missing_assets} 个`, summary.missing_assets ? "failed" : "completed")}
            ${statusFact("未登记源文件", `${summary.unregistered_source_files} 个`, summary.unregistered_source_files ? "partial" : "completed")}
            ${statusFact("Zarr 数据仓", `${summary.array_store_count} 个`, summary.array_store_count ? "completed" : "reserved")}
            ${statusFact("协议版本", summary.protocol?.version || "未知", "completed")}
          </div>
        </article>
        <article class="panel">
          <header class="panel-header"><div><h2>当前数据范围</h2><p>来自语义注册表</p></div></header>
          <div class="panel-body status-list">
            ${state.products.map((product) => `<div class="status-item"><div><strong>${escapeHtml(product.product_name)}</strong><small>${escapeHtml(product.source_id)} · ${product.variable_count} variables · ${formatBytes(product.byte_size)}</small></div>${badge(product.processing_level)}</div>`).join("") || emptyInline("暂无产品")}
          </div>
        </article>
      </div>
    </section>`;
}

const metric = (label, value, foot) => `<article class="metric-card"><span class="metric-label">${label}</span><strong class="metric-value">${value}</strong><span class="metric-foot">${foot}</span></article>`;
const statusFact = (name, value, status) => `<div class="status-item"><div><strong>${name}</strong><small>${value}</small></div>${badge(status)}</div>`;
const emptyInline = (text) => `<p class="muted">${text}</p>`;

function renderCatalog() {
  main.innerHTML = pageHeader("Data Catalog", "数据目录", "以产品和语义元数据组织数据，物理文件路径只作为资产详情的一部分。", `${state.products.length} 个产品`) + `
    <div class="toolbar"><label class="search-field"><input id="catalogSearch" type="search" placeholder="筛选产品、Collection 或数据源"></label><select id="modalityFilter" class="quiet-button"><option value="">全部模态</option><option value="observation">Observation</option><option value="forcing">Forcing</option><option value="static">Static</option><option value="hydrology">Hydrology</option></select></div>
    <section class="product-grid" id="productGrid">${productCards(state.products)}</section>
    <article class="panel" style="margin-top:14px">
      <header class="panel-header"><div><h2>全部资产</h2><p>点击一行查看空间范围、血缘和校验信息</p></div><span class="mono muted">${state.assets.length} records</span></header>
      ${assetsTable(state.assets)}
    </article>`;
  document.querySelector("#catalogSearch").addEventListener("input", filterProducts);
  document.querySelector("#modalityFilter").addEventListener("change", filterProducts);
}

function productCards(products) {
  if (!products.length) return `<div class="panel">${emptyState("P", "还没有产品", "完成一次下载后，产品和变量会自动进入注册表。")}</div>`;
  return products.map((product) => `
    <article class="product-card" data-product="${escapeHtml(product.product_id)}">
      <span class="badge ${escapeHtml(product.modality)}">${escapeHtml(product.modality)}</span>
      <h3>${escapeHtml(product.title || product.product_name)}</h3>
      <p>${escapeHtml(product.description || `${product.source_id} / ${product.collection_id} · version ${product.product_version}`)}</p>
      <footer><span>资产<strong>${product.asset_count}</strong></span><span>变量<strong>${product.variable_count}</strong></span><span>容量<strong>${formatBytes(product.byte_size)}</strong></span></footer>
    </article>`).join("");
}

function filterProducts() {
  const query = document.querySelector("#catalogSearch").value.trim().toLowerCase();
  const modality = document.querySelector("#modalityFilter").value;
  const products = state.products.filter((product) => (!modality || product.modality === modality) && (!query || JSON.stringify(product).toLowerCase().includes(query)));
  document.querySelector("#productGrid").innerHTML = productCards(products);
}

function assetsTable(assets) {
  if (!assets.length) return emptyState("A", "还没有资产", "通过下载页面创建任务后，资产会在这里出现。", "section");
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>资产</th><th>产品 / 变量</th><th>获取时间</th><th>大小</th><th>状态</th><th>路径</th></tr></thead><tbody>
    ${assets.map((asset) => `<tr data-id="${asset.asset_id}"><td class="mono" title="${asset.asset_id}">${shortId(asset.source_item_id, 18)}</td><td><strong>${escapeHtml(asset.product_id)}</strong><br><span class="muted mono">${escapeHtml(asset.asset_key)}</span></td><td>${formatDate(asset.datetime)}</td><td class="mono">${formatBytes(asset.byte_size)}</td><td>${badge(asset.status)}</td><td class="truncate mono" title="${escapeHtml(asset.local_path)}">${escapeHtml(asset.local_path)}</td></tr>`).join("")}
  </tbody></table></div>`;
}

async function renderExplore() {
  const spatial = await api("/lake/spatial/assets");
  state.spatialFeatures = spatial.features || [];
  const available = state.spatialFeatures;
  if (!available.some((feature) => feature.properties.asset_id === state.selectedAssetId)) state.selectedAssetId = preferredSpatialFeature(available)?.properties.asset_id || null;
  const options = (name) => [...new Set(available.map((feature) => feature.properties[name]).filter(Boolean))].sort();
  main.innerHTML = pageHeader("Spatial Browser", "全球空间浏览", "在全球底图上筛选已注册资产，按需叠加缓存后的 GeoTIFF 预览。", `${available.length} 个空间资产`) + `
    <section class="spatial-toolbar" aria-label="空间资产筛选">
      <label class="search-field"><input id="spatialSearch" type="search" placeholder="搜索 Granule、产品或变量"></label>
      <label class="field"><span>产品</span><select id="spatialProduct"><option value="">全部产品</option>${options("product_id").map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}</select></label>
      <label class="field"><span>变量</span><select id="spatialVariable"><option value="">全部变量</option>${options("variable").map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}</select></label>
      <label class="field"><span>状态</span><select id="spatialStatus"><option value="">全部状态</option>${options("status").map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}</select></label>
      <label class="checkbox spatial-toggle"><input id="previewToggle" type="checkbox" checked> 显示选中栅格预览</label>
      <button id="fitAssets" class="quiet-button">适应资产范围</button>
    </section>
    <section class="world-map-layout">
      <aside class="map-sidebar"><header><h3>Assets</h3><p class="muted" id="spatialCount">${available.length} 个空间资产</p></header><div id="spatialAssetList" class="asset-list"></div></aside>
      <div class="world-map-stage"><div id="worldMap" aria-label="全球资产地图"></div><div class="map-note"><strong>空间浏览</strong><br><span class="muted">已下载 GeoTIFF 优先显示有效像元范围；无有效掩膜时回退栅格网格、STAC geometry 或 bbox。预览仅渲染当前选中资产，并复用服务器缓存。</span></div><div class="map-legend"><span><i class="footprint-key"></i>有效数据范围</span><span><i class="preview-key"></i>GeoTIFF 预览</span></div></div>
      <aside class="map-controls"><h3>图层</h3><label class="checkbox"><input id="footprintToggle" type="checkbox" checked> 资产范围</label><label class="checkbox"><input id="previewToggleSide" type="checkbox" checked> 栅格预览</label><hr><h3>选择</h3><div id="spatialSelection" class="muted">选择资产以查看元数据和预览。</div></aside>
    </section>`;
  bindExploreFilters();
  renderSpatialAssetList(available);
  initializeExploreMap(available);
}

function filteredSpatialFeatures() {
  const query = document.querySelector("#spatialSearch")?.value.trim().toLowerCase() || "";
  const product = document.querySelector("#spatialProduct")?.value || "";
  const variable = document.querySelector("#spatialVariable")?.value || "";
  const status = document.querySelector("#spatialStatus")?.value || "";
  return state.spatialFeatures.filter((feature) => {
    const props = feature.properties || {};
    return (!product || props.product_id === product) && (!variable || props.variable === variable)
      && (!status || props.status === status) && (!query || JSON.stringify(props).toLowerCase().includes(query));
  });
}

function preferredSpatialFeature(features) {
  return features.find((feature) => feature.properties.variable === "B04")
    || features.find((feature) => feature.properties.variable !== "Fmask")
    || features[0];
}

function renderSpatialAssetList(features) {
  const target = document.querySelector("#spatialAssetList");
  const count = document.querySelector("#spatialCount");
  if (!target || !count) return;
  count.textContent = `${features.length} 个空间资产`;
  target.innerHTML = features.length ? features.map((feature) => {
    const props = feature.properties;
    return `<button data-map-asset="${escapeHtml(props.asset_id)}" class="${props.asset_id === state.selectedAssetId ? "active" : ""}"><strong>${escapeHtml(props.source_item_id)} / ${escapeHtml(props.variable)}</strong><small>${escapeHtml(props.product_id)} · ${formatDate(props.datetime)}</small></button>`;
  }).join("") : emptyInline("没有符合条件的空间资产");
  target.querySelectorAll("[data-map-asset]").forEach((button) => button.addEventListener("click", () => selectMapAsset(button.dataset.mapAsset, true)));
}

function bindExploreFilters() {
  const apply = () => {
    const features = filteredSpatialFeatures();
    if (!features.some((feature) => feature.properties.asset_id === state.selectedAssetId)) state.selectedAssetId = preferredSpatialFeature(features)?.properties.asset_id || null;
    renderSpatialAssetList(features); updateExploreMap(features);
  };
  ["#spatialSearch", "#spatialProduct", "#spatialVariable", "#spatialStatus"].forEach((selector) => document.querySelector(selector).addEventListener(selector === "#spatialSearch" ? "input" : "change", apply));
  const togglePreview = (checked) => { document.querySelector("#previewToggle").checked = checked; document.querySelector("#previewToggleSide").checked = checked; updateExploreMap(filteredSpatialFeatures()); };
  document.querySelector("#previewToggle").addEventListener("change", (event) => togglePreview(event.target.checked));
  document.querySelector("#previewToggleSide").addEventListener("change", (event) => togglePreview(event.target.checked));
  document.querySelector("#footprintToggle").addEventListener("change", (event) => {
    const map = state.exploreMap; if (!map?.isStyleLoaded()) return;
    ["asset-footprints-fill", "asset-footprints-outline", "selected-asset-outline"].forEach((id) => map.setLayoutProperty(id, "visibility", event.target.checked ? "visible" : "none"));
  });
  document.querySelector("#fitAssets").addEventListener("click", () => fitExploreMap(filteredSpatialFeatures()));
}

function mapStyle() {
  return {version: 8, sources: {osm: {type: "raster", tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], tileSize: 256, attribution: "© OpenStreetMap contributors"}}, layers: [{id: "osm", type: "raster", source: "osm"}]};
}

function initializeExploreMap(features) {
  if (!window.maplibregl) {
    document.querySelector("#worldMap").innerHTML = emptyState("M", "地图组件未加载", "请检查网络后刷新页面；资产列表和元数据仍可使用。", "div");
    return;
  }
  const map = new window.maplibregl.Map({container: "worldMap", style: mapStyle(), center: [0, 18], zoom: 1.25, minZoom: 0});
  state.exploreMap = map;
  map.addControl(new window.maplibregl.NavigationControl({showCompass: true}), "top-right");
  map.addControl(new window.maplibregl.ScaleControl({maxWidth: 140, unit: "metric"}), "bottom-right");
  map.on("load", () => {
    map.addSource("assets", {type: "geojson", data: {type: "FeatureCollection", features: []}});
    map.addSource("selected-asset", {type: "geojson", data: {type: "FeatureCollection", features: []}});
    map.addLayer({id: "asset-footprints-fill", type: "fill", source: "assets", paint: {"fill-color": "#167d64", "fill-opacity": 0.08}});
    map.addLayer({id: "asset-footprints-outline", type: "line", source: "assets", paint: {"line-color": "#0d5c4a", "line-width": 1.5}});
    map.addLayer({id: "selected-asset-outline", type: "line", source: "selected-asset", paint: {"line-color": "#2563a9", "line-width": 3}});
    map.on("click", "asset-footprints-fill", (event) => {
      const feature = event.features?.[0];
      if (feature?.properties?.asset_id) selectMapAsset(feature.properties.asset_id, true);
    });
    map.on("mouseenter", "asset-footprints-fill", () => { map.getCanvas().style.cursor = "pointer"; });
    map.on("mouseleave", "asset-footprints-fill", () => { map.getCanvas().style.cursor = ""; });
    updateExploreMap(features, true);
  });
}

function geometryBounds(geometry) {
  const points = [];
  const visit = (value) => {
    if (Array.isArray(value) && typeof value[0] === "number" && typeof value[1] === "number") points.push(value);
    else if (Array.isArray(value)) value.forEach(visit);
  };
  visit(geometry?.coordinates);
  if (!points.length) return null;
  return points.reduce((bounds, [lng, lat]) => [[Math.min(bounds[0][0], lng), Math.min(bounds[0][1], lat)], [Math.max(bounds[1][0], lng), Math.max(bounds[1][1], lat)]], [[Infinity, Infinity], [-Infinity, -Infinity]]);
}

function fitExploreMap(features) {
  const map = state.exploreMap;
  const points = features.flatMap((feature) => geometryBounds(feature.geometry) || []);
  if (!map || !points.length) return;
  const bounds = points.reduce((result, point) => [[Math.min(result[0][0], point[0]), Math.min(result[0][1], point[1])], [Math.max(result[1][0], point[0]), Math.max(result[1][1], point[1])]], [[Infinity, Infinity], [-Infinity, -Infinity]]);
  map.fitBounds(bounds, {padding: 74, maxZoom: 9, duration: 500});
}

function updateExploreMap(features, fit = false) {
  const map = state.exploreMap;
  if (!map) return;
  if (!map.isStyleLoaded()) {
    map.once("idle", () => updateExploreMap(features, fit));
    return;
  }
  map.getSource("assets")?.setData({type: "FeatureCollection", features});
  const selected = features.find((feature) => feature.properties.asset_id === state.selectedAssetId);
  map.getSource("selected-asset")?.setData({type: "FeatureCollection", features: selected ? [selected] : []});
  updateSpatialSelection(selected);
  if (fit) fitExploreMap(features);
}

function removeMapPreviewLayer(map) {
  if (map.getLayer("raster-preview")) map.removeLayer("raster-preview");
  if (map.getSource("raster-preview")) map.removeSource("raster-preview");
  document.querySelector("#worldMap")?.removeAttribute("data-preview-status");
}

function renderMapPreviewLayer(feature) {
  const map = state.exploreMap;
  const mapElement = document.querySelector("#worldMap");
  if (!map || !mapElement) return;
  if (!map.isStyleLoaded()) {
    mapElement.dataset.previewStatus = "waiting-for-style";
    map.once("idle", () => renderMapPreviewLayer(feature));
    return;
  }
  removeMapPreviewLayer(map);
  const previewEnabled = document.querySelector("#previewToggle")?.checked;
  const coordinates = feature?.properties.preview_coordinates;
  if (!previewEnabled) { mapElement.dataset.previewStatus = "disabled"; return; }
  if (!feature?.properties.previewable) { mapElement.dataset.previewStatus = "unavailable"; return; }
  if (!Array.isArray(coordinates) || coordinates.length !== 4) { mapElement.dataset.previewStatus = "invalid-coordinates"; return; }
  try {
    mapElement.dataset.previewStatus = "loading";
    map.addSource("raster-preview", {type: "image", url: previewUrl(feature), coordinates});
    map.addLayer({
      id: "raster-preview",
      type: "raster",
      source: "raster-preview",
      paint: {"raster-opacity": 0.88, "raster-fade-duration": 0},
    }, "asset-footprints-outline");
    map.once("idle", () => { if (mapElement.dataset.previewStatus === "loading") mapElement.dataset.previewStatus = "ready"; });
  } catch (error) {
    mapElement.dataset.previewStatus = "error";
    console.error("Unable to render GeoTIFF preview on the map", error);
  }
}

function previewUrl(feature) {
  const props = feature.properties;
  const cacheKey = encodeURIComponent(props.preview_cache_key || props.asset_id);
  const relativeUrl = `/lake/previews/${encodeURIComponent(props.asset_id)}.png?max_size=1024&style=auto&v=${cacheKey}`;
  return new URL(relativeUrl, window.location.origin).toString();
}

function updateSpatialSelection(feature) {
  const target = document.querySelector("#spatialSelection");
  if (!target) return;
  if (!feature) { target.textContent = "选择资产以查看元数据和预览。"; renderMapPreviewLayer(null); return; }
  const props = feature.properties;
  const preview = props.previewable ? `<img class="asset-preview-image" src="${escapeHtml(previewUrl(feature))}" alt="${escapeHtml(props.variable)} 栅格预览">` : `<small>该资产没有可用的 GeoTIFF 预览。</small>`;
  target.innerHTML = `<strong>${escapeHtml(props.source_item_id)} / ${escapeHtml(props.variable)}</strong><small>${escapeHtml(props.product_id)} · ${formatDate(props.datetime)}</small>${preview}<button id="openSpatialAsset" class="quiet-button">查看资产详情</button>`;
  document.querySelector("#openSpatialAsset").addEventListener("click", () => openAsset(props.asset_id));
  renderMapPreviewLayer(feature);
}

function selectMapAsset(assetId, fit = false) {
  state.selectedAssetId = assetId;
  const features = filteredSpatialFeatures();
  renderSpatialAssetList(features); updateExploreMap(features, fit);
}

async function renderEntities() {
  if (!state.resources.entities) {
    const [entities, arrays] = await Promise.all([api("/lake/resources/entities"), api("/lake/resources/arrays")]);
    state.resources.entities = entities; state.resources.arrays = arrays;
  }
  const entityFiles = state.resources.entities.items.filter((item) => item.kind === "file");
  main.innerHTML = pageHeader("Typed Resources", "实体与数组", "查看 basin、station、river、patch 等表格/矢量实体，以及已物化的 Zarr 数据仓。", `${entityFiles.length} 个实体文件 · ${state.arrays.length} 个 Zarr`) + `
    <section class="content-grid">
      <article class="panel">
        <header class="panel-header"><div><h2>矢量与表格实体</h2><p>entities/ 下的协议实体</p></div>${badge(entityFiles.length ? "completed" : "reserved")}</header>
        ${entityFiles.length ? resourceTable(entityFiles) : emptyState("E", "实体层尚未物化", "当前目录结构已经初始化。生成 GeoParquet、Parquet 或 GeoJSON 实体后，可在这里显示 Schema、记录和空间范围。")}
      </article>
      <article class="panel">
        <header class="panel-header"><div><h2>Zarr 数组</h2><p>维度、chunk、dtype 和属性</p></div>${badge(state.arrays.length ? "completed" : "reserved")}</header>
        ${state.arrays.length ? arraysTable(state.arrays) : emptyState("Z", "数组层尚未物化", "当前下载只维护不可变 Source Layer，不会把 GeoTIFF 伪装为 Zarr。后续物化的数据仓会自动出现在此处。")}
      </article>
    </section>`;
}

function resourceTable(items) {
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>资源</th><th>类型</th><th>大小</th><th>更新时间</th></tr></thead><tbody>${items.map((item) => `<tr><td class="mono">${escapeHtml(item.path)}</td><td>${escapeHtml(item.suffix || item.kind)}</td><td>${formatBytes(item.byte_size)}</td><td>${formatDate(item.modified_at)}</td></tr>`).join("")}</tbody></table></div>`;
}

function arraysTable(items) {
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>Store</th><th>文件</th><th>大小</th><th>更新时间</th></tr></thead><tbody>${items.map((item) => `<tr><td class="mono">${escapeHtml(item.path)}</td><td>${item.file_count}</td><td>${formatBytes(item.byte_size)}</td><td>${formatDate(item.modified_at)}</td></tr>`).join("")}</tbody></table></div>`;
}

function renderDownloads() {
  const today = new Date();
  const start = new Date(today); start.setDate(start.getDate() - 7);
  main.innerHTML = pageHeader("Acquisition Control", "下载任务", "创建 STAC / NASA CMR 查询，监控实时任务，并查看持久化运行记录。", `${state.tasks.length} 个当前进程任务`) + `
    <section class="download-grid">
      <article class="panel">
        <header class="panel-header"><div><h2>新建采集</h2><p>搜索后直接写入 Earth Lake Source Layer</p></div></header>
        <form id="downloadForm" class="download-form">
          <label class="field"><span>数据源</span><select name="catalog"><option value="nasa">NASA Earthdata</option><option value="earth-search">Earth Search</option><option value="microsoft">Microsoft Planetary Computer</option></select></label>
          <label class="field"><span>Collection / 产品</span><input name="collections" value="HLSL30_V2.0" required></label>
          <label class="field"><span>开始日期</span><input type="date" name="start_date" value="${start.toISOString().slice(0,10)}" required></label>
          <label class="field"><span>结束日期</span><input type="date" name="end_date" value="${today.toISOString().slice(0,10)}" required></label>
          <label class="field"><span>最多条目</span><input type="number" name="max_items" min="1" max="500" value="3" required></label>
          <label class="checkbox"><input type="checkbox" name="only_main" checked>只下载代表性资产</label>
          <label class="field wide"><span>AOI (WKT)</span><textarea name="wkt" required>POLYGON((-125 24,-66 24,-66 49,-125 49,-125 24))</textarea></label>
          <div class="form-actions wide"><span class="form-hint">NASA 受保护数据需要提前配置 ~/.netrc。未知远端文件大小时，ETA 可能不可用。</span><button class="primary-button" type="submit">开始下载</button></div>
        </form>
      </article>
      <div>
        <article class="panel">
          <header class="panel-header"><div><h2>实时任务</h2><p>API 进程内的下载状态</p></div><span class="mono muted">auto refresh</span></header>
          <div id="tasksTable">${tasksTable(state.tasks)}</div>
        </article>
        <article class="panel">
          <header class="panel-header"><div><h2>运行历史</h2><p>registry/processing_runs.parquet</p></div></header>
          ${runsTable(state.runs, true)}
        </article>
      </div>
    </section>`;
  document.querySelector("#downloadForm").addEventListener("submit", submitDownload);
}

function tasksTable(tasks) {
  if (!tasks.length) return emptyState("T", "没有实时任务", "创建下载后，文件数、字节进度、ETA、跳过和失败信息会显示在这里。", "div");
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>任务</th><th>状态</th><th>进度</th><th>传输</th><th>当前文件 / 消息</th></tr></thead><tbody>${tasks.map((task) => `<tr><td class="mono" title="${task.task_id}">${shortId(task.task_id)}</td><td>${badge(task.status)}</td><td class="progress-cell"><span class="mono">${Number(task.progress || 0).toFixed(1)}% · ETA ${formatDuration(task.remaining_time)}</span><div class="bar-track"><span style="width:${task.progress || 0}%"></span></div></td><td><span class="mono">${task.completed_files || task.results.length + task.skipped.length}/${task.total_files || "?"}</span><br><span class="muted">${formatBytes(task.downloaded_bytes)} / ${task.total_bytes ? formatBytes(task.total_bytes) : "未知"}</span></td><td class="truncate" title="${escapeHtml(task.current_file || task.message)}"><strong>${escapeHtml(task.current_file || task.message)}</strong><br><span class="muted">${task.results.length + task.skipped.length} ok · ${task.failures.length} failed</span></td></tr>`).join("")}</tbody></table></div>`;
}

function runsTable(runs, detailed) {
  if (!runs.length) return emptyState("R", "还没有运行记录", "CLI 和 API 下载都会自动维护 processing_runs。", "div");
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>运行</th><th>状态</th><th>数据源</th><th>开始时间</th>${detailed ? "<th>输出</th>" : ""}</tr></thead><tbody>${runs.map((run) => `<tr><td class="mono" title="${run.run_id}">${shortId(run.run_id, 17)}</td><td>${badge(run.status)}</td><td>${escapeHtml(run.parameters?.catalog || "—")}</td><td>${formatDate(run.start_time)}</td>${detailed ? `<td>${run.output_asset_ids?.length || 0}</td>` : ""}</tr>`).join("")}</tbody></table></div>`;
}

async function submitDownload(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector("button[type=submit]");
  const data = new FormData(form);
  const body = {
    catalog: data.get("catalog"), collections: data.get("collections").split(",").map((value) => value.trim()).filter(Boolean),
    start_date: data.get("start_date"), end_date: data.get("end_date"), max_items: Number(data.get("max_items")), wkt: data.get("wkt"),
  };
  button.disabled = true; button.textContent = "正在创建…";
  try {
    const result = await api(`/stac/search_and_download?only_main=${data.get("only_main") === "on"}`, {method: "POST", body: JSON.stringify(body)});
    toast(`任务已创建：${shortId(result.task_id)}`);
    await refreshTasks(); renderDownloads();
  } catch (error) { toast(`创建失败：${error.message}`, "error"); }
  finally { button.disabled = false; button.textContent = "开始下载"; }
}

async function renderSystem() {
  if (!Object.keys(state.protocol).length) state.protocol = await api("/lake/protocol");
  const summary = state.summary;
  main.innerHTML = pageHeader("Protocol & System", "协议与系统", "检查协议版本、受控词表、存储层和 Parquet 注册表。", `root: ${escapeHtml(summary.root_name)}`) + `
    <section class="content-grid">
      <div>
        <article class="panel">
          <header class="panel-header"><div><h2>存储层</h2><p>目录用于协议分层，业务浏览请使用数据目录</p></div></header>
          <div class="table-wrap"><table class="data-table"><thead><tr><th>Layer</th><th>文件</th><th>目录</th><th>大小</th><th>更新时间</th></tr></thead><tbody>${summary.layer_stats.map((layer) => `<tr><td class="mono">${layer.layer}/</td><td>${layer.file_count}</td><td>${layer.directory_count}</td><td class="mono">${formatBytes(layer.byte_size)}</td><td>${formatDate(layer.modified_at)}</td></tr>`).join("")}</tbody></table></div>
        </article>
        <article class="panel">
          <header class="panel-header"><div><h2>注册表浏览</h2><p>白名单、分页读取 Parquet</p></div><div class="tabs">${["sources","products","variables","grids","assets","processing_runs"].map((name) => `<button class="tab ${state.registryTable === name ? "active" : ""}" data-registry="${name}">${name}</button>`).join("")}</div></header>
          <div id="registryTable"><div class="page-loading" style="min-height:220px"><span class="loader"></span></div></div>
        </article>
      </div>
      <article class="panel">
        <header class="panel-header"><div><h2>协议文档</h2><p>protocol/*.json</p></div>${badge("completed")}</header>
        <div class="panel-body"><pre class="json-view">${escapeHtml(JSON.stringify(state.protocol, null, 2))}</pre></div>
      </article>
    </section>`;
  document.querySelectorAll("[data-registry]").forEach((button) => button.addEventListener("click", async () => { state.registryTable = button.dataset.registry; await renderSystem(); }));
  loadRegistryTable();
}

async function loadRegistryTable() {
  try {
    const result = await api(`/lake/registries/${state.registryTable}?limit=100`);
    const columns = result.columns.slice(0, 7);
    document.querySelector("#registryTable").innerHTML = result.items.length ? `<div class="table-wrap"><table class="data-table"><thead><tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead><tbody>${result.items.map((row) => `<tr>${columns.map((column) => `<td class="truncate" title="${escapeHtml(typeof row[column] === "object" ? JSON.stringify(row[column]) : row[column])}">${escapeHtml(typeof row[column] === "object" ? JSON.stringify(row[column]) : row[column] ?? "—")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : emptyState("0", "注册表为空", `${state.registryTable}.parquet 已初始化，但当前没有记录。`, "div");
  } catch (error) { toast(`注册表读取失败：${error.message}`, "error"); }
}

function emptyState(mark, title, description, tag = "div") {
  return `<${tag} class="empty-state"><span class="empty-mark">${mark}</span><h3>${title}</h3><p>${description}</p></${tag}>`;
}

function openAsset(assetId) {
  const asset = state.assets.find((item) => item.asset_id === assetId);
  if (!asset) return;
  document.querySelector("#drawerEyebrow").textContent = "Asset Record";
  document.querySelector("#drawerTitle").textContent = `${asset.source_item_id} / ${asset.asset_key}`;
  document.querySelector("#drawerBody").innerHTML = `
    <section class="drawer-section"><h3>资产状态</h3>${badge(asset.status)}</section>
    <section class="drawer-section"><h3>核心元数据</h3><dl class="schema-grid">
      ${definition("Product", asset.product_id)}${definition("Variable", asset.asset_key)}${definition("Grid", asset.grid_id)}${definition("Datetime", formatDate(asset.datetime))}${definition("Size", formatBytes(asset.byte_size))}${definition("Media type", asset.media_type)}${definition("Run", asset.run_id)}
    </dl></section>
    <section class="drawer-section"><h3>协议位置</h3><p class="mono muted">${escapeHtml(asset.local_path)}</p></section>
    <section class="drawer-section"><h3>SHA-256</h3><p class="mono muted">${escapeHtml(asset.checksum_sha256)}</p></section>
    <section class="drawer-section"><h3>空间与栅格信息</h3><pre class="json-view">${escapeHtml(JSON.stringify({bbox: asset.bbox, geometry: asset.geometry, raster_metadata: asset.raster_metadata}, null, 2))}</pre></section>
    <section class="drawer-section"><h3>来源 URL</h3><p class="mono muted">${escapeHtml(asset.source_url)}</p></section>`;
  document.querySelector("#drawerBackdrop").classList.remove("hidden");
  document.querySelector("#detailDrawer").classList.remove("hidden");
}

async function openProduct(productId) {
  let product = state.products.find((item) => item.product_id === productId);
  if (!product) return;
  try {
    product = await api(`/lake/products/${encodeURIComponent(productId)}`);
    state.products = state.products.map((item) => item.product_id === productId ? product : item);
  } catch (error) {
    toast(`产品详情未刷新：${error.message}`, "error");
  }
  const documentation = (product.documentation_urls || [])
    .filter((url) => /^https?:\/\//i.test(url))
    .map((url) => `<li><a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(url)}</a></li>`)
    .join("") || "<li class=\"muted\">没有上游文档链接</li>";
  const variables = product.variables || [];
  document.querySelector("#drawerEyebrow").textContent = "Collection / Product";
  document.querySelector("#drawerTitle").textContent = product.title || product.product_name;
  document.querySelector("#drawerBody").innerHTML = `
    <section class="drawer-section"><p class="muted">${escapeHtml(product.description || "上游数据源未提供简介。")}</p></section>
    <section class="drawer-section"><h3>Collection 元数据</h3><dl class="schema-grid">
      ${definition("Collection", product.collection_id)}${definition("Provider", (product.providers || []).join(", "))}${definition("License", product.license)}${definition("Temporal start", product.temporal_start)}${definition("Temporal end", product.temporal_end)}${definition("Resolution", product.spatial_resolution_m ? `${product.spatial_resolution_m} m` : null)}${definition("Keywords", (product.keywords || []).join(", "))}
    </dl></section>
    <section class="drawer-section"><h3>文档</h3><ul class="documentation-list">${documentation}</ul></section>
    <section class="drawer-section"><h3>变量 (${variables.length})</h3>${variables.length ? `<div class="table-wrap"><table class="data-table"><thead><tr><th>名称</th><th>说明</th><th>单位</th><th>波长</th><th>NoData</th><th>质量标志</th></tr></thead><tbody>${variables.map((variable) => `<tr><td class="mono">${escapeHtml(variable.source_name)}</td><td>${escapeHtml(variable.long_name)}</td><td>${escapeHtml(variable.unit || "—")}</td><td>${variable.central_wavelength_nm ? `${variable.central_wavelength_nm} nm` : "—"}</td><td>${escapeHtml(variable.nodata || "—")}</td><td title="${escapeHtml(variable.quality_flag_definition || "")}">${variable.quality_flag_definition ? "bit mask" : "—"}</td></tr>`).join("")}</tbody></table></div>` : emptyInline("还没有已注册变量")}</section>`;
  document.querySelector("#drawerBackdrop").classList.remove("hidden");
  document.querySelector("#detailDrawer").classList.remove("hidden");
}

const definition = (name, value) => `<div class="definition"><dt>${name}</dt><dd>${escapeHtml(value ?? "—")}</dd></div>`;
function closeDrawer() { document.querySelector("#drawerBackdrop").classList.add("hidden"); document.querySelector("#detailDrawer").classList.add("hidden"); }

function bindAssetRows() {
  document.querySelectorAll("tr[data-id], path[data-id]").forEach((row) => row.addEventListener("click", () => openAsset(row.dataset.id)));
  document.querySelectorAll(".product-card").forEach((card) => card.addEventListener("click", () => {
    void openProduct(card.dataset.product);
  }));
}

function bindRouteTargets() {
  document.querySelectorAll("[data-route-target]").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.routeTarget)));
  bindAssetRows();
}

async function render() {
  if (state.exploreMap) {
    state.exploreMap.remove();
    state.exploreMap = null;
  }
  navItems.forEach((item) => item.classList.toggle("active", item.dataset.route === state.route));
  try {
    if (state.route === "overview") renderOverview();
    else if (state.route === "catalog") renderCatalog();
    else if (state.route === "explore") await renderExplore();
    else if (state.route === "entities") await renderEntities();
    else if (state.route === "downloads") renderDownloads();
    else if (state.route === "system") await renderSystem();
    else { state.route = "overview"; renderOverview(); }
    bindRouteTargets();
    main.focus({preventScroll: true});
  } catch (error) {
    main.innerHTML = emptyState("!", "页面加载失败", escapeHtml(error.message));
  }
}

function navigate(route) {
  state.route = route; location.hash = route; render();
}

async function refreshTasks() {
  try {
    const previousActive = state.tasks.some((task) => !["completed", "partial", "failed"].includes(task.status));
    const tasks = await api("/stac/tasks");
    const active = tasks.some((task) => !["completed", "partial", "failed"].includes(task.status));
    state.tasks = tasks;
    state.pollTick += 1;
    if (state.pollTick % 6 === 0 || (previousActive && !active)) {
      state.runs = (await api("/lake/runs?limit=200")).items;
    }
    updateTransferDock();
    if (state.route === "downloads") {
      const target = document.querySelector("#tasksTable"); if (target) target.innerHTML = tasksTable(tasks);
    }
  } catch (_) { /* health state is handled by explicit refresh */ }
}

function updateTransferDock() {
  const dock = document.querySelector("#transferDock");
  const active = state.tasks.find((task) => !["completed", "partial", "failed"].includes(task.status));
  dock.classList.toggle("hidden", !active);
  if (!active) return;
  document.querySelector("#dockTitle").textContent = `${active.status} · ${shortId(active.task_id)}`;
  document.querySelector("#dockMessage").textContent = active.current_file
    ? `${active.current_file} · ${formatBytes(active.downloaded_bytes)} / ${active.total_bytes ? formatBytes(active.total_bytes) : "未知"}`
    : active.message;
  document.querySelector("#dockPercent").textContent = `${Number(active.progress || 0).toFixed(1)}%`;
  document.querySelector("#dockProgress").style.width = `${active.progress || 0}%`;
}

navItems.forEach((item) => item.addEventListener("click", () => navigate(item.dataset.route)));
window.addEventListener("hashchange", () => { state.route = location.hash.slice(1) || "overview"; render(); });
document.querySelector("#refreshButton").addEventListener("click", async () => {
  try { await loadBaseData(); await render(); toast("数据已刷新"); }
  catch (error) { toast(`刷新失败：${error.message}`, "error"); }
});
document.querySelector("#drawerClose").addEventListener("click", closeDrawer);
document.querySelector("#drawerBackdrop").addEventListener("click", closeDrawer);
document.querySelector("#dockToggle").addEventListener("click", () => navigate("downloads"));
document.querySelector("#globalSearch").addEventListener("keydown", (event) => {
  if (event.key === "Enter") { navigate("catalog"); setTimeout(() => { const input = document.querySelector("#catalogSearch"); if (input) { input.value = event.target.value; filterProducts(); } }, 0); }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") { event.preventDefault(); document.querySelector("#globalSearch").focus(); }
  if (event.key === "Escape") closeDrawer();
});

(async function boot() {
  try { await loadBaseData(); await render(); setInterval(refreshTasks, 2500); }
  catch (error) {
    document.querySelector("#apiPulse").className = "pulse error";
    document.querySelector("#apiState").textContent = "API 连接失败";
    main.innerHTML = emptyState("!", "无法读取监控数据", `请确认 FastAPI 已启动。${escapeHtml(error.message)}`);
  }
})();
