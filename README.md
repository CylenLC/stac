# STAC & NASA CMR 地理空间数据下载服务

这是一个专业级的地理空间数据检索与下载平台。它整合了 **Microsoft Planetary Computer (MPC)**、**AWS Earth Search** 以及 **NASA CMR (Common Metadata Repository)**，旨在通过统一的 API 接口提供高效、可靠的数据获取服务。

## 🌟 核心特性

- **多源数据整合**：支持搜索并下载 Sentinel (2/3/5P), Landsat, MODIS 以及 NASA SWOT 等多种卫星数据。
- **专业级任务管理**：基于 FastAPI 的异步后台任务系统，支持大规模数据下载而不阻塞 API。
- **字节级进度监控**：具备精确到字节的下载进度追踪，支持实时显示下载速度和预计剩余时间（ETA）。
- **NASA SWOT 深度支持**：完美集成 NASA CMR 接口，支持 SWOT L2/L4 等复杂数据集的搜索与自动认证下载。
- **AI 智能集成**：内置 `stac_downloader` Skill，支持 AI Agent 直接执行检索和下载任务。

## 🛠️ 环境准备

### 1. 依赖安装
推荐使用 `uv` 或 `pip`：
```bash
uv sync
```
```bash
pip install fastapi uvicorn requests pystac-client planetary-computer pyarrow shapely tqdm
```

### 2. NASA 认证 (针对 SWOT 等数据)
如果需要下载受保护的 NASA 数据，请确保在用户主目录下配置 `~/.netrc` 文件：
```text
machine urs.earthdata.nasa.gov
    login 你的用户名
    password 你的密码
```

## 🚀 快速上手

### 1. 启动服务
运行 FastAPI 服务中心：
```bash
python stac_api.py
```
默认地址：`http://localhost:8000`。根路径是 Earth Lake 监控页面，访问 `/docs` 可查看交互式 API 文档。

### 2. 使用监控页面

浏览器打开 [http://localhost:8000](http://localhost:8000)，无需单独安装 Node.js 或运行前端构建命令。FastAPI 会直接托管 `frontend/` 下的页面。

监控页面提供：

- **概览**：资产、产品、变量、协议层容量、运行历史和目录一致性状态。
- **数据目录**：按产品浏览资产，查看相对路径、SHA-256、来源 URL、空间 Geometry 和血缘运行。
- **空间浏览**：根据 Registry 中的 STAC Geometry 绘制资产覆盖范围。
- **实体与数组**：发现 `entities/` 中的文件和 `arrays/` 中的 Zarr Store；尚未物化时显示明确的预留状态。
- **下载任务**：提交 STAC/NASA 查询，轮询显示实时进度、成功、跳过和失败项，并查看持久化运行历史。
- **协议与系统**：查看协议 JSON、各存储层以及分页读取 Parquet Registry。

页面读取的都是真实协议数据，不生成演示记录。当前的空间页面展示资产覆盖 Geometry，不直接渲染完整 GeoTIFF；Zarr 页面读取 Store 元数据，不将原始文件伪装为 Zarr。

### 3. 一键搜索与下载
调用 `/stac/search_and_download` 接口：
```bash
curl -X POST "http://localhost:8000/stac/search_and_download" \
     -H "Content-Type: application/json" \
     -d '{
       "wkt": "POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))",
       "collections": ["sentinel-2-l2a"],
       "catalog": "microsoft",
       "start_date": "2023-12-01",
       "end_date": "2023-12-05"
     }'
```

### 4. 实时进度监控
使用内置的监控客户端（支持进度条和 ETA 显示）：
```bash
python monitor_task.py <TASK_ID>
```

## 🤖 使用 Agent Skill 自动下载

仓库中的 Skill 位于 `.agents/skills/stac_downloader/`。在支持项目 Skill 和终端执行能力的 Agent 中，可以直接用自然语言要求它完成检索与下载，而不必手工调用 API 服务。

例如，向 Agent 提交以下任务：

```text
使用本项目的 stac_downloader skill 执行搜索和下载，不要只告诉我命令。

数据源：microsoft
Collection：sentinel-2-l2a
区域 WKT：POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))
开始日期：2023-12-01
结束日期：2023-12-05
最大结果数：10
下载范围：主要资产
输出目录：downloads/

完成后请列出成功下载的文件路径和失败项。
```

Agent 会按照 Skill 说明执行两步流程：

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py search \
  --catalog microsoft \
  --wkt "POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))" \
  --collections "sentinel-2-l2a" \
  --start "2023-12-01" \
  --end "2023-12-05" \
  --max 10 \
  --output results.json

uv run python .agents/skills/stac_downloader/scripts/stac_tool.py download \
  --input results.json \
  --catalog microsoft \
  --outdir downloads
```

默认下载预览或代表性波段；HLS L30 使用 `B04`、`B05`、`Fmask`，HLS S30 使用 `B04`、`B8A`、`Fmask`。如果需要下载结果中的所有资产，请在指令中明确写出“下载所有资产”；对应下载命令会增加 `--all` 参数。下载使用临时 `.part` 文件，只有完整传输后才会写入最终文件名。

NASA CMR 下载同样可以交给 Agent 执行，将数据源设置为 `nasa`。HLS v2.0 可以直接使用产品名称 `HLSL30_V2.0` 和 `HLSS30_V2.0`，工具会自动映射到 NASA CMR 的 `HLSL30` / `HLSS30` collection，并指定版本 `2.0`。下载 NASA 受保护文件前，需先配置上述 Earthdata 凭据。

Skill 是直接执行的 CLI 工作流，不提供后台任务进度查询。需要任务 ID、后台下载和进度监控时，请使用 FastAPI 服务的 `/stac/search_and_download` 接口。任务状态可能为 `completed`、`partial` 或 `failed`；`partial` 表示至少一个文件下载失败，详情可从任务状态中的 `failures` 字段取得。

## Earth Zarr Protocol 0.1

每次通过 API 或 CLI 下载时，程序都会自动初始化并维护 `downloads/` 下的 Earth Lake。当前阶段保存原始 Source Layer，并维护 STAC 和语义注册表；不会在下载时伪装生成 Zarr，后续物化任务再向 `arrays/` 写入连续数据立方体。

```text
downloads/
├── protocol/                 # 协议版本和受控词表
├── catalog/stac/             # 自包含 STAC Catalog/Collection/Item
├── registry/                 # sources/products/variables/grids/assets/runs Parquet
├── source/                   # 不可变原始下载资产
│   └── <catalog>/<collection>/<item_id>/
├── entities/                 # basin/station/river/patch 预留层
├── arrays/                   # Zarr 物化层，当前只初始化目录
├── virtual/                  # Kerchunk/VirtualiZarr 预留层
├── manifests/               # 训练样本清单预留层
└── cache/                   # 可重建缓存
```

每次下载自动维护：

- `registry/processing_runs.parquet`：运行参数、代码 commit、状态和输出资产。
- `registry/assets.parquet`：本地路径、来源 URL、大小、SHA-256、时空范围和 lineage。
- `sources/products/variables/grids.parquet`：数据源、产品、变量语义和原生网格。
- `catalog/stac/`：每个 source granule 对应一个 STAC Item，文件对应 STAC Asset。

下载过程还会自动维护三层元数据：

- **Collection 层**：从 STAC Collection 或 NASA CMR Collection 获取标题、简介、许可、提供方、时间范围、文档链接和关键词；HLS profile 仅在上游字段缺失时提供兜底值。
- **Product profile 层**：HLS L30/S30 profile 写入波段长名称、中心波长、带宽、单位、比例因子和质量变量语义。反射率使用无量纲单位 `1`，Fmask 没有物理单位。
- **Asset/grid 层**：每个 GeoTIFF 使用 Rasterio 读取原生 CRS/EPSG、affine transform、原点、像元大小、宽高、dtype、NoData 和原始 GDAL 标签；grid 按实际投影和 transform 注册，不再给所有 HLS tile 写入一个通用 EPSG。

对于下载功能加入前已经存在的资产，可执行一次回填：

```bash
uv run python reindex_lake.py downloads
```

该命令不会重下载源文件；它会读取现有 `metadata.json` 和 GeoTIFF，并更新 Registry。可安全重复执行。

API 任务状态会返回 `run_id` 和 `protocol_root`。重复下载不会新增重复 asset 行，而是更新现有资产记录并把文件标记为 `skipped`。

## 📂 项目结构

- `stac_api.py`: FastAPI 服务核心，管理后台下载流与状态。
- `stac_core.py`: API 与 CLI 共用的 catalog 查询、asset 解析和安全下载逻辑。
- `earth_lake.py`: Earth Zarr Protocol 初始化、Parquet registry 和 STAC 维护逻辑。
- `lake_monitor.py`: 监控页面使用的只读目录统计、Registry 查询和资源发现逻辑。
- `reindex_lake.py`: 为已有 Source Layer 回填 Collection、产品 profile 和 GeoTIFF 元数据。
- `frontend/`: 无构建依赖的监控控制台页面、样式和交互脚本。
- `monitor_task.py`: CLI 进度监控工具。
- `.agents/skills/stac_downloader/SKILL.md`: Agent Skill 使用说明。
- `.agents/skills/stac_downloader/scripts/stac_tool.py`: 供 AI Agent 或直接 CLI 调用的统一检索下载工具。
- `downloads/`: Earth Lake 根目录；原始文件位于 `source/<catalog>/<collection>/<item_id>/`。
- `tests/`: 不依赖网络的核心功能测试。

## 🧪 高级接口

- **`POST /stac/discover`**: 根据指定的 WKT 范围，查询当前 Catalog 下有哪些可用的数据集。适用于不确定 Collection ID 的场景。
- **`GET /stac/tasks`**: 列出当前 API 进程中的下载任务和字节进度。
- **`GET /lake/summary`**: 返回协议版本、Registry 计数、容量和目录一致性事实。
- **`GET /lake/products`**、**`GET /lake/assets`**: 浏览产品和资产元数据。
- **`GET /lake/runs`**: 查询持久化的 processing runs。
- **`GET /lake/registries/{table}`**: 分页读取白名单内的 Parquet Registry。
- **`GET /lake/resources/{layer}`**: 浏览指定协议层中的相对资源路径。
- **`GET /lake/arrays`**: 发现 Zarr Store 及其元数据文件。

---

*提示：对于 NASA 数据，请确保本地网络可以顺畅访问 `earthdata.nasa.gov`。*
