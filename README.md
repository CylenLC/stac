# STAC & NASA CMR 地理空间数据下载服务

这是一个专业级的地理空间数据检索与下载平台。它整合了 **Microsoft Planetary Computer (MPC)**、**AWS Earth Search** 以及 **NASA CMR (Common Metadata Repository)**，旨在通过统一的 API 接口提供高效、可靠的数据获取服务。

## 🌟 核心特性

- **多源数据整合**：支持搜索并下载 Sentinel (2/3/5P), Landsat, MODIS 以及 NASA SWOT 等多种卫星数据。
- **可恢复采集运行**：SQLite 持久化分页游标、下载批次和文件尝试，支持暂停、恢复、取消、失败重试与进程重启恢复。
- **可恢复协议提交**：Registry 与 STAC 先写入暂存区，再通过提交日志发布；进程中断后可自动回滚准备阶段或继续发布阶段。
- **数据健康中心**：检查 Registry、STAC、Source 文件、物化清单和实体/Zarr 输出的一致性，可选执行完整 SHA-256 校验。
- **可控物化运行**：把 SHP、属性表、NetCDF 和已有 Zarr 作为持久化运行处理，支持暂停、继续、取消、重试与进度查看。
- **实体与 Zarr 探索**：分页查看 Parquet/GeoParquet、在地图预览矢量，并按变量读取带缓存的轻量 Zarr 切片。
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
pip install fastapi uvicorn requests pystac-client planetary-computer pyarrow rasterio pyproj shapely zarr tqdm
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
- **实体与数组**：分页、搜索 Parquet/GeoParquet 行，预览矢量 Geometry；查看 Zarr 变量、维度、Chunk、统计量和轻量热力切片。
- **下载任务**：提交 STAC/NASA 查询，轮询显示实时进度、成功、跳过和失败项，并查看持久化运行历史。
- **物化任务**：从本地水文目录或已有 Zarr 创建 Materialization Run，并控制暂停、继续、取消和失败重试。
- **数据健康**：执行快速或完整审计，查看分级问题、修复建议、磁盘容量和审计历史。
- **协议与系统**：查看协议 JSON、各存储层以及分页读取 Parquet Registry。

页面读取的都是真实协议数据，不生成演示记录。当前的空间页面展示资产覆盖 Geometry，不直接渲染完整 GeoTIFF；Zarr 页面读取 Store 元数据，不将原始文件伪装为 Zarr。

### 3. 一键搜索与下载
调用 `/acquisitions` 接口。`Idempotency-Key` 必填；使用相同键重试提交会返回同一个运行：
```bash
curl -X POST "http://localhost:8000/acquisitions?only_main=true" \
     -H "Content-Type: application/json" \
     -H "Idempotency-Key: sentinel-test-2023-12-01" \
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

Agent 默认执行一个可恢复的 Acquisition Run：

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py acquire \
  --catalog microsoft \
  --wkt "POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))" \
  --collections "sentinel-2-l2a" \
  --start "2023-12-01" \
  --end "2023-12-05" \
  --max 10 \
  --outdir downloads \
  --idempotency-key sentinel-test-2023-12-01
```

默认下载预览或代表性波段；HLS L30 使用 `B04`、`B05`、`Fmask`，HLS S30 使用 `B04`、`B8A`、`Fmask`。如果需要下载结果中的所有资产，请在指令中明确写出“下载所有资产”；对应下载命令会增加 `--all` 参数。下载使用临时 `.part` 文件，只有完整传输后才会写入最终文件名。

NASA CMR 下载同样可以交给 Agent 执行，将数据源设置为 `nasa`。HLS v2.0 可以直接使用产品名称 `HLSL30_V2.0` 和 `HLSS30_V2.0`，工具会自动映射到 NASA CMR 的 `HLSL30` / `HLSS30` collection，并指定版本 `2.0`。下载 NASA 受保护文件前，需先配置上述 Earthdata 凭据。

CLI、API 和 Skill 共用 Acquisition Run Module。CLI 前台执行同一持久化运行；API 由单机调度器后台执行，并可在监控页面暂停、恢复、取消或重试失败文件。服务重启会自动恢复排队和中断运行，手动暂停的运行保持暂停。

## Earth Zarr Protocol 0.1

每次通过 API 或 CLI 下载时，程序都会自动初始化并维护 Earth Lake。检测到 `/Volumes/Untitled/` 时默认使用 `/Volumes/Untitled/stac/`，其他环境回退到项目的 `downloads/`；可通过 `EARTH_LAKE_ROOT` 环境变量显式覆盖。当前阶段保存原始 Source Layer，并维护 STAC 和语义注册表；不会在下载时伪装生成 Zarr，后续物化任务再向 `arrays/` 写入连续数据立方体。

### 本地水文数据物化

`hydro_materializer.py` 将 `/Volumes/Untitled/data` 中 hydrodataset 已适配的数据物化到协议层：SHP 写为 GeoParquet，静态属性 CSV/TXT/XLSX 写为 Parquet，标准化 `*_D.nc` 写为 basin × time Zarr v3。源数据保持只读；每个输出在 `manifests/materializations/hydrodatasets.json` 中记录逻辑物化 ID、来源指纹、内容 SHA-256、大小、行数、字段、CRS 和范围，只有内容校验通过的未变化输出才会复用。

先查看可识别数据：

```bash
uv run python hydro_materializer.py inventory
```

物化实体，并复用 `/Volumes/Untitled/zarr-v3` 中已经生成的 Zarr：

```bash
uv run --extra hydrology python hydro_materializer.py materialize --kind all
```

分数据集转换尚未物化的标准化 NetCDF：

```bash
uv run --extra hydrology python hydro_materializer.py materialize \
  --kind arrays \
  --dataset camels_se \
  --convert-netcdf
```

`--dataset` 可以重复指定，`--limit` 可用于小批量验证；有限额的实体导入会在小体积 SHP 和属性表之间交替取样。已有 Zarr 在同一文件系统中优先通过硬链接复用数据块，失败时才复制；复制中断后会保留 `.partial` 目录，重跑时按文件大小校验并跳过已完成块。标准 NetCDF 会先顺序暂存到本机再转换，避免在外置盘上进行大量随机切片读取。程序不会移动或删除 `/Volumes/Untitled/data` 和 `/Volumes/Untitled/zarr-v3`。物化完成后，“实体与数组”页面会自动显示数据集、类别、记录数、大小和 Zarr 元数据。

也可以在“物化任务”页面创建持久化运行。运行进度保存在 `registry/materialization_state.sqlite`；服务重启会把中断的运行重新排队，手工暂停的运行保持暂停。暂停是协作式的，会在当前源文件或 Zarr 分块文件处理完成后的检查点生效。

```text
/Volumes/Untitled/stac/
├── protocol/                 # 协议版本和受控词表
├── catalog/stac/             # 自包含 STAC Catalog/Collection/Item
├── registry/                 # Parquet 事实注册表及 acquisition_state.sqlite
├── source/                   # 不可变原始下载资产
│   └── <catalog>/<collection>/<item_id>/
├── entities/                 # basin/station/river/patch GeoParquet 与属性 Parquet
├── arrays/                   # basin × time 水文时序和静态属性 Zarr v3
├── virtual/                  # Kerchunk/VirtualiZarr 预留层
├── manifests/               # Acquisition 请求/搜索页快照及训练样本清单
└── cache/                   # 可重建的预览与物化分块缓存
```

每次下载自动维护：

- `registry/processing_runs.parquet`：运行参数、代码 commit、状态和输出资产。
- `registry/acquisition_state.sqlite`：可恢复的运行状态、分页游标、批次和下载尝试；这是可变执行状态的事实来源。
- `manifests/acquisitions/<run_id>/`：原子写入的请求与 gzip JSONL 搜索页快照；页面提交后下载批次才可引用其中资产。
- `registry/assets.parquet`：本地路径、来源 URL、大小、SHA-256、时空范围和 lineage。
- `sources/products/variables/grids.parquet`：数据源、产品、变量语义和原生网格。
- `catalog/stac/`：每个 source granule 对应一个 STAC Item，文件对应 STAC Asset。

Registry 与 STAC 不直接分别覆盖。每次资产登记会创建 Protocol Commit，在 `manifests/protocol_commits/.staging/` 暂存待发布文件，并在 `manifests/protocol_commits/` 记录准备和发布状态。暂存数据属于恢复所需的事务状态，不放入可清理的 `cache/`。准备阶段失败不会污染正式协议；发布阶段被中断时，下次启动会根据日志继续完成，因此 Registry 与 STAC 可以恢复到同一提交。

下载过程还会自动维护三层元数据：

- **Collection 层**：从 STAC Collection 或 NASA CMR Collection 获取标题、简介、许可、提供方、时间范围、文档链接和关键词；HLS profile 仅在上游字段缺失时提供兜底值。
- **Product profile 层**：HLS L30/S30 profile 写入波段长名称、中心波长、带宽、单位、比例因子和质量变量语义。反射率使用无量纲单位 `1`，Fmask 没有物理单位。
- **Asset/grid 层**：每个 GeoTIFF 使用 Rasterio 读取原生 CRS/EPSG、affine transform、原点、像元大小、宽高、dtype、NoData 和原始 GDAL 标签；grid 按实际投影和 transform 注册，不再给所有 HLS tile 写入一个通用 EPSG。

对于下载功能加入前已经存在的资产，可执行一次回填：

```bash
uv run python reindex_lake.py /Volumes/Untitled/stac
```

该命令不会重下载源文件；它会读取现有 `metadata.json` 和 GeoTIFF，并更新 Registry。可安全重复执行。

API 任务状态会返回 `run_id` 和 `protocol_root`。重复下载不会新增重复 asset 行，而是更新现有资产记录并把文件标记为 `skipped`。

## 📂 项目结构

- `stac_api.py`: FastAPI 服务核心，管理后台下载流与状态。
- `stac_core.py`: API 与 CLI 共用的 catalog 查询、asset 解析和安全下载逻辑。
- `earth_lake.py`: Earth Zarr Protocol 初始化、Parquet registry 和 STAC 维护逻辑。
- `protocol_commit.py`: Registry、STAC 和物化清单的暂存、提交日志与中断恢复。
- `materialization.py`: 持久化 Materialization Run 状态、调度与控制。
- `lake_health.py`: 数据健康审计、问题分级和持久化报告。
- `lake_monitor.py`: 监控页面使用的只读目录统计、Registry 查询和资源发现逻辑。
- `reindex_lake.py`: 为已有 Source Layer 回填 Collection、产品 profile 和 GeoTIFF 元数据。
- `frontend/`: 无构建依赖的监控控制台页面、样式和交互脚本。
- `monitor_task.py`: CLI 进度监控工具。
- `.agents/skills/stac_downloader/SKILL.md`: Agent Skill 使用说明。
- `.agents/skills/stac_downloader/scripts/stac_tool.py`: 供 AI Agent 或直接 CLI 调用的统一检索下载工具。
- `/Volumes/Untitled/stac/`: 默认 Earth Lake 根目录；原始文件位于 `source/<catalog>/<collection>/<item_id>/`。
- `tests/`: 不依赖网络的核心功能测试。

## 🧪 高级接口

- **`POST /stac/discover`**: 根据指定的 WKT 范围，查询当前 Catalog 下有哪些可用的数据集。适用于不确定 Collection ID 的场景。
- **`POST /acquisitions`**: 使用显式幂等键创建持久化采集运行；`max_items` 可省略，不再限制为 500。
- **`GET /acquisitions`**、**`GET /acquisitions/{run_id}`**: 游标分页列出运行，或查看运行和下载批次详情。
- **`POST /acquisitions/{run_id}/pause|resume|cancel|retry`**: 控制运行或仅重试失败传输。
- **`GET /stac/tasks`**: 兼容任务视图，数据来自持久化 Acquisition Run。
- **`GET /lake/summary`**: 返回协议版本、Registry 计数、容量和目录一致性事实。
- **`GET /lake/products`**、**`GET /lake/assets`**: 浏览产品和资产元数据。
- **`GET /lake/runs`**: 查询持久化的 processing runs。
- **`GET /lake/registries/{table}`**: 分页读取白名单内的 Parquet Registry。
- **`GET /lake/resources/{layer}`**: 浏览指定协议层中的相对资源路径。
- **`GET /lake/arrays`**: 发现 Zarr Store 及其元数据文件。
- **`GET /lake/entities/page`**、**`GET /lake/entities/features`**: 分页读取实体属性，或返回 GeoParquet 空间要素预览。
- **`GET /lake/arrays/detail`**、**`GET /lake/arrays/slice`**: 查看 Zarr 结构并读取受限大小、可缓存的变量切片。
- **`POST /materializations`**、**`GET /materializations`**: 创建和浏览 Materialization Run。
- **`POST /materializations/{run_id}/pause|resume|cancel|retry`**: 控制物化运行。
- **`GET /health`**、**`GET /health/live`**、**`GET /health/ready`**: 分别提供 liveness 和只读 readiness；ready 失败时返回 HTTP 503，不检查外部 STAC 可用性。
- **`GET /lake/health`**、**`POST /lake/health/audits`**: 查看最近健康报告或启动快速/完整审计；审计不会自动修复数据。

---

*提示：对于 NASA 数据，请确保本地网络可以顺畅访问 `earthdata.nasa.gov`。*
