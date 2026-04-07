# STAC & NASA CMR 地理空间数据下载服务

这是一个专业级的地理空间数据检索与下载平台。它整合了 **Microsoft Planetary Computer (MPC)**、**AWS Earth Search** 以及 **NASA CMR (Common Metadata Repository)**，旨在通过统一的 API 接口提供高效、可靠的数据获取服务。

## 🌟 核心特性

- **多源数据整合**：支持搜索并下载 Sentinel (2/3/5P), Landsat, MODIS 以及 NASA SWOT 等多种卫星数据。
- **专业级任务管理**：基于 FastAPI 的异步后台任务系统，支持大规模数据下载而不阻塞 API。
- **字节级进度监控**：具备精确到字节的下载进度追踪，支持实时显示下载速度和预计剩余时间（ETA）。
- **NASA SWOT 深度支持**：完美集成 NASA CMR 接口，支持 SWOT L2/L4 等复杂数据集的搜索与自动认证下载。
- **AI 智能集成**：内置 Antigravity Skill (`stac_tool.py`)，支持 AI Agent 直接通过命令行执行搜寻和下载任务。

## 🛠️ 环境准备

### 1. 依赖安装
推荐使用 `uv` 或 `pip`：
```bash
uv sync
```
```bash
pip install fastapi uvicorn requests pystac-client planetary-computer shapely tqdm
```

### 2. NASA 认证 (针对 SWOT 等数据)
如果需要下载 NASA 数据，请确保在根目录下配置 `.netrc` 文件：
```bash
# ~/.netrc
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
默认地址：`http://localhost:8000`，访问 `/docs` 可查看交互式文档。

### 2. 一键搜索与下载
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

### 3. 实时进度监控
使用内置的监控客户端（支持进度条和 ETA 显示）：
```bash
python monitor_task.py <TASK_ID>
```

## 📂 项目结构

- `stac_api.py`: FastAPI 服务核心，管理后台下载流与状态。
- `monitor_task.py`: CLI 进度监控工具。
- `scripts/stac_tool.py`: 供 AI Agent 或直接 CLI 调用的统一检索下载工具。
- `downloads/`: 下载数据存放目录，按 `Collection/ItemID` 自动分类。

## 🧪 高级接口

- **`POST /stac/discover`**: 根据指定的 WKT 范围，查询当前 Catalog 下有哪些可用的数据集。适用于不确定 Collection ID 的场景。

---

*提示：对于 NASA 数据，请确保本地网络可以顺畅访问 `earthdata.nasa.gov`。*
