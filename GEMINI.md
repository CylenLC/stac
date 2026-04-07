# Project Overview: STAC & NASA CMR API

This project provides a professional API and toolset for searching and downloading geospatial data from SpatioTemporal Asset Catalogs (STAC) and NASA's Common Metadata Repository (CMR). It specifically supports the Microsoft Planetary Computer, Earth Search (AWS), and NASA CMR.

## Core Technologies
- **Backend:** Python, FastAPI, Uvicorn
- **Geospatial Libraries:** `pystac-client`, `shapely`, `planetary-computer`
- **Data Fetching:** `requests`, `tqdm` (for progress monitoring)
- **Environment Management:** `uv` (implied by `uv.lock` and `pyproject.toml`)

## Architecture
- **API (`stac_api.py`):** A FastAPI application providing endpoints for collection discovery, searching items across catalogs, and triggering asynchronous download workflows.
- **Task Management:** Background workers handle data downloads, allowing users to track progress via task IDs.
- **Progress Monitoring (`monitor_task.py`):** A CLI utility that connects to the API to provide a real-time progress bar for active download tasks.
- **Agent Integration:** A custom skill located in `.agents/skills/stac_downloader/` allows Gemini agents to perform STAC operations directly via CLI tools.

## Key Commands

### Running the API
```bash
# Start the FastAPI server
python stac_api.py
```
Default URL: `http://localhost:8000`

### Monitoring a Download Task
```bash
# Monitor a task by its UUID
python monitor_task.py <task_id>
```

### Using the Agent Skill (CLI)
```bash
# Search for Sentinel-2 data on Microsoft Planetary Computer
python .agents/skills/stac_downloader/scripts/stac_tool.py search \
  --wkt "POLYGON((...))" \
  --collections "sentinel-2-l2a" \
  --catalog microsoft \
  --output results.json

# Download the found items
python .agents/skills/stac_downloader/scripts/stac_tool.py download \
  --input results.json \
  --catalog microsoft
```

## Development Conventions
- **Asynchronous Workflows:** Long-running downloads are handled as background tasks.
- **WKT for AOI:** Areas of Interest are primarily defined using Well-Known Text (WKT) strings.
- **Asset Signing:** Automatically handles Microsoft Planetary Computer asset signing using `planetary-computer`.
- **Directory Structure:** Downloads are organized by collection and item ID within the `downloads/` directory.

## Key Files
- `stac_api.py`: Main FastAPI entry point and logic.
- `monitor_task.py`: CLI tool for task progress visualization.
- `pyproject.toml`: Dependency and project metadata.
- `.agents/skills/stac_downloader/SKILL.md`: Documentation for the agent skill.
- `.agents/skills/stac_downloader/scripts/stac_tool.py`: The core CLI tool used by the agent skill.
