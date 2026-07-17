import json
import logging
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from pystac_client import Client
from shapely.wkt import loads as load_wkt

from earth_lake import EarthLake
from lake_monitor import LAKE_LAYERS, LakeMonitor
from lake_preview import PreviewError, render_preview
from stac_core import (
    REQUEST_TIMEOUT,
    STAC_CATALOGS,
    asset_filename,
    download_asset,
    get_collection_metadata,
    get_asset_size,
    http_session,
    resolve_asset_url,
    search_items,
    selected_assets,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stac_api")

app = FastAPI(
    title="STAC & NASA CMR API",
    description="Search and download geospatial data from STAC catalogs and NASA CMR.",
    version="1.2.0",
)

DOWNLOAD_DIR = "downloads"
Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
FRONTEND_DIR = Path(__file__).parent / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
TASKS: dict[str, dict[str, Any]] = {}


class SearchRequest(BaseModel):
    wkt: str = Field(..., description="WKT geometry for the area of interest")
    collections: list[str] = Field(..., min_length=1, description="Collection IDs or NASA short names")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    catalog: Literal["microsoft", "earth-search", "nasa"] = "microsoft"
    max_items: int = Field(100, ge=1, le=500, description="Maximum items per collection")

    @field_validator("wkt")
    @classmethod
    def validate_wkt(cls, value: str) -> str:
        try:
            geometry = load_wkt(value)
        except Exception as exc:
            raise ValueError("Invalid WKT geometry") from exc
        if geometry.is_empty:
            raise ValueError("WKT geometry must not be empty")
        return value

    @field_validator("collections")
    @classmethod
    def validate_collections(cls, values: list[str]) -> list[str]:
        cleaned = [value.strip() for value in values if value.strip()]
        if not cleaned:
            raise ValueError("At least one collection is required")
        return cleaned

    @model_validator(mode="after")
    def validate_dates(self) -> "SearchRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be later than end_date")
        return self


class DiscoveryRequest(BaseModel):
    wkt: str = Field(..., description="WKT geometry for the area of interest")
    catalog: Literal["microsoft", "earth-search", "nasa"] = "microsoft"

    @field_validator("wkt")
    @classmethod
    def validate_wkt(cls, value: str) -> str:
        try:
            geometry = load_wkt(value)
        except Exception as exc:
            raise ValueError("Invalid WKT geometry") from exc
        if geometry.is_empty:
            raise ValueError("WKT geometry must not be empty")
        return value


class CollectionInfo(BaseModel):
    id: str
    title: str | None = None
    description: str | None = None
    start_date: str | None = None
    end_date: str | None = None


class TaskStatus(BaseModel):
    task_id: str
    status: Literal["pending", "searching", "downloading", "completed", "partial", "failed"]
    progress: float
    message: str
    start_time: float | None = None
    elapsed_time: float | None = None
    remaining_time: float | None = None
    total_bytes: int = 0
    downloaded_bytes: int = 0
    total_files: int = 0
    completed_files: int = 0
    current_file: str | None = None
    results: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)
    run_id: str | None = None
    protocol_root: str | None = None


def lake_monitor() -> LakeMonitor:
    return LakeMonitor(DOWNLOAD_DIR)


def task_status(task_id: str, data: dict[str, Any]) -> TaskStatus:
    status_data = data.copy()
    if status_data.get("start_time"):
        if status_data["status"] not in {"completed", "partial", "failed"}:
            elapsed = time.time() - status_data["start_time"]
            status_data["elapsed_time"] = round(elapsed, 2)
            progress = status_data.get("progress", 0)
            status_data["remaining_time"] = (
                round(max(0, elapsed / (progress / 100) - elapsed), 2) if progress > 1 else None
            )
        else:
            status_data["remaining_time"] = 0
    return TaskStatus(task_id=task_id, **status_data)


def discover_nasa_collections(bbox: tuple[float, float, float, float]) -> list[CollectionInfo]:
    response = http_session().get(
        "https://cmr.earthdata.nasa.gov/search/collections.json",
        params={"bounding_box": ",".join(map(str, bbox)), "page_size": 200},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return [
        CollectionInfo(
            id=entry.get("short_name"),
            title=entry.get("title"),
            description=entry.get("summary"),
            start_date=entry.get("time_start"),
            end_date=entry.get("time_end"),
        )
        for entry in response.json().get("feed", {}).get("entry", [])
        if entry.get("short_name")
    ]


def discover_stac_collections(catalog: str, aoi_wkt: str) -> list[CollectionInfo]:
    geometry = load_wkt(aoi_wkt)
    min_x, min_y, max_x, max_y = geometry.bounds
    results: list[CollectionInfo] = []
    for collection in Client.open(STAC_CATALOGS[catalog]).get_all_collections():
        try:
            extent = collection.extent.spatial.bboxes[0]
            if extent[0] > max_x or extent[2] < min_x or extent[1] > max_y or extent[3] < min_y:
                continue
            interval = collection.extent.temporal.intervals[0]
            results.append(
                CollectionInfo(
                    id=collection.id,
                    title=collection.title,
                    description=collection.description,
                    start_date=str(interval[0]) if interval[0] else None,
                    end_date=str(interval[1]) if interval[1] else None,
                )
            )
        except (IndexError, TypeError, AttributeError):
            logger.warning("Skipping malformed collection metadata: %s", collection.id)
    return results


def download_worker(
    task_id: str,
    catalog: str,
    items: list[dict[str, Any]],
    only_main: bool,
    lake: EarthLake | None = None,
    run_id: str | None = None,
    collection_metadata_by_id: dict[str, dict[str, Any]] | None = None,
) -> list[str]:
    task = TASKS[task_id]
    task.update(status="downloading", message="Preparing download queue...", total_bytes=0, downloaded_bytes=0)
    lake = lake or EarthLake(DOWNLOAD_DIR)
    owns_run = run_id is None
    run_id = run_id or lake.start_run(
        {"interface": "api", "catalog": catalog, "only_main": only_main, "item_count": len(items)}
    )
    task.update(run_id=run_id, protocol_root=str(lake.root))
    session = http_session()
    jobs: list[dict[str, Any]] = []
    output_asset_ids: list[str] = []
    collection_metadata_by_id = collection_metadata_by_id or {}

    for item in items:
        assets = list(selected_assets(item, only_main))
        if not assets:
            task["failures"].append(f"{item.get('id', 'unknown')}: no matching downloadable assets")
            continue
        for key, asset in assets:
            url = resolve_asset_url(asset, catalog)
            if not url:
                task["failures"].append(f"{item.get('id', 'unknown')}/{key}: missing asset URL")
                continue
            try:
                directory = lake.source_item_directory(catalog, item.get("collection"), item.get("id"))
                filename = asset_filename(key, asset)
            except ValueError as exc:
                task["failures"].append(f"{item.get('id', 'unknown')}/{key}: {exc}")
                continue
            size = get_asset_size(session, url)
            task["total_bytes"] += size
            jobs.append(
                {
                    "url": url,
                    "directory": directory,
                    "filename": filename,
                    "item": item,
                    "asset_key": key,
                    "source_url": asset.get("href", ""),
                    "expected_size": size,
                }
            )

    if not jobs:
        task.update(status="failed", progress=0.0, message="No downloadable assets found.")
        if owns_run:
            lake.finish_run(run_id, "failed", [])
        return []

    task.update(
        message=f"Downloading {len(jobs)} assets...",
        total_files=len(jobs),
        completed_files=0,
        current_file=None,
    )
    for job in jobs:
        destination = job["directory"] / job["filename"]
        task["current_file"] = job["filename"]
        try:
            job["directory"].mkdir(parents=True, exist_ok=True)
            metadata_path = job["directory"] / "metadata.json"
            if not metadata_path.exists():
                metadata_path.write_text(json.dumps(job["item"], indent=2), encoding="utf-8")

            if destination.exists():
                actual_size = destination.stat().st_size
                expected_size = job["expected_size"]
                if expected_size and actual_size != expected_size:
                    task["failures"].append(
                        f"{destination}: existing file size {actual_size} does not match expected {expected_size}"
                    )
                    continue
                task["downloaded_bytes"] += actual_size
                task["skipped"].append(str(destination))
                output_asset_ids.append(
                    lake.record_asset(
                        run_id=run_id,
                        catalog=catalog,
                        item=job["item"],
                        asset_key=job["asset_key"],
                        source_url=job["source_url"],
                        local_path=destination,
                        status="skipped",
                        collection_metadata=collection_metadata_by_id.get(job["item"].get("collection")),
                    )
                )
                continue

            logger.info("Task %s: downloading %s", task_id, job["url"])

            def on_chunk(size: int) -> None:
                task["downloaded_bytes"] += size
                if task["total_bytes"]:
                    task["progress"] = min(99.9, task["downloaded_bytes"] / task["total_bytes"] * 100)

            download_asset(session, job["url"], destination, on_chunk)
            output_asset_ids.append(
                lake.record_asset(
                    run_id=run_id,
                    catalog=catalog,
                    item=job["item"],
                    asset_key=job["asset_key"],
                    source_url=job["source_url"],
                    local_path=destination,
                    status="downloaded",
                    collection_metadata=collection_metadata_by_id.get(job["item"].get("collection")),
                )
            )
            task["results"].append(str(destination))
        except Exception as exc:
            logger.exception("Task %s: failed to download %s", task_id, job["url"])
            task["failures"].append(f"{destination}: {exc}")
        finally:
            task["completed_files"] += 1
            if not task["total_bytes"]:
                task["progress"] = task["completed_files"] / task["total_files"] * 100

    completed = len(task["results"]) + len(task["skipped"])
    task["current_file"] = None
    if task["failures"] and completed:
        task.update(status="partial", progress=100.0, message=f"Downloaded {completed} assets with {len(task['failures'])} failures.")
    elif task["failures"]:
        task.update(status="failed", message=f"All downloads failed ({len(task['failures'])} failures).")
    else:
        task.update(status="completed", progress=100.0, message=f"Downloaded {completed} assets.")
    if owns_run:
        lake.finish_run(run_id, task["status"], output_asset_ids)
    return output_asset_ids


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/", include_in_schema=False)
def monitor_frontend() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/lake/summary")
def get_lake_summary() -> dict[str, Any]:
    return lake_monitor().summary()


@app.get("/lake/products")
def get_lake_products() -> list[dict[str, Any]]:
    return lake_monitor().products()


@app.get("/lake/products/{product_id}")
def get_lake_product(product_id: str) -> dict[str, Any]:
    product = lake_monitor().product(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@app.get("/lake/assets")
def get_lake_assets(
    product_id: str | None = None,
    status: str | None = None,
    q: str | None = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    return lake_monitor().assets(
        product_id=product_id,
        status=status,
        query=q,
        offset=offset,
        limit=limit,
    )


@app.get("/lake/spatial/assets")
def get_spatial_assets(
    product_id: str | None = None,
    variable: str | None = None,
    status: str | None = None,
    q: str | None = None,
) -> dict[str, Any]:
    return lake_monitor().spatial_assets(
        product_id=product_id,
        variable=variable,
        status=status,
        query=q,
    )


@app.get("/lake/previews/{asset_id}.png")
def get_lake_preview(
    asset_id: str,
    max_size: Annotated[int, Query(ge=64, le=2048)] = 1024,
    style: Literal["auto", "gray", "fmask"] = "auto",
) -> FileResponse:
    asset = lake_monitor().asset(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    try:
        preview = render_preview(DOWNLOAD_DIR, asset, max_size=max_size, style=style)
    except PreviewError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return FileResponse(
        preview.path,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable", "X-EarthLake-Cache": "hit" if preview.cached else "miss"},
    )


@app.get("/lake/assets/{asset_id}")
def get_lake_asset(asset_id: str) -> dict[str, Any]:
    asset = lake_monitor().asset(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


@app.get("/lake/runs")
def get_lake_runs(
    q: str | None = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    return lake_monitor().registry_rows(
        "processing_runs",
        query=q,
        offset=offset,
        limit=limit,
    )


@app.get("/lake/registries/{table}")
def get_lake_registry(
    table: str,
    q: str | None = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    try:
        return lake_monitor().registry_rows(table, query=q, offset=offset, limit=limit)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown registry table: {table}") from exc


@app.get("/lake/resources/{layer}")
def get_lake_resources(
    layer: str,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> dict[str, Any]:
    if layer not in LAKE_LAYERS:
        raise HTTPException(status_code=404, detail=f"Unknown lake layer: {layer}")
    return lake_monitor().resources(layer, limit=limit)


@app.get("/lake/arrays")
def get_lake_arrays() -> list[dict[str, Any]]:
    return lake_monitor().arrays()


@app.get("/lake/protocol")
def get_lake_protocol() -> dict[str, Any]:
    return lake_monitor().protocol()


@app.post("/stac/discover", response_model=list[CollectionInfo])
def discover_collections(req: DiscoveryRequest) -> list[CollectionInfo]:
    try:
        if req.catalog == "nasa":
            return discover_nasa_collections(load_wkt(req.wkt).bounds)
        return discover_stac_collections(req.catalog, req.wkt)
    except requests.RequestException as exc:
        logger.exception("Collection discovery failed")
        raise HTTPException(status_code=502, detail=f"Catalog request failed: {exc}") from exc
    except Exception as exc:
        logger.exception("Collection discovery failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def workflow_worker(task_id: str, req: SearchRequest, only_main: bool) -> None:
    task = TASKS[task_id]
    task.update(status="searching", start_time=time.time(), message=f"Searching {req.catalog}...")
    lake: EarthLake | None = None
    run_id: str | None = None
    output_asset_ids: list[str] = []
    collection_metadata_by_id: dict[str, dict[str, Any]] = {}
    try:
        lake = EarthLake(DOWNLOAD_DIR)
        run_id = lake.start_run(
            {
                "interface": "api",
                "catalog": req.catalog,
                "collections": req.collections,
                "wkt": req.wkt,
                "start_date": req.start_date.isoformat(),
                "end_date": req.end_date.isoformat(),
                "max_items": req.max_items,
                "only_main": only_main,
            }
        )
        task.update(run_id=run_id, protocol_root=str(lake.root))
        items = search_items(
            req.catalog,
            req.wkt,
            req.collections,
            req.start_date.isoformat(),
            req.end_date.isoformat(),
            req.max_items,
        )
        if not items:
            task.update(status="completed", progress=100.0, message="No items found in search area/time.")
            return
        for collection in {str(item.get("collection")) for item in items if item.get("collection")}:
            try:
                collection_metadata_by_id[collection] = get_collection_metadata(req.catalog, collection)
            except Exception:
                logger.warning("Could not fetch collection metadata for %s", collection, exc_info=True)
        task["message"] = f"Found {len(items)} items. Starting download..."
        output_asset_ids = download_worker(
            task_id,
            req.catalog,
            items,
            only_main,
            lake,
            run_id,
            collection_metadata_by_id,
        )
    except Exception as exc:
        logger.exception("Task %s failed", task_id)
        task.update(status="failed", message=f"Error: {exc}")
    finally:
        task["elapsed_time"] = round(time.time() - task["start_time"], 2)
        if lake and run_id:
            try:
                lake.finish_run(run_id, task["status"], output_asset_ids)
            except Exception as exc:
                logger.exception("Task %s failed to finalize protocol registry", task_id)
                task.update(status="failed", message=f"Protocol finalization failed: {exc}")


@app.post("/stac/search_and_download", response_model=dict[str, str], status_code=202)
def search_and_download(
    req: SearchRequest,
    background_tasks: BackgroundTasks,
    only_main: bool = Query(True, description="Download representative assets only"),
) -> dict[str, str]:
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "status": "pending",
        "progress": 0.0,
        "message": "Task queued.",
        "results": [],
        "skipped": [],
        "failures": [],
        "total_files": 0,
        "completed_files": 0,
        "current_file": None,
        "run_id": None,
        "protocol_root": str(Path(DOWNLOAD_DIR).resolve()),
    }
    background_tasks.add_task(workflow_worker, task_id, req, only_main)
    return {"task_id": task_id, "message": "Workflow started. Use /stac/tasks/{task_id} to track progress."}


@app.get("/stac/tasks", response_model=list[TaskStatus])
def list_tasks() -> list[TaskStatus]:
    return [
        task_status(task_id, data)
        for task_id, data in sorted(
            TASKS.items(),
            key=lambda item: item[1].get("start_time", 0),
            reverse=True,
        )
    ]


@app.get("/stac/tasks/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str) -> TaskStatus:
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Task not found")
    return task_status(task_id, TASKS[task_id])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
