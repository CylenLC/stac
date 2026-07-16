import json
import logging
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from pystac_client import Client
from shapely.wkt import loads as load_wkt

from stac_core import (
    REQUEST_TIMEOUT,
    STAC_CATALOGS,
    asset_filename,
    download_asset,
    get_asset_size,
    http_session,
    item_directory,
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
    results: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


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


def download_worker(task_id: str, catalog: str, items: list[dict[str, Any]], only_main: bool) -> None:
    task = TASKS[task_id]
    task.update(status="downloading", message="Preparing download queue...", total_bytes=0, downloaded_bytes=0)
    session = http_session()
    jobs: list[dict[str, Any]] = []

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
                directory = item_directory(DOWNLOAD_DIR, item.get("collection"), item.get("id"))
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
                    "expected_size": size,
                }
            )

    if not jobs:
        task.update(status="failed", progress=0.0, message="No downloadable assets found.")
        return

    task["message"] = f"Downloading {len(jobs)} assets..."
    for job in jobs:
        destination = job["directory"] / job["filename"]
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
                continue

            logger.info("Task %s: downloading %s", task_id, job["url"])

            def on_chunk(size: int) -> None:
                task["downloaded_bytes"] += size
                if task["total_bytes"]:
                    task["progress"] = min(99.9, task["downloaded_bytes"] / task["total_bytes"] * 100)

            download_asset(session, job["url"], destination, on_chunk)
            task["results"].append(str(destination))
        except Exception as exc:
            logger.exception("Task %s: failed to download %s", task_id, job["url"])
            task["failures"].append(f"{destination}: {exc}")

    completed = len(task["results"]) + len(task["skipped"])
    if task["failures"] and completed:
        task.update(status="partial", progress=100.0, message=f"Downloaded {completed} assets with {len(task['failures'])} failures.")
    elif task["failures"]:
        task.update(status="failed", message=f"All downloads failed ({len(task['failures'])} failures).")
    else:
        task.update(status="completed", progress=100.0, message=f"Downloaded {completed} assets.")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


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
    try:
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
        task["message"] = f"Found {len(items)} items. Starting download..."
        download_worker(task_id, req.catalog, items, only_main)
    except Exception as exc:
        logger.exception("Task %s failed", task_id)
        task.update(status="failed", message=f"Error: {exc}")
    finally:
        task["elapsed_time"] = round(time.time() - task["start_time"], 2)


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
    }
    background_tasks.add_task(workflow_worker, task_id, req, only_main)
    return {"task_id": task_id, "message": "Workflow started. Use /stac/tasks/{task_id} to track progress."}


@app.get("/stac/tasks/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str) -> TaskStatus:
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Task not found")
    status_data = TASKS[task_id].copy()
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
