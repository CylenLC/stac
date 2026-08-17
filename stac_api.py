import json
import logging
import os
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from pystac_client import Client
from shapely.wkt import loads as load_wkt

from lake_monitor import LAKE_LAYERS, LakeMonitor
from lake_preview import PreviewError, render_preview
from stac_core import (
    REQUEST_TIMEOUT,
    STAC_CATALOGS,
    http_session,
)
from acquisition import AcquisitionManager, AcquisitionRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stac_api")

app = FastAPI(
    title="STAC & NASA CMR API",
    description="Search and download geospatial data from STAC catalogs and NASA CMR.",
    version="1.2.0",
)

DEFAULT_DOWNLOAD_DIR = Path("/Volumes/Untitled/stac")
DOWNLOAD_DIR = Path(os.environ.get("EARTH_LAKE_ROOT", str(DEFAULT_DOWNLOAD_DIR))).expanduser().resolve()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
FRONTEND_DIR = Path(__file__).parent / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
TASK_LIST_CACHE_TTL_SECONDS = 5.0
TASK_LIST_CACHE: dict[str, Any] = {"expires_at": 0.0, "items": []}
TASK_LIST_CACHE_LOCK = threading.Lock()


def invalidate_task_list_cache() -> None:
    with TASK_LIST_CACHE_LOCK:
        TASK_LIST_CACHE.update(expires_at=0.0, items=[])


class SearchRequest(BaseModel):
    wkt: str = Field(..., description="WKT geometry for the area of interest")
    collections: list[str] = Field(..., min_length=1, description="Collection IDs or NASA short names")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    catalog: Literal["microsoft", "earth-search", "nasa"] = "microsoft"
    max_items: int | None = Field(None, ge=1, description="Optional total item limit; omit to follow all pages")
    asset_keys: list[str] | None = Field(
        None,
        description="Optional explicit STAC Asset keys; omit for provider-aware main/all selection",
    )

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

    @field_validator("asset_keys")
    @classmethod
    def validate_asset_keys(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        cleaned = list(dict.fromkeys(value.strip() for value in values if value.strip()))
        if not cleaned:
            raise ValueError("asset_keys must contain at least one non-empty key")
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
    status: Literal["pending", "queued", "recovering", "searching", "discovering", "planning", "downloading", "finalizing", "paused", "auth_required", "cancelling", "cancelled", "completed", "partial", "failed"]
    progress: float
    message: str
    start_time: float | None = None
    elapsed_time: float | None = None
    remaining_time: float | None = None
    total_bytes: int = 0
    downloaded_bytes: int = 0
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    planning_errors: int = 0
    current_file: str | None = None
    results: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)
    run_id: str | None = None
    protocol_root: str | None = None


def acquisition_manager() -> AcquisitionManager:
    manager = getattr(app.state, "acquisition_manager", None)
    if manager is None:
        manager = AcquisitionManager(DOWNLOAD_DIR)
        app.state.acquisition_manager = manager
    return manager


@app.on_event("startup")
def start_acquisition_scheduler() -> None:
    manager = acquisition_manager()
    manager.start_scheduler()
    monitor = LakeMonitor(DOWNLOAD_DIR)
    monitor.warm_registry()
    app.state.lake_monitor = monitor


@app.on_event("shutdown")
def stop_acquisition_scheduler() -> None:
    manager = getattr(app.state, "acquisition_manager", None)
    if manager:
        manager.stop_scheduler()


def lake_monitor() -> LakeMonitor:
    monitor = getattr(app.state, "lake_monitor", None)
    if monitor is None:
        monitor = LakeMonitor(DOWNLOAD_DIR)
        app.state.lake_monitor = monitor
    return monitor


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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/", include_in_schema=False)
def monitor_frontend() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/lake/summary")
def get_lake_summary(scan_filesystem: bool = False) -> dict[str, Any]:
    return lake_monitor().summary(scan_filesystem=scan_filesystem)


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
    exact: bool = False,
) -> dict[str, Any]:
    return lake_monitor().spatial_assets(
        product_id=product_id,
        variable=variable,
        status=status,
        query=q,
        exact=exact,
    )


@app.get("/lake/spatial/assets/{asset_id}")
def get_spatial_asset(asset_id: str) -> dict[str, Any]:
    feature = lake_monitor().spatial_asset(asset_id, exact=True)
    if feature is None:
        raise HTTPException(status_code=404, detail="Spatial asset not found")
    return feature


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


@app.post("/stac/search_and_download", response_model=dict[str, str], status_code=202)
def search_and_download(
    req: SearchRequest,
    only_main: bool = Query(True, description="Download representative assets only"),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
) -> dict[str, str]:
    manager = acquisition_manager()
    request = AcquisitionRequest(
        catalog=req.catalog, collections=req.collections, wkt=req.wkt,
        start_date=req.start_date.isoformat(), end_date=req.end_date.isoformat(),
        max_items=req.max_items, only_main=only_main, asset_keys=req.asset_keys,
    )
    task_id = manager.create_run(request, idempotency_key)
    invalidate_task_list_cache()
    return {"task_id": task_id, "run_id": task_id, "message": "Acquisition queued. Use /acquisitions/{run_id} to track progress."}


@app.get("/stac/tasks", response_model=list[TaskStatus])
def list_tasks() -> list[TaskStatus]:
    # Acquisition completion is reconciled against the Processing registry on
    # every manager.list_runs() call. Do not serve a stale terminal snapshot.
    now = time.monotonic()
    items = [_run_task(run) for run in acquisition_manager().list_runs(limit=200)["items"]]
    with TASK_LIST_CACHE_LOCK:
        TASK_LIST_CACHE.update(expires_at=now + TASK_LIST_CACHE_TTL_SECONDS, items=items)
    return items


@app.get("/stac/tasks/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str) -> TaskStatus:
    try:
        return _run_task(acquisition_manager().reconcile_external_status(task_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Task not found") from exc


def _run_task(run: dict[str, Any]) -> TaskStatus:
    status = "searching" if run["status"] == "discovering" else run["status"]
    started = datetime.fromisoformat(run["started_at"]).timestamp() if run.get("started_at") else None
    finished = datetime.fromisoformat(run["finished_at"]).timestamp() if run.get("finished_at") else time.time()
    elapsed = max(0.0, finished - started) if started else None
    speed = run["downloaded_bytes"] / elapsed if elapsed and run["downloaded_bytes"] else 0.0
    remaining = (
        max(0.0, (run["total_bytes"] - run["downloaded_bytes"]) / speed)
        if speed and run["total_bytes"] > run["downloaded_bytes"]
        else None
    )
    return TaskStatus(
        task_id=run["run_id"], run_id=run["run_id"], status=status,
        progress=run["progress"], message=run["message"], start_time=started,
        elapsed_time=elapsed, remaining_time=remaining,
        total_bytes=run["total_bytes"], downloaded_bytes=run["downloaded_bytes"],
        total_files=run["total_files"], completed_files=run["completed_files"],
        failed_files=run["failed_files"],
        planning_errors=run.get("planning_errors", 0),
        current_file=run["current_file"], failures=[run["error"]] if run.get("error") else [],
        protocol_root=str(Path(DOWNLOAD_DIR).resolve()),
    )


@app.post("/acquisitions", status_code=202)
def create_acquisition(
    req: SearchRequest,
    only_main: bool = Query(True),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
) -> dict[str, Any]:
    return search_and_download(req, only_main, idempotency_key)


@app.get("/acquisitions")
def list_acquisitions(cursor: str | None = None, limit: Annotated[int, Query(ge=1, le=200)] = 50) -> dict[str, Any]:
    return acquisition_manager().list_runs(cursor, limit)


@app.get("/acquisitions/{run_id}")
def get_acquisition(run_id: str) -> dict[str, Any]:
    try:
        run = acquisition_manager().reconcile_external_status(run_id)
        run["batches"] = acquisition_manager().list_batches(run_id)
        return run
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Acquisition Run not found") from exc


def _control_run(run_id: str, action: str) -> dict[str, Any]:
    manager = acquisition_manager()
    try:
        run = getattr(manager, f"{action}_run")(run_id)
        invalidate_task_list_cache()
        return run
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Acquisition Run not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/acquisitions/{run_id}/pause")
def pause_acquisition(run_id: str) -> dict[str, Any]:
    return _control_run(run_id, "pause")


@app.post("/acquisitions/{run_id}/resume")
def resume_acquisition(run_id: str) -> dict[str, Any]:
    return _control_run(run_id, "resume")


@app.post("/acquisitions/{run_id}/cancel")
def cancel_acquisition(run_id: str) -> dict[str, Any]:
    return _control_run(run_id, "cancel")


@app.post("/acquisitions/{run_id}/retry")
def retry_acquisition(run_id: str) -> dict[str, Any]:
    manager = acquisition_manager()
    try:
        run = manager.retry_failed(run_id)
        invalidate_task_list_cache()
        return run
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Acquisition Run not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
