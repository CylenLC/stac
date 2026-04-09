import os
import json
import uuid
import asyncio
import logging
import requests
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from shapely.wkt import loads as load_wkt
from shapely.geometry import shape
from pystac_client import Client
import planetary_computer as pc

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stac_api")

app = FastAPI(
    title="Professional STAC & NASA CMR API",
    description="API for searching and downloading geospatial data from STAC catalogs (MPC, Earth-Search) and NASA CMR (SWOT, etc).",
    version="1.1.0"
)

# --- Configuration ---
STAC_CATALOGS = {
    "microsoft": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "earth-search": "https://earth-search.aws.element84.com/v1",
}
# NASA CMR Search API (Granules)
NASA_CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# In-memory task storage
TASKS = {}

# --- Models ---
class SearchRequest(BaseModel):
    wkt: str = Field(..., description="WKT string of the area of interest")
    collections: List[str] = Field(..., description="List of dataset IDs (Collections/ShortNames)")
    start_date: str = Field("2023-12-01", description="Start date (YYYY-MM-DD)")
    end_date: str = Field("2023-12-31", description="End date (YYYY-MM-DD)")
    catalog: str = Field("microsoft", description="Catalog key: microsoft, earth-search, or nasa")
    max_items: Optional[int] = Field(100, description="Max items per collection")

class DiscoveryRequest(BaseModel):
    wkt: str = Field(..., description="WKT string of the area of interest")
    catalog: str = Field("microsoft", description="Catalog key: microsoft, earth-search, or nasa")

class CollectionInfo(BaseModel):
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None




import time

class TaskStatus(BaseModel):
    task_id: str
    status: str
    progress: float
    message: str
    start_time: Optional[float] = None
    elapsed_time: Optional[float] = None
    remaining_time: Optional[float] = None
    total_bytes: Optional[int] = 0
    downloaded_bytes: Optional[int] = 0
    results: Optional[List[str]] = None



def discover_nasa_collections(bbox: tuple) -> List[CollectionInfo]:
    """Search NASA CMR for collections intersecting bbox."""
    url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    params = {
        "bounding_box": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
        "page_size": 200
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    entries = r.json().get("feed", {}).get("entry", [])
    
    results = []
    for entry in entries:
        results.append(CollectionInfo(
            id=entry.get("short_name"),
            title=entry.get("title"),
            description=entry.get("summary"),
            start_date=entry.get("time_start"),
            end_date=entry.get("time_end")
        ))
    return results

def discover_stac_collections(catalog_url: str, aoi_wkt: str) -> List[CollectionInfo]:
    """Discover STAC collections whose extent intersects the AOI."""
    client = Client.open(catalog_url)
    aoi_geom = load_wkt(aoi_wkt)
    bbox_aoi = aoi_geom.bounds # (minx, miny, maxx, maxy)
    
    results = []
    # Get all collections from the catalog
    for col in client.get_all_collections():
        try:
            # Check spatial intersection using bbox
            # STAC extent is usually [[minx, miny, maxx, maxy]]
            extent = col.extent.spatial.bboxes[0]
            # Simple bbox overlap check
            if not (extent[0] > bbox_aoi[2] or extent[2] < bbox_aoi[0] or 
                    extent[1] > bbox_aoi[3] or extent[3] < bbox_aoi[1]):
                
                # Fetch temporal extent
                temp_ext = col.extent.temporal.intervals[0]
                
                results.append(CollectionInfo(
                    id=col.id,
                    title=col.title,
                    description=col.description,
                    start_date=str(temp_ext[0]) if temp_ext[0] else None,
                    end_date=str(temp_ext[1]) if temp_ext[1] else None
                ))
        except:
            continue
    return results

# --- Helpers ---

def resolve_asset_url(asset_info: dict, catalog: str) -> str:
    """Resolves and signs URLs based on catalog type."""
    href = asset_info.get("href", "")
    
    # 1. Microsoft Planetary Computer Signing
    if catalog == "microsoft" or "planetarycomputer" in href:
        if asset_info.get("msft:https-url"):
            href = asset_info.get("msft:https-url")
        if href.startswith("http"):
            try:
                return pc.sign(href)
            except:
                return href
    
    # 2. NASA URLs usually direct but requiring auth (handled by requests via .netrc)
    return href

def get_asset_size(url: str) -> int:
    """Gets the size of an asset via HEAD request."""
    try:
        r = requests.head(url, timeout=5, allow_redirects=True)
        return int(r.headers.get("content-length", 0))
    except:
        return 0

def download_worker(task_id: str, catalog: str, items: List[dict], only_main: bool):
    TASKS[task_id]["status"] = "running"
    TASKS[task_id]["start_time"] = time.time()
    TASKS[task_id]["downloaded_bytes"] = 0
    
    # 1. Pre-calculate total size if not already known
    total_bytes = 0
    download_queue = []
    
    TASKS[task_id]["message"] = "Pre-calculating total download size..."
    for item in items:
        cid = item.get("collection") or item.get("collection_id") or "unknown"
        iid = item.get("id", "unknown")
        
        assets = item.get("assets", {})
        if not assets and "download_url" in item:
            assets = {"data": {"href": item["download_url"], "roles": ["data"]}}

        for a_key, a_info in assets.items():
            roles = a_info.get("roles", [])
            is_main = any(r in ["data", "visual", "overview"] for r in roles) or a_key in ["visual", "rendered_preview", "B04", "B08", "data"]
            if only_main and not is_main:
                continue
            
            # --- 加固：从原始 href 提取文件名 ---
            original_href = a_info.get("href", "")
            filename = original_href.split("/")[-1].split("?")[0]
            if not filename or len(filename) > 120:
                filename = f"{a_key}.data"
            
            url = resolve_asset_url(a_info, catalog)
            size = get_asset_size(url)
            total_bytes += size
            
            download_queue.append({
                "url": url,
                "item_dir": os.path.join(DOWNLOAD_DIR, cid, iid),
                "filename": filename,
                "metadata": item
            })



            
    TASKS[task_id]["total_bytes"] = total_bytes
    TASKS[task_id]["status"] = "downloading"
    
    results = []
    for job in download_queue:
        try:
            os.makedirs(job["item_dir"], exist_ok=True)
            save_path = os.path.join(job["item_dir"], job["filename"])
            
            # Save metadata if not exists
            meta_path = os.path.join(job["item_dir"], f"{job['metadata'].get('id')}_metadata.json")
            if not os.path.exists(meta_path):
                with open(meta_path, "w") as f:
                    json.dump(job["metadata"], f, indent=2)

            if os.path.exists(save_path):
                fsize = os.path.getsize(save_path)
                TASKS[task_id]["downloaded_bytes"] += fsize
                results.append(save_path)
                continue

            logger.info(f"Task {task_id}: Downloading {job['url']}")
            r = requests.get(job["url"], stream=True, timeout=60)
            if r.status_code == 200:
                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                            TASKS[task_id]["downloaded_bytes"] += len(chunk)
                            # Update percentage based on bytes
                            if total_bytes > 0:
                                TASKS[task_id]["progress"] = min(99.9, (TASKS[task_id]["downloaded_bytes"] / total_bytes) * 100)
                results.append(save_path)
            else:
                logger.error(f"Failed {job['url']}: {r.status_code}")
                
        except Exception as e:
            logger.exception(f"Error in download job")
            TASKS[task_id]["message"] = f"Warning: Failure in {job['filename']}: {str(e)}"
            
    TASKS[task_id]["status"] = "completed"
    TASKS[task_id]["progress"] = 100.0
    TASKS[task_id]["results"] = results
    TASKS[task_id]["elapsed_time"] = round(time.time() - TASKS[task_id]["start_time"], 2)



# --- Endpoints ---
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

def perform_search(req: SearchRequest) -> List[Dict[str, Any]]:
    """Internal helper to perform search based on catalog."""
    aoi_geom = load_wkt(req.wkt)
    bbox = aoi_geom.bounds
    
    if req.catalog == "nasa":
        logger.info("Using NASA CMR Search API...")
        all_nasa_items = []
        temporal_str = f"{req.start_date}T00:00:00Z,{req.end_date}T23:59:59Z"
        
        for col_id in req.collections:
            params = {
                "short_name": col_id,
                "bounding_box": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                "temporal": temporal_str,
                "page_size": req.max_items,
            }
            r = requests.get(NASA_CMR_URL, params=params)
            r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
            
            for entry in entries:
                item = {
                    "id": entry.get("id"),
                    "collection": col_id,
                    "properties": {"datetime": entry.get("time_start")},
                    "assets": {}
                }
                for link in entry.get("links", []):
                    href = link.get("href", "")
                    if href.endswith((".nc", ".zip", ".nc.zip")) and "opendap" not in href.lower():
                        item["assets"]["data"] = {"href": href, "roles": ["data"]}
                        item["download_url"] = href
                        break
                all_nasa_items.append(item)
        return all_nasa_items

    catalog_url = STAC_CATALOGS.get(req.catalog)
    if not catalog_url:
        raise HTTPException(status_code=400, detail=f"Invalid catalog key. Choices: {list(STAC_CATALOGS.keys())} or 'nasa'")
        
    client = Client.open(catalog_url)
    # 3. Perform Search
    # Special optimization: DEM data (like cop-dem-glo-30) often doesn't have a record date 
    # matching the imagery window. We skip datetime for DEMs.
    is_dem = any(token in req.collections[0].lower() for token in ["dem", "nasadem", "alpsml"])
    current_dt = None if is_dem else f"{req.start_date}/{req.end_date}"
    
    search = client.search(
        collections=req.collections,
        intersects=aoi_geom,
        datetime=current_dt,
        max_items=req.max_items
    )

    return [item.to_dict() for item in search.items()]

@app.post("/stac/discover", response_model=List[CollectionInfo])
def discover_collections(req: DiscoveryRequest):
    """Find all collections that intersect with the given WKT."""
    try:
        aoi_geom = load_wkt(req.wkt)
        bbox = aoi_geom.bounds
        
        if req.catalog == "nasa":
            return discover_nasa_collections(bbox)
        
        catalog_url = STAC_CATALOGS.get(req.catalog)
        if not catalog_url:
            raise HTTPException(status_code=400, detail="Invalid catalog")
            
        return discover_stac_collections(catalog_url, req.wkt)
    except Exception as e:
        logger.exception("Discovery failed")
        raise HTTPException(status_code=500, detail=str(e))




def workflow_worker(task_id: str, req: SearchRequest, only_main: bool):
    """Background worker that performs both search and download (Synchronous)."""
    TASKS[task_id]["status"] = "searching"
    TASKS[task_id]["message"] = f"Searching {req.catalog} for {len(req.collections)} collections..."

    try:
        # 1. Perform Search (Now sync)
        items = perform_search(req)
        
        if not items:
            TASKS[task_id]["status"] = "completed"
            TASKS[task_id]["message"] = "No items found in search area/time."
            TASKS[task_id]["progress"] = 100.0
            return

        TASKS[task_id]["status"] = "downloading"
        TASKS[task_id]["message"] = f"Found {len(items)} items. Starting download..."
        
        # 2. Perform Download (Now sync)
        download_worker(task_id, req.catalog, items, only_main)
        
    except Exception as e:
        logger.exception("Background workflow failed")
        TASKS[task_id]["status"] = "failed"
        TASKS[task_id]["message"] = f"Error: {str(e)}"

@app.post("/stac/search_and_download", response_model=Dict[str, str])
def search_and_download(req: SearchRequest, background_tasks: BackgroundTasks, only_main: bool = Query(True)):
    """Async Search + Download. Returns task_id immediately."""
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "status": "pending",
        "progress": 0.0,
        "message": "Task queued.",
        "results": []
    }
    
    # Send the whole search workflow to background
    background_tasks.add_task(
        workflow_worker,
        task_id,
        req,
        only_main
    )
    
    return {
        "task_id": task_id, 
        "message": "Workflow started. Use /stac/tasks/{task_id} to track progress."
    }


@app.get("/stac/tasks/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = TASKS[task_id]
    status_data = task.copy()
    
    # Calculate times on the fly for better accuracy
    now = time.time()
    if task.get("start_time"):
        if task["status"] not in ["completed", "failed"]:
            elapsed = now - task["start_time"]
            status_data["elapsed_time"] = round(elapsed, 2)
            
            # Calculate ETA (Remaining time)
            progress = task.get("progress", 0)
            if progress > 1: # Avoid division by zero and jumping at start
                total_est_duration = elapsed / (progress / 100.0)
                remaining = total_est_duration - elapsed
                status_data["remaining_time"] = round(max(0, remaining), 2)
            else:
                status_data["remaining_time"] = None
        else:
            # Task is done
            status_data["remaining_time"] = 0
            # elapsed_time is already set in the worker for completed tasks
    
    return TaskStatus(task_id=task_id, **status_data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
