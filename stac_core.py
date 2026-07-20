"""Shared catalog search and download helpers for the API and CLI."""

import json
import os
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import unquote, urlparse

import planetary_computer as pc
import requests
from pystac_client import Client
from requests.adapters import HTTPAdapter
from shapely.wkt import loads as load_wkt
from urllib3.util.retry import Retry

STAC_CATALOGS = {
    "microsoft": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "earth-search": "https://earth-search.aws.element84.com/v1",
}
NASA_CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
NASA_CMR_COLLECTIONS_URL = "https://cmr.earthdata.nasa.gov/search/collections.json"
NASA_COLLECTIONS = {
    "HLSL30_V2.0": ("HLSL30", "2.0"),
    "HLSS30_V2.0": ("HLSS30", "2.0"),
}
REQUEST_TIMEOUT = (10, 60)
DATA_EXTENSIONS = (".nc", ".zip", ".nc.zip", ".tif", ".tiff")


def http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session


def resolve_asset_url(asset: dict[str, Any], catalog: str) -> str:
    href = asset.get("href", "")
    if catalog == "microsoft" or "planetarycomputer" in href:
        href = asset.get("msft:https-url", href)
        if href.startswith("http"):
            try:
                return pc.sign(href)
            except Exception:
                return href
    return href


def safe_component(value: object, fallback: str) -> str:
    value = str(value or fallback).replace("\\", "/")
    name = Path(value).name
    if name in {"", ".", ".."}:
        return fallback
    return name[:120]


def asset_filename(asset_key: str, asset: dict[str, Any]) -> str:
    path = unquote(urlparse(asset.get("href", "")).path)
    return safe_component(Path(path).name, f"{safe_component(asset_key, 'asset')}.data")


def item_directory(output_dir: str, collection: object, item_id: object) -> Path:
    base = Path(output_dir).resolve()
    path = base / safe_component(collection, "unknown") / safe_component(item_id, "unknown")
    if base not in path.resolve().parents:
        raise ValueError("Download path escapes the output directory")
    return path


def nasa_assets(links: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    assets: dict[str, dict[str, Any]] = {}
    for link in links:
        href = link.get("href", "")
        if (
            not href.lower().endswith(DATA_EXTENSIONS)
            or "opendap" in href.lower()
            or not link.get("rel", "").endswith("/data#")
        ):
            continue

        filename = asset_filename("data", {"href": href})
        key = filename.rsplit(".", 1)[0].rsplit(".", 1)[-1]
        key = safe_component(key, "data")
        suffix = 2
        unique_key = key
        while unique_key in assets:
            unique_key = f"{key}_{suffix}"
            suffix += 1
        assets[unique_key] = {"href": href, "roles": ["data"]}
    return assets


def cmr_geometry(entry: dict[str, Any]) -> tuple[dict[str, Any] | None, list[float] | None]:
    rings: list[list[list[float]]] = []
    for polygon_group in entry.get("polygons", []):
        for polygon in polygon_group:
            values = [float(value) for value in polygon.split()]
            coordinates = [[values[index + 1], values[index]] for index in range(0, len(values), 2)]
            if coordinates and coordinates[0] != coordinates[-1]:
                coordinates.append(coordinates[0])
            if len(coordinates) >= 4:
                rings.append(coordinates)

    if not rings and entry.get("boxes"):
        south, west, north, east = map(float, entry["boxes"][0].split())
        rings = [[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    if not rings:
        return None, None

    points = [point for ring in rings for point in ring]
    bbox = [
        min(point[0] for point in points),
        min(point[1] for point in points),
        max(point[0] for point in points),
        max(point[1] for point in points),
    ]
    if len(rings) == 1:
        return {"type": "Polygon", "coordinates": [rings[0]]}, bbox
    return {"type": "MultiPolygon", "coordinates": [[[point for point in ring]] for ring in rings]}, bbox


def search_items(
    catalog: str,
    wkt: str,
    collections: list[str],
    start_date: str,
    end_date: str,
    max_items: int,
) -> list[dict[str, Any]]:
    aoi = load_wkt(wkt)
    bbox = aoi.bounds

    if catalog == "nasa":
        session = http_session()
        items: list[dict[str, Any]] = []
        temporal = f"{start_date}T00:00:00Z,{end_date}T23:59:59Z"
        for collection in collections:
            short_name, version = NASA_COLLECTIONS.get(collection, (collection, None))
            params: dict[str, Any] = {
                "short_name": short_name,
                "bounding_box": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                "temporal": temporal,
                "page_size": max_items,
            }
            if version:
                params["version"] = version
            response = session.get(NASA_CMR_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            for entry in response.json().get("feed", {}).get("entry", []):
                geometry, item_bbox = cmr_geometry(entry)
                items.append(
                    {
                        "id": entry.get("id"),
                        "collection": collection,
                        "bbox": item_bbox,
                        "geometry": geometry,
                        "properties": {
                            "datetime": entry.get("time_start"),
                            "cloud_cover": entry.get("cloud_cover"),
                            "producer_granule_id": entry.get("producer_granule_id"),
                        },
                        "assets": nasa_assets(entry.get("links", [])),
                    }
                )
        return items

    catalog_url = STAC_CATALOGS.get(catalog)
    if not catalog_url:
        raise ValueError(f"Invalid catalog: {catalog}")
    is_dem = any(
        token in collection.lower()
        for collection in collections
        for token in ("dem", "nasadem", "alpsml")
    )
    search = Client.open(catalog_url).search(
        collections=collections,
        intersects=aoi,
        datetime=None if is_dem else f"{start_date}/{end_date}",
        max_items=max_items,
    )
    return [item.to_dict() for item in search.items()]


def search_items_page(
    catalog: str,
    wkt: str,
    collections: list[str],
    start_date: str,
    end_date: str,
    cursor: str | None,
    page_size: int = 100,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch a resumable logical page without imposing a 500-item run limit."""
    if catalog == "nasa":
        state = json.loads(cursor) if cursor else {"collection_index": 0, "page_num": 1}
        collection_index = int(state["collection_index"])
        if collection_index >= len(collections):
            return [], None
        collection = collections[collection_index]
        short_name, version = NASA_COLLECTIONS.get(collection, (collection, None))
        bbox = load_wkt(wkt).bounds
        params: dict[str, Any] = {
            "short_name": short_name,
            "bounding_box": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
            "temporal": f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
            "page_size": page_size,
            "page_num": int(state["page_num"]),
        }
        if version:
            params["version"] = version
        response = http_session().get(NASA_CMR_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        entries = response.json().get("feed", {}).get("entry", [])
        items = []
        for entry in entries:
            geometry, item_bbox = cmr_geometry(entry)
            items.append({
                "id": entry.get("id"), "collection": collection, "bbox": item_bbox,
                "geometry": geometry,
                "properties": {
                    "datetime": entry.get("time_start"), "cloud_cover": entry.get("cloud_cover"),
                    "producer_granule_id": entry.get("producer_granule_id"),
                },
                "assets": nasa_assets(entry.get("links", [])),
            })
        if len(entries) == page_size:
            outgoing_state = {"collection_index": collection_index, "page_num": int(state["page_num"]) + 1}
        elif collection_index + 1 < len(collections):
            outgoing_state = {"collection_index": collection_index + 1, "page_num": 1}
        else:
            outgoing_state = None
        return items, json.dumps(outgoing_state, separators=(",", ":")) if outgoing_state else None

    offset = int(cursor or 0)
    items = search_items(catalog, wkt, collections, start_date, end_date, offset + page_size)
    page = items[offset : offset + page_size]
    return page, str(offset + len(page)) if len(page) == page_size else None


def get_collection_metadata(catalog: str, collection: str) -> dict[str, Any]:
    """Fetch one authoritative STAC Collection or CMR collection record."""
    if catalog == "nasa":
        short_name, version = NASA_COLLECTIONS.get(collection, (collection, None))
        params: dict[str, Any] = {"short_name": short_name, "page_size": 1}
        if version:
            params["version"] = version
        response = http_session().get(NASA_CMR_COLLECTIONS_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        entries = response.json().get("feed", {}).get("entry", [])
        return entries[0] if entries else {}

    catalog_url = STAC_CATALOGS.get(catalog)
    if not catalog_url:
        raise ValueError(f"Invalid catalog: {catalog}")
    return Client.open(catalog_url).get_collection(collection).to_dict()


def is_main_asset(asset_key: str, asset: dict[str, Any], asset_count: int) -> bool:
    if asset_count == 1:
        return True
    key = asset_key.lower()
    roles = {role.lower() for role in asset.get("roles", [])}
    return bool(roles & {"visual", "overview"}) or key in {
        "visual",
        "rendered_preview",
        "overview",
        "b04",
        "b08",
        "fmask",
    }


def selected_assets(item: dict[str, Any], only_main: bool) -> Iterable[tuple[str, dict[str, Any]]]:
    assets = item.get("assets", {})
    collection = item.get("collection", "")
    preferred = {
        "HLSL30_V2.0": {"B04", "B05", "Fmask"},
        "HLSS30_V2.0": {"B04", "B8A", "Fmask"},
    }.get(collection)
    for key, asset in assets.items():
        if not only_main or (key in preferred if preferred else is_main_asset(key, asset, len(assets))):
            yield key, asset


def get_asset_size(session: requests.Session, url: str) -> int:
    try:
        response = session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return int(response.headers.get("content-length", 0))
    except (requests.RequestException, ValueError):
        return 0


def download_asset(
    session: requests.Session,
    url: str,
    destination: Path,
    on_chunk: Callable[[int], None] | None = None,
) -> int:
    temporary = destination.with_name(f"{destination.name}.part")
    downloaded = 0
    existing = temporary.stat().st_size if temporary.exists() else 0
    headers = {"Range": f"bytes={existing}-"} if existing else {}
    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers) as response:
            response.raise_for_status()
            resumed = existing > 0 and response.status_code == 206
            mode = "ab" if resumed else "wb"
            with open(temporary, mode) as file:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        if on_chunk:
                            on_chunk(len(chunk))
        os.replace(temporary, destination)
        return downloaded
    except Exception:
        raise
