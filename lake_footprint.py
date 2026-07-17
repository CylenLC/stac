"""Cached valid-data footprints for local GeoTIFF assets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.errors import RasterioError
from rasterio.features import shapes
from rasterio.warp import transform_geom
from shapely.geometry import Polygon, shape
from shapely.ops import unary_union

from lake_preview import PreviewError, source_asset_path


FOOTPRINT_VERSION = "2"


class FootprintError(ValueError):
    """Raised when a GeoTIFF footprint cannot be created safely."""


@dataclass(frozen=True)
class FootprintResult:
    geometry: dict[str, Any] | None
    cached: bool


def _cache_path(lake_root: Path, asset_id: str, source: Path) -> Path:
    stat = source.stat()
    fingerprint = "|".join((FOOTPRINT_VERSION, asset_id, str(stat.st_size), str(stat.st_mtime_ns)))
    token = hashlib.sha256(fingerprint.encode()).hexdigest()[:20]
    return lake_root / "cache" / "footprints" / f"{asset_id}-{token}.geojson"


def _read_cached_geometry(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if value is None:
        return None
    return value if isinstance(value, dict) and value.get("type") in {"Polygon", "MultiPolygon"} else None


def valid_data_footprint(lake_root: str | Path, asset: dict[str, Any]) -> FootprintResult:
    """Return a cached EPSG:4326 polygon covering non-NoData GeoTIFF pixels."""
    root = Path(lake_root).resolve()
    try:
        source = source_asset_path(root, asset)
    except PreviewError as exc:
        raise FootprintError(str(exc)) from exc
    output = _cache_path(root, str(asset["asset_id"]), source)
    if output.is_file():
        return FootprintResult(_read_cached_geometry(output), cached=True)

    try:
        with rasterio.open(source) as dataset:
            if dataset.crs is None:
                raise FootprintError("GeoTIFF has no coordinate reference system")
            valid = dataset.dataset_mask() > 0
            if not valid.any():
                geometry = None
            else:
                polygons = [
                    shape(native_geometry)
                    for native_geometry, value in shapes(valid.astype(np.uint8), mask=valid, transform=dataset.transform)
                    if value
                ]
                merged = unary_union(polygons)
                pixel_size = max(abs(dataset.transform.a), abs(dataset.transform.e))
                parts = list(merged.geoms) if merged.geom_type == "MultiPolygon" else [merged]
                largest = max(parts, key=lambda polygon: polygon.area)
                minimum_area = pixel_size * pixel_size * 64
                shells = [Polygon(part.exterior) for part in parts if part is largest or part.area >= minimum_area]
                coverage = unary_union(shells)
                simplified = coverage.simplify(pixel_size * 2, preserve_topology=True)
                geometry = transform_geom(dataset.crs, "EPSG:4326", simplified.__geo_interface__, precision=8)
    except RasterioError as exc:
        raise FootprintError(f"GeoTIFF footprint could not be read: {exc}") from exc

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(geometry, separators=(",", ":")), encoding="utf-8")
    return FootprintResult(geometry, cached=False)
