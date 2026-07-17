"""Cached, browser-sized renderings for source GeoTIFF assets."""

from __future__ import annotations

import hashlib
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import NotGeoreferencedWarning, RasterioError
from rasterio.io import MemoryFile


PREVIEW_RENDER_VERSION = "1"
MAX_PREVIEW_SIZE = 2048


class PreviewError(ValueError):
    """Raised when an asset cannot safely be rendered as a preview."""


@dataclass(frozen=True)
class PreviewResult:
    path: Path
    cached: bool


def source_asset_path(lake_root: str | Path, asset: dict[str, Any]) -> Path:
    """Resolve a registry path, allowing only files in the immutable source layer."""
    root = Path(lake_root).resolve()
    source_root = (root / "source").resolve()
    local_path = asset.get("local_path")
    if not isinstance(local_path, str) or not local_path:
        raise PreviewError("Asset has no local source path")
    candidate = (root / local_path).resolve()
    if not candidate.is_relative_to(source_root):
        raise PreviewError("Asset preview is restricted to the source layer")
    if candidate.suffix.lower() not in {".tif", ".tiff"}:
        raise PreviewError("Asset is not a GeoTIFF")
    if not candidate.is_file():
        raise PreviewError("Local source file is missing")
    return candidate


def _cache_path(lake_root: Path, asset_id: str, source: Path, max_size: int, style: str) -> Path:
    stat = source.stat()
    fingerprint = "|".join(
        (PREVIEW_RENDER_VERSION, asset_id, str(stat.st_size), str(stat.st_mtime_ns), str(max_size), style)
    )
    token = hashlib.sha256(fingerprint.encode()).hexdigest()[:20]
    return lake_root / "cache" / "previews" / f"{asset_id}-{token}.png"


def _percentile_bounds(values: np.ndarray) -> tuple[float, float]:
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 1.0
    low, high = np.percentile(finite, (2, 98))
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        low = float(np.min(finite))
        high = float(np.max(finite))
    if high <= low:
        high = low + 1.0
    return float(low), float(high)


def _render_fmask(values: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Render HLS Fmask bits with a stable categorical preview palette."""
    result = np.zeros((*values.shape, 4), dtype=np.uint8)
    result[..., :3] = (83, 105, 96)  # clear/land
    result[..., 3] = np.where(valid, 210, 0)
    integer_values = np.where(valid, values, 0).astype(np.uint16, copy=False)
    palette = (
        (integer_values & 32 != 0, (54, 128, 190)),   # water
        (integer_values & 16 != 0, (241, 246, 249)),   # snow/ice
        (integer_values & 8 != 0, (92, 72, 111)),      # cloud shadow
        (integer_values & 4 != 0, (232, 158, 71)),     # adjacent cloud
        (integer_values & 2 != 0, (238, 112, 94)),     # cloud
        (integer_values & 1 != 0, (180, 119, 190)),    # cirrus
    )
    for mask, color in palette:
        apply = valid & mask
        result[apply, :3] = color
        result[apply, 3] = 235
    return result


def _render_rgba(data: np.ma.MaskedArray, style: str) -> np.ndarray:
    values = np.asarray(data.astype(np.float32).filled(np.nan), dtype=np.float32)
    valid = np.isfinite(values) & ~np.ma.getmaskarray(data)
    if style == "fmask":
        return _render_fmask(values, valid)
    low, high = _percentile_bounds(values[valid])
    scaled = np.clip((values - low) / (high - low), 0, 1)
    intensity = np.nan_to_num(scaled * 255, nan=0).astype(np.uint8)
    result = np.empty((*values.shape, 4), dtype=np.uint8)
    result[..., 0] = intensity
    result[..., 1] = intensity
    result[..., 2] = intensity
    result[..., 3] = np.where(valid, 235, 0)
    return result


def render_preview(
    lake_root: str | Path,
    asset: dict[str, Any],
    *,
    max_size: int = 1024,
    style: str = "auto",
) -> PreviewResult:
    """Create or reuse a PNG preview without mutating the source asset."""
    if not 64 <= max_size <= MAX_PREVIEW_SIZE:
        raise PreviewError(f"max_size must be between 64 and {MAX_PREVIEW_SIZE}")
    if style not in {"auto", "gray", "fmask"}:
        raise PreviewError("style must be one of: auto, gray, fmask")

    root = Path(lake_root).resolve()
    source = source_asset_path(root, asset)
    selected_style = "fmask" if style == "auto" and asset.get("asset_key") == "Fmask" else style
    output = _cache_path(root, str(asset["asset_id"]), source, max_size, selected_style)
    if output.is_file():
        return PreviewResult(path=output, cached=True)

    try:
        with rasterio.open(source) as dataset:
            scale = min(1.0, max_size / max(dataset.width, dataset.height))
            width = max(1, round(dataset.width * scale))
            height = max(1, round(dataset.height * scale))
            resampling = Resampling.nearest if selected_style == "fmask" else Resampling.bilinear
            data = dataset.read(
                1,
                out_shape=(height, width),
                masked=True,
                resampling=resampling,
            )
    except RasterioError as exc:
        raise PreviewError(f"GeoTIFF could not be read: {exc}") from exc

    output.parent.mkdir(parents=True, exist_ok=True)
    rgba = np.moveaxis(_render_rgba(data, selected_style), -1, 0)
    profile = {"driver": "PNG", "height": rgba.shape[1], "width": rgba.shape[2], "count": 4, "dtype": "uint8"}
    with warnings.catch_warnings():
        # PNG is intentionally a browser preview without a georeferencing payload.
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        with MemoryFile() as memory_file:
            with memory_file.open(**profile) as destination:
                destination.write(rgba)
            output.write_bytes(memory_file.read())
    return PreviewResult(path=output, cached=False)
