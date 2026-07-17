"""Read-only monitoring queries for an Earth Zarr Protocol lake."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from rasterio.crs import CRS
from rasterio.transform import Affine
from rasterio.warp import transform as transform_coordinates

from earth_lake import REGISTRY_SCHEMAS
from lake_footprint import FootprintError, valid_data_footprint


LAKE_LAYERS = (
    "protocol",
    "catalog",
    "registry",
    "source",
    "entities",
    "arrays",
    "virtual",
    "manifests",
    "cache",
)

JSON_COLUMNS = {
    "bbox_json",
    "geometry_json",
    "parameters_json",
    "input_asset_ids",
    "output_asset_ids",
    "keywords_json",
    "providers_json",
    "documentation_urls_json",
    "collection_metadata_json",
    "flag_values_json",
    "transform_json",
    "raster_metadata_json",
}


def _json_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for column in JSON_COLUMNS.intersection(result):
        normalized_name = column.removesuffix("_json")
        result[normalized_name] = _json_value(result.pop(column))
    return result


def _directory_stats(path: Path) -> dict[str, Any]:
    file_count = 0
    directory_count = 0
    byte_size = 0
    modified_at: float | None = None
    if path.exists():
        for entry in path.rglob("*"):
            try:
                stat = entry.stat()
            except OSError:
                continue
            modified_at = max(modified_at or stat.st_mtime, stat.st_mtime)
            if entry.is_file():
                file_count += 1
                byte_size += stat.st_size
            elif entry.is_dir():
                directory_count += 1
    return {
        "file_count": file_count,
        "directory_count": directory_count,
        "byte_size": byte_size,
        "modified_at": (
            datetime.fromtimestamp(modified_at, timezone.utc).isoformat() if modified_at else None
        ),
    }


class LakeMonitor:
    """Build API-friendly views over the protocol filesystem and registries."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.registry_dir = self.root / "registry"

    def registry_rows(
        self,
        table: str,
        *,
        offset: int = 0,
        limit: int = 100,
        query: str | None = None,
    ) -> dict[str, Any]:
        if table not in REGISTRY_SCHEMAS:
            raise KeyError(table)
        rows = self._read_registry(table)
        if query:
            needle = query.casefold()
            rows = [row for row in rows if needle in json.dumps(row, default=str).casefold()]
        rows = self._sort_rows(table, rows)
        return {
            "table": table,
            "columns": [field.name for field in REGISTRY_SCHEMAS[table]],
            "total": len(rows),
            "offset": offset,
            "limit": limit,
            "items": [_normalize_row(row) for row in rows[offset : offset + limit]],
        }

    def assets(
        self,
        *,
        product_id: str | None = None,
        status: str | None = None,
        query: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        rows = self._read_registry("assets")
        if product_id:
            rows = [row for row in rows if row.get("product_id") == product_id]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if query:
            needle = query.casefold()
            rows = [row for row in rows if needle in json.dumps(row, default=str).casefold()]
        rows = self._sort_rows("assets", rows)
        return {
            "total": len(rows),
            "offset": offset,
            "limit": limit,
            "items": [_normalize_row(row) for row in rows[offset : offset + limit]],
        }

    def asset(self, asset_id: str) -> dict[str, Any] | None:
        return next(
            (_normalize_row(row) for row in self._read_registry("assets") if row["asset_id"] == asset_id),
            None,
        )

    def spatial_assets(
        self,
        *,
        product_id: str | None = None,
        variable: str | None = None,
        status: str | None = None,
        query: str | None = None,
    ) -> dict[str, Any]:
        """Return registered asset coverage as lightweight GeoJSON features."""
        assets = self.assets(product_id=product_id, status=status, query=query, limit=10_000)["items"]
        features: list[dict[str, Any]] = []
        for asset in assets:
            if variable and asset.get("asset_key") != variable:
                continue
            preview_coordinates = self._preview_coordinates(asset)
            previewable = self._previewable(asset)
            valid_data_geometry = self._valid_data_geometry(asset) if previewable else None
            raster_geometry = self._coordinates_geometry(preview_coordinates)
            geometry = valid_data_geometry or raster_geometry or asset.get("geometry") or self._bbox_geometry(asset.get("bbox"))
            if not geometry:
                continue
            features.append(
                {
                    "type": "Feature",
                    "id": asset["asset_id"],
                    "geometry": geometry,
                    "properties": {
                        "asset_id": asset["asset_id"],
                        "product_id": asset.get("product_id"),
                        "variable": asset.get("asset_key"),
                        "datetime": asset.get("datetime"),
                        "status": asset.get("status"),
                        "source_item_id": asset.get("source_item_id"),
                        "byte_size": asset.get("byte_size"),
                        "previewable": previewable,
                        "preview_coordinates": preview_coordinates,
                        "preview_cache_key": self._preview_cache_key(asset) if previewable else None,
                        "geometry_source": (
                            "valid_data" if valid_data_geometry else "raster_grid" if raster_geometry else "stac" if asset.get("geometry") else "bbox"
                        ),
                    },
                }
            )
        return {"type": "FeatureCollection", "features": features}

    def products(self) -> list[dict[str, Any]]:
        assets = self._read_registry("assets")
        variables = self._read_registry("variables")
        products: list[dict[str, Any]] = []
        for row in self._sort_rows("products", self._read_registry("products")):
            product_assets = [asset for asset in assets if asset.get("product_id") == row["product_id"]]
            dates = sorted(asset["datetime"] for asset in product_assets if asset.get("datetime"))
            bboxes = [_json_value(asset.get("bbox_json")) for asset in product_assets]
            product_variables = [
                _normalize_row(variable)
                for variable in variables
                if variable.get("source_product") == row["product_id"]
            ]
            products.append(
                {
                    **_normalize_row(row),
                    "asset_count": len(product_assets),
                    "byte_size": sum(asset.get("byte_size") or 0 for asset in product_assets),
                    "variable_count": len(product_variables),
                    "variables": product_variables,
                    "start_datetime": dates[0] if dates else None,
                    "end_datetime": dates[-1] if dates else None,
                    "bboxes": [bbox for bbox in bboxes if bbox],
                }
            )
        return products

    def product(self, product_id: str) -> dict[str, Any] | None:
        return next((product for product in self.products() if product["product_id"] == product_id), None)

    def resources(self, layer: str, *, limit: int = 500) -> dict[str, Any]:
        if layer not in LAKE_LAYERS:
            raise KeyError(layer)
        root = self.root / layer
        items: list[dict[str, Any]] = []
        if root.exists():
            for entry in sorted(root.rglob("*"), key=lambda value: value.as_posix().casefold()):
                if len(items) >= limit:
                    break
                try:
                    stat = entry.stat()
                except OSError:
                    continue
                items.append(
                    {
                        "name": entry.name,
                        "path": entry.relative_to(self.root).as_posix(),
                        "kind": "directory" if entry.is_dir() else "file",
                        "suffix": entry.suffix.lower() if entry.is_file() else None,
                        "byte_size": stat.st_size if entry.is_file() else None,
                        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                    }
                )
        return {"layer": layer, "total": len(items), "items": items}

    def arrays(self) -> list[dict[str, Any]]:
        array_root = self.root / "arrays"
        stores: list[dict[str, Any]] = []
        candidates: set[Path] = set(array_root.rglob("*.zarr")) if array_root.exists() else set()
        for marker_name in ("zarr.json", ".zgroup", ".zmetadata"):
            candidates.update(marker.parent for marker in array_root.rglob(marker_name))
        for store in sorted(candidates):
            metadata = {}
            for marker_name in ("zarr.json", ".zmetadata", ".zgroup", ".zattrs"):
                marker = store / marker_name
                if marker.is_file():
                    try:
                        metadata[marker_name] = json.loads(marker.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        metadata[marker_name] = {"error": "metadata could not be read"}
            stats = _directory_stats(store)
            stores.append(
                {
                    "name": store.name,
                    "path": store.relative_to(self.root).as_posix(),
                    "metadata": metadata,
                    **stats,
                }
            )
        return stores

    def summary(self) -> dict[str, Any]:
        registry_counts = {
            name: len(self._read_registry(name)) for name in REGISTRY_SCHEMAS
        }
        layer_stats = [
            {"layer": layer, **_directory_stats(self.root / layer)} for layer in LAKE_LAYERS
        ]
        assets = self._read_registry("assets")
        registered_paths = {row.get("local_path") for row in assets if row.get("local_path")}
        missing_assets = sum(not (self.root / path).is_file() for path in registered_paths)
        source_files = {
            path.relative_to(self.root).as_posix()
            for path in (self.root / "source").rglob("*")
            if path.is_file() and path.name != "metadata.json" and path.suffix != ".part"
        } if (self.root / "source").exists() else set()
        runs = self._sort_rows("processing_runs", self._read_registry("processing_runs"))
        successful_runs = [run for run in runs if run.get("status") == "completed"]
        protocol = self._read_json(self.root / "protocol" / "schema_version.json")
        return {
            "root_name": self.root.name,
            "protocol": protocol,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "registry_counts": registry_counts,
            "asset_bytes": sum(row.get("byte_size") or 0 for row in assets),
            "available_assets": len(assets) - missing_assets,
            "missing_assets": missing_assets,
            "unregistered_source_files": len(source_files - registered_paths),
            "last_successful_run": successful_runs[0].get("end_time") if successful_runs else None,
            "layer_stats": layer_stats,
            "array_store_count": len(self.arrays()),
        }

    def protocol(self) -> dict[str, Any]:
        protocol_dir = self.root / "protocol"
        documents: dict[str, Any] = {}
        if protocol_dir.exists():
            for path in sorted(protocol_dir.rglob("*.json")):
                documents[path.relative_to(protocol_dir).as_posix()] = self._read_json(path)
        return documents

    def _read_registry(self, table: str) -> list[dict[str, Any]]:
        path = self.registry_dir / f"{table}.parquet"
        if not path.is_file():
            return []
        try:
            return pq.read_table(path).to_pylist()
        except (OSError, ValueError):
            return []

    @staticmethod
    def _sort_rows(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sort_field = {
            "assets": "updated_at",
            "processing_runs": "start_time",
            "products": "updated_at",
            "variables": "updated_at",
            "sources": "updated_at",
            "grids": "updated_at",
        }[table]
        return sorted(rows, key=lambda row: row.get(sort_field) or "", reverse=True)

    @staticmethod
    def _read_json(path: Path) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _previewable(self, asset: dict[str, Any]) -> bool:
        local_path = asset.get("local_path")
        if not isinstance(local_path, str) or not local_path.lower().endswith((".tif", ".tiff")):
            return False
        metadata = asset.get("raster_metadata")
        if not isinstance(metadata, dict) or not isinstance(metadata.get("width"), int) or not isinstance(metadata.get("height"), int):
            return False
        if not isinstance(metadata.get("transform"), list) or len(metadata["transform"]) != 6:
            return False
        candidate = (self.root / local_path).resolve()
        source_root = (self.root / "source").resolve()
        return candidate.is_relative_to(source_root) and candidate.is_file()

    def _preview_cache_key(self, asset: dict[str, Any]) -> str | None:
        local_path = asset.get("local_path")
        if not isinstance(local_path, str):
            return None
        candidate = (self.root / local_path).resolve()
        try:
            stat = candidate.stat()
        except OSError:
            return None
        return f"{stat.st_size:x}-{stat.st_mtime_ns:x}-preview-v1"

    def _valid_data_geometry(self, asset: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return valid_data_footprint(self.root, asset).geometry
        except FootprintError:
            return None

    @staticmethod
    def _preview_coordinates(asset: dict[str, Any]) -> list[list[float]] | None:
        """Build MapLibre image corners from the registered native raster grid."""
        metadata = asset.get("raster_metadata")
        if not isinstance(metadata, dict):
            return None
        coefficients = metadata.get("transform")
        width = metadata.get("width")
        height = metadata.get("height")
        if not isinstance(coefficients, list) or len(coefficients) != 6:
            return None
        if not isinstance(width, int) or not isinstance(height, int) or width < 1 or height < 1:
            return None
        try:
            source_crs = CRS.from_epsg(metadata["epsg"]) if metadata.get("epsg") else CRS.from_wkt(metadata["crs_wkt"])
            affine = Affine(*coefficients)
            native_corners = [affine * point for point in ((0, 0), (width, 0), (width, height), (0, height))]
            longitudes, latitudes = transform_coordinates(source_crs, "EPSG:4326", *zip(*native_corners))
            return [[float(longitude), float(latitude)] for longitude, latitude in zip(longitudes, latitudes)]
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _coordinates_geometry(coordinates: list[list[float]] | None) -> dict[str, Any] | None:
        """Turn raster corners into the same footprint shown by the preview overlay."""
        if not isinstance(coordinates, list) or len(coordinates) != 4:
            return None
        if not all(
            isinstance(point, list)
            and len(point) == 2
            and all(isinstance(value, (int, float)) for value in point)
            for point in coordinates
        ):
            return None
        return {"type": "Polygon", "coordinates": [coordinates + [coordinates[0]]]}

    @staticmethod
    def _bbox_geometry(bbox: Any) -> dict[str, Any] | None:
        if not isinstance(bbox, list) or len(bbox) != 4:
            return None
        west, south, east, north = bbox
        if not all(isinstance(value, (int, float)) for value in bbox):
            return None
        return {
            "type": "Polygon",
            "coordinates": [[[west, south], [east, south], [east, north], [west, north], [west, south]]],
        }
