"""Read-only monitoring queries for an Earth Zarr Protocol lake."""

import json
import hashlib
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
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


def _directory_inventory(path: Path) -> tuple[dict[str, Any], set[Path]]:
    file_count = 0
    directory_count = 0
    byte_size = 0
    modified_at: float | None = None
    files_found: set[Path] = set()
    if path.exists():
        for current_root, directory_names, file_names in os.walk(path):
            directory_names[:] = [name for name in directory_names if not name.startswith("._") and name != ".DS_Store"]
            directory_count += len(directory_names)
            try:
                root_stat = os.stat(current_root)
            except OSError:
                root_stat = None
            if root_stat:
                modified_at = max(modified_at or root_stat.st_mtime, root_stat.st_mtime)
            for name in file_names:
                if name.startswith("._") or name == ".DS_Store":
                    continue
                entry = Path(current_root) / name
                try:
                    stat = entry.stat()
                except OSError:
                    continue
                file_count += 1
                byte_size += stat.st_size
                modified_at = max(modified_at or stat.st_mtime, stat.st_mtime)
                files_found.add(entry)
    stats = {
        "file_count": file_count,
        "directory_count": directory_count,
        "byte_size": byte_size,
        "modified_at": (
            datetime.fromtimestamp(modified_at, timezone.utc).isoformat() if modified_at else None
        ),
    }
    return stats, files_found


def _directory_stats(path: Path) -> dict[str, Any]:
    return _directory_inventory(path)[0]


class LakeMonitor:
    """Build API-friendly views over the protocol filesystem and registries."""

    REGISTRY_CACHE_TTL_SECONDS = 30.0
    RESOURCE_CACHE_TTL_SECONDS = 30.0

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.registry_dir = self.root / "registry"
        self._registry_cache: dict[str, tuple[tuple[int, int] | None, float, list[dict[str, Any]]]] = {}
        self._registry_lock = threading.RLock()
        self._registry_io_lock = threading.RLock()
        self._registry_refreshing: set[str] = set()
        self._protocol_schema: dict[str, Any] | None = None
        self._resource_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
        self._array_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._materialization_cache: tuple[float, dict[str, dict[str, Any]]] | None = None
        self._resource_lock = threading.RLock()

    def warm_registry(self) -> None:
        for table in REGISTRY_SCHEMAS:
            self._read_registry(table)
        schema = self._read_json(self.root / "protocol" / "schema_version.json")
        self._protocol_schema = schema if isinstance(schema, dict) else None

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
        exact: bool = True,
    ) -> dict[str, Any]:
        """Return registered asset coverage as GeoJSON features.

        Exact footprints touch the raster/footprint cache and are intended for a
        selected asset. Bulk browser requests should use ``exact=False`` so the
        response is built entirely from registry metadata.
        """
        assets = self.assets(product_id=product_id, status=status, query=query, limit=10_000)["items"]
        features: list[dict[str, Any]] = []
        for asset in assets:
            if variable and asset.get("asset_key") != variable:
                continue
            feature = self._spatial_feature(asset, exact=exact)
            if feature:
                features.append(feature)
        return {"type": "FeatureCollection", "features": features}

    def spatial_asset(self, asset_id: str, *, exact: bool = True) -> dict[str, Any] | None:
        asset = self.asset(asset_id)
        return self._spatial_feature(asset, exact=exact) if asset else None

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
        products = self.products()
        exact = next((product for product in products if product["product_id"] == product_id), None)
        if exact is not None:
            return exact
        # Compatibility for pre-identity URLs. Only return the legacy
        # Collection alias when it is unambiguous across catalogs.
        matches = [
            product
            for product in products
            if str(product.get("collection_id") or "").casefold() == product_id.casefold()
        ]
        return matches[0] if len(matches) == 1 else None

    def resources(self, layer: str, *, limit: int = 500) -> dict[str, Any]:
        if layer not in LAKE_LAYERS:
            raise KeyError(layer)
        cache_key = (layer, limit)
        now = time.monotonic()
        with self._resource_lock:
            cached = self._resource_cache.get(cache_key)
            if cached and now < cached[0]:
                return cached[1]
        root = self.root / layer
        materializations = self._materialization_index()
        items: list[dict[str, Any]] = []
        if root.exists():
            for current_root, directory_names, file_names in os.walk(root):
                directory_names[:] = sorted(
                    name
                    for name in directory_names
                    if not name.startswith(".") and not name.startswith("~$") and not name.endswith(".partial")
                )
                current = Path(current_root)
                if layer == "arrays" and current.name.endswith(".zarr"):
                    directory_names.clear()
                    continue
                for name in [*directory_names, *sorted(file_names)]:
                    if len(items) >= limit:
                        break
                    if name.startswith(".") or name.startswith("~$"):
                        continue
                    entry = current / name
                    try:
                        stat = entry.stat()
                        is_directory = entry.is_dir()
                    except OSError:
                        continue
                    relative_path = entry.relative_to(self.root).as_posix()
                    items.append(
                        {
                            "name": entry.name,
                            "path": relative_path,
                            "kind": "directory" if is_directory else "file",
                            "suffix": entry.suffix.lower() if not is_directory else None,
                            "byte_size": stat.st_size if not is_directory else None,
                            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                            "materialization": materializations.get(relative_path),
                        }
                    )
                if len(items) >= limit:
                    break
        result = {"layer": layer, "total": len(items), "items": items}
        with self._resource_lock:
            self._resource_cache[cache_key] = (
                time.monotonic() + self.RESOURCE_CACHE_TTL_SECONDS,
                result,
            )
        return result

    def arrays(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        with self._resource_lock:
            if self._array_cache and now < self._array_cache[0]:
                return self._array_cache[1]
        array_root = self.root / "arrays"
        materializations = self._materialization_index()
        stores: list[dict[str, Any]] = []
        candidates: set[Path] = set()
        if array_root.exists():
            for current_root, directory_names, file_names in os.walk(array_root):
                directory_names[:] = [
                    name
                    for name in directory_names
                    if not name.startswith(".") and not name.endswith(".partial")
                ]
                current = Path(current_root)
                if current.name.endswith(".zarr"):
                    candidates.add(current)
                    directory_names.clear()
                elif {"zarr.json", ".zgroup", ".zmetadata"}.intersection(file_names):
                    candidates.add(current)
                    directory_names.clear()
        for store in sorted(candidates):
            metadata = {}
            for marker_name in ("zarr.json", ".zmetadata", ".zgroup", ".zattrs"):
                marker = store / marker_name
                if marker.is_file():
                    try:
                        metadata[marker_name] = json.loads(marker.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        metadata[marker_name] = {"error": "metadata could not be read"}
            relative_path = store.relative_to(self.root).as_posix()
            materialization = materializations.get(relative_path)
            stats = (
                {
                    "file_count": materialization.get("file_count"),
                    "directory_count": materialization.get("directory_count"),
                    "byte_size": materialization.get("byte_size"),
                    "modified_at": materialization.get("updated_at"),
                }
                if materialization
                else _directory_stats(store)
            )
            stores.append(
                {
                    "name": store.name,
                    "path": relative_path,
                    "metadata": metadata,
                    "materialization": materialization,
                    **stats,
                }
            )
        with self._resource_lock:
            self._array_cache = (
                time.monotonic() + self.RESOURCE_CACHE_TTL_SECONDS,
                stores,
            )
        return stores

    def resource_detail(self, relative_path: str, *, sample_rows: int = 10) -> dict[str, Any]:
        """Return lightweight, inspectable metadata for a materialized entity file."""
        path = self._lake_path(relative_path)
        if not path.is_file():
            raise KeyError(relative_path)

        stat = path.stat()
        detail: dict[str, Any] = {
            "name": path.name,
            "path": path.relative_to(self.root).as_posix(),
            "suffix": path.suffix.lower(),
            "byte_size": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "materialization": self._materialization_index().get(path.relative_to(self.root).as_posix()),
        }
        if path.suffix.lower() not in {".parquet", ".geoparquet"}:
            return detail

        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow
        metadata = self._decode_metadata(schema.metadata)
        batch = next(parquet_file.iter_batches(batch_size=max(1, min(sample_rows, 20))), None)
        detail.update(
            {
                "format": "GeoParquet" if "geo" in metadata else "Parquet",
                "row_count": parquet_file.metadata.num_rows,
                "row_group_count": parquet_file.metadata.num_row_groups,
                "schema": [
                    {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                    for field in schema
                ],
                "metadata": metadata,
                "sample_rows": self._json_safe(batch.to_pylist()) if batch is not None else [],
            }
        )
        return detail

    def array_detail(self, relative_path: str) -> dict[str, Any]:
        """Return a compact Zarr store description without enumerating chunk files."""
        store = self._lake_path(relative_path)
        if not store.is_dir() or store.suffix != ".zarr":
            raise KeyError(relative_path)

        metadata = self._zarr_metadata(store)
        root = metadata.get("zarr.json") or metadata.get(".zgroup") or {}
        consolidated = (
            root.get("consolidated_metadata", {}).get("metadata", {})
            if isinstance(root, dict)
            else {}
        )
        variables: list[dict[str, Any]] = []
        for name, node in sorted(consolidated.items()):
            if not isinstance(node, dict) or node.get("node_type") != "array":
                continue
            attributes = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
            chunk_grid = node.get("chunk_grid") if isinstance(node.get("chunk_grid"), dict) else {}
            configuration = chunk_grid.get("configuration") if isinstance(chunk_grid.get("configuration"), dict) else {}
            variables.append(
                {
                    "name": name,
                    "shape": node.get("shape"),
                    "dtype": node.get("data_type"),
                    "dimensions": node.get("dimension_names") or attributes.get("_ARRAY_DIMENSIONS"),
                    "chunks": configuration.get("chunk_shape"),
                    "attributes": attributes,
                }
            )
        if not variables:
            zmetadata = metadata.get(".zmetadata")
            entries = zmetadata.get("metadata", {}) if isinstance(zmetadata, dict) else {}
            for key, node in sorted(entries.items()):
                if not key.endswith("/.zarray") or not isinstance(node, dict):
                    continue
                name = key.removesuffix("/.zarray")
                attributes = entries.get(f"{name}/.zattrs", {})
                attributes = attributes if isinstance(attributes, dict) else {}
                variables.append(
                    {
                        "name": name,
                        "shape": node.get("shape"),
                        "dtype": node.get("dtype"),
                        "dimensions": attributes.get("_ARRAY_DIMENSIONS"),
                        "chunks": node.get("chunks"),
                        "attributes": attributes,
                    }
                )
        if not variables:
            try:
                import zarr

                group = zarr.open_group(str(store), mode="r")
                for name, array in group.arrays():
                    attributes = dict(array.attrs)
                    variables.append(
                        {
                            "name": name,
                            "shape": list(array.shape),
                            "dtype": str(array.dtype),
                            "dimensions": attributes.get("_ARRAY_DIMENSIONS"),
                            "chunks": list(array.chunks) if array.chunks else None,
                            "attributes": attributes,
                        }
                    )
            except Exception:
                variables = []
        relative = store.relative_to(self.root).as_posix()
        materialization = self._materialization_index().get(relative)
        return {
            "name": store.name,
            "path": relative,
            "zarr_format": root.get("zarr_format") if isinstance(root, dict) else None,
            "attributes": root.get("attributes", {}) if isinstance(root, dict) else {},
            "variables": variables,
            "materialization": materialization,
        }

    def entity_page(
        self,
        relative_path: str,
        *,
        offset: int = 0,
        limit: int = 50,
        query: str | None = None,
    ) -> dict[str, Any]:
        path = self._entity_path(relative_path)
        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow
        geometry_column = self._geometry_column(schema.metadata)
        needle = (query or "").strip().casefold()
        items: list[dict[str, Any]] = []
        matched = 0
        for batch in parquet_file.iter_batches(batch_size=1024):
            for row in batch.to_pylist():
                display = {
                    key: self._json_safe(value)
                    for key, value in row.items()
                    if key != geometry_column
                }
                if needle and needle not in json.dumps(display, ensure_ascii=False, default=str).casefold():
                    continue
                if matched >= offset and len(items) < limit:
                    items.append(display)
                matched += 1
            if not needle and len(items) >= limit:
                break
        total = matched if needle else parquet_file.metadata.num_rows
        return {
            "path": relative_path,
            "columns": [field.name for field in schema if field.name != geometry_column],
            "geometry_column": geometry_column,
            "offset": offset,
            "limit": limit,
            "total": total,
            "next_offset": offset + len(items) if offset + len(items) < total else None,
            "items": items,
        }

    def entity_features(
        self,
        relative_path: str,
        *,
        offset: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        path = self._entity_path(relative_path)
        parquet_file = pq.ParquetFile(path)
        schema_metadata = parquet_file.schema_arrow.metadata
        geometry_column = self._geometry_column(schema_metadata)
        if not geometry_column or geometry_column not in parquet_file.schema_arrow.names:
            return {"type": "FeatureCollection", "features": [], "total": 0}
        try:
            from shapely import from_wkb
            from shapely.geometry import mapping
            from shapely.ops import transform as transform_geometry
        except ImportError as exc:
            raise RuntimeError("GeoParquet feature preview requires shapely") from exc
        transformer = self._geometry_transformer(schema_metadata, geometry_column)
        features: list[dict[str, Any]] = []
        seen = 0
        for batch in parquet_file.iter_batches(batch_size=512):
            for row in batch.to_pylist():
                if seen < offset:
                    seen += 1
                    continue
                if len(features) >= limit:
                    break
                geometry_value = row.pop(geometry_column, None)
                if geometry_value:
                    try:
                        parsed = from_wkb(geometry_value)
                        if transformer:
                            parsed = transform_geometry(transformer.transform, parsed)
                        geometry = mapping(parsed)
                    except Exception:
                        geometry = None
                else:
                    geometry = None
                features.append({
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": self._json_safe(row),
                })
                seen += 1
            if len(features) >= limit:
                break
        return {
            "type": "FeatureCollection",
            "features": features,
            "total": parquet_file.metadata.num_rows,
            "offset": offset,
            "limit": limit,
        }

    def array_slice(
        self,
        relative_path: str,
        variable: str,
        *,
        max_cells: int = 2500,
    ) -> dict[str, Any]:
        store = self._array_path(relative_path)
        marker = store / variable / "zarr.json"
        root_marker = store / "zarr.json"
        fingerprint_parts = [relative_path, variable, str(max_cells)]
        for path in (root_marker, marker):
            try:
                stat = path.stat()
                fingerprint_parts.extend((str(stat.st_size), str(stat.st_mtime_ns)))
            except OSError:
                fingerprint_parts.append("missing")
        cache_key = hashlib.sha256("|".join(fingerprint_parts).encode()).hexdigest()
        cache_path = self.root / "cache" / "zarr-slices" / f"{cache_key}.json"
        cached = self._read_json(cache_path)
        if isinstance(cached, dict):
            cached["cached"] = True
            return cached

        try:
            import zarr
        except ImportError as exc:
            raise RuntimeError("Zarr slice preview requires zarr") from exc
        group = zarr.open_group(str(store), mode="r")
        try:
            array = group[variable]
        except KeyError as exc:
            raise KeyError(variable) from exc
        shape = tuple(int(value) for value in array.shape)
        if not shape:
            selection: tuple[Any, ...] = ()
        elif len(shape) == 1:
            selection = (slice(0, min(shape[0], max_cells)),)
        else:
            side = max(1, int(math.sqrt(max_cells)))
            selection = (
                slice(0, min(shape[0], side)),
                slice(0, min(shape[1], side)),
                *(0 for _ in shape[2:]),
            )
        values = np.asarray(array[selection])
        numeric = np.issubdtype(values.dtype, np.number)
        finite = values[np.isfinite(values)] if numeric else np.asarray([])
        stats = {
            "count": int(values.size),
            "missing": int(values.size - finite.size) if numeric else 0,
            "min": self._scalar(finite.min()) if finite.size else None,
            "max": self._scalar(finite.max()) if finite.size else None,
            "mean": self._scalar(finite.mean()) if finite.size else None,
        }
        payload = {
            "path": relative_path,
            "variable": variable,
            "dtype": str(array.dtype),
            "shape": list(shape),
            "preview_shape": list(values.shape),
            "selection": [self._slice_label(value) for value in selection],
            "numeric": bool(numeric),
            "stats": stats,
            "values": self._array_values(values),
            "cached": False,
        }
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, cache_path)
        return payload

    def _materialization_index(self) -> dict[str, dict[str, Any]]:
        now = time.monotonic()
        with self._resource_lock:
            if self._materialization_cache and now < self._materialization_cache[0]:
                return self._materialization_cache[1]
        manifest = self._read_json(
            self.root / "manifests" / "materializations" / "hydrodatasets.json"
        )
        if not isinstance(manifest, dict) or not isinstance(manifest.get("records"), list):
            result: dict[str, dict[str, Any]] = {}
        else:
            result = {
                record["output"]: record
                for record in manifest["records"]
                if isinstance(record, dict)
                and isinstance(record.get("output"), str)
                and record["output"]
            }
        with self._resource_lock:
            self._materialization_cache = (
                time.monotonic() + self.RESOURCE_CACHE_TTL_SECONDS,
                result,
            )
        return result

    def _lake_path(self, relative_path: str) -> Path:
        path = (self.root / relative_path).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise KeyError(relative_path) from exc
        return path

    def _entity_path(self, relative_path: str) -> Path:
        path = self._lake_path(relative_path)
        try:
            path.relative_to(self.root / "entities")
        except ValueError as exc:
            raise KeyError(relative_path) from exc
        if not path.is_file() or path.suffix.lower() not in {".parquet", ".geoparquet"}:
            raise KeyError(relative_path)
        return path

    def _array_path(self, relative_path: str) -> Path:
        path = self._lake_path(relative_path)
        try:
            path.relative_to(self.root / "arrays")
        except ValueError as exc:
            raise KeyError(relative_path) from exc
        if not path.is_dir() or path.suffix != ".zarr":
            raise KeyError(relative_path)
        return path

    @staticmethod
    def _geometry_column(metadata: dict[bytes, bytes] | None) -> str | None:
        decoded = LakeMonitor._decode_metadata(metadata)
        geo = decoded.get("geo")
        return geo.get("primary_column") if isinstance(geo, dict) else None

    @staticmethod
    def _geometry_transformer(metadata: dict[bytes, bytes] | None, column: str):
        decoded = LakeMonitor._decode_metadata(metadata)
        geo = decoded.get("geo")
        columns = geo.get("columns") if isinstance(geo, dict) else None
        definition = columns.get(column) if isinstance(columns, dict) else None
        source_value = definition.get("crs") if isinstance(definition, dict) else None
        if source_value is None:
            return None
        try:
            from pyproj import CRS as PyprojCRS
            from pyproj import Transformer

            source = PyprojCRS.from_user_input(source_value)
            target = PyprojCRS.from_epsg(4326)
            return None if source.equals(target, ignore_axis_order=True) else Transformer.from_crs(source, target, always_xy=True)
        except Exception as exc:
            raise RuntimeError(f"GeoParquet CRS cannot be transformed to EPSG:4326: {exc}") from exc

    @staticmethod
    def _scalar(value: Any) -> Any:
        return value.item() if isinstance(value, np.generic) else value

    @classmethod
    def _array_values(cls, values: np.ndarray) -> Any:
        if values.ndim == 0:
            return cls._safe_scalar(values.item())
        return [cls._array_values(value) if isinstance(value, np.ndarray) else cls._safe_scalar(value) for value in values]

    @classmethod
    def _safe_scalar(cls, value: Any) -> Any:
        value = cls._scalar(value)
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, (datetime, np.datetime64)):
            return str(value)
        return value

    @staticmethod
    def _slice_label(value: Any) -> str | int:
        if isinstance(value, slice):
            return f"{value.start or 0}:{value.stop}:{value.step or 1}"
        return int(value)

    @staticmethod
    def _decode_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in (metadata or {}).items():
            name = key.decode("utf-8", errors="replace")
            text = value.decode("utf-8", errors="replace")
            try:
                result[name] = json.loads(text)
            except json.JSONDecodeError:
                result[name] = text
        return result

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, bytes):
            return f"<binary {len(value)} bytes>"
        if isinstance(value, dict):
            return {str(key): LakeMonitor._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [LakeMonitor._json_safe(item) for item in value]
        return value

    @staticmethod
    def _zarr_metadata(store: Path) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for marker_name in ("zarr.json", ".zmetadata", ".zgroup", ".zattrs"):
            marker = store / marker_name
            if marker.is_file():
                try:
                    metadata[marker_name] = json.loads(marker.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    metadata[marker_name] = {"error": "metadata could not be read"}
        return metadata

    def summary(self, *, scan_filesystem: bool = False) -> dict[str, Any]:
        registries = {name: self._read_registry(name) for name in REGISTRY_SCHEMAS}
        registry_counts = {name: len(rows) for name, rows in registries.items()}
        assets = registries["assets"]
        registered_paths = {row.get("local_path") for row in assets if row.get("local_path")}
        if scan_filesystem:
            layer_stats: list[dict[str, Any]] = []
            source_entries: set[Path] = set()
            for layer in LAKE_LAYERS:
                stats, entries = _directory_inventory(self.root / layer)
                layer_stats.append({"layer": layer, **stats})
                if layer == "source":
                    source_entries = entries
            source_files = {
                path.relative_to(self.root).as_posix()
                for path in source_entries
                if path.name != "metadata.json" and path.suffix != ".part"
            }
            missing_assets = len(registered_paths - source_files)
            unregistered_source_files = len(source_files - registered_paths)
        else:
            layer_stats = self._registry_layer_stats(registries)
            missing_assets = sum(row.get("status") == "missing" for row in assets)
            unregistered_source_files = 0
        runs = self._sort_rows("processing_runs", registries["processing_runs"])
        successful_runs = [run for run in runs if run.get("status") == "completed"]
        protocol = self._protocol_schema or self._read_json(self.root / "protocol" / "schema_version.json")
        return {
            "root_name": self.root.name,
            "protocol": protocol,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "registry_counts": registry_counts,
            "asset_bytes": sum(row.get("byte_size") or 0 for row in assets),
            "available_assets": len(assets) - missing_assets,
            "missing_assets": missing_assets,
            "unregistered_source_files": unregistered_source_files,
            "filesystem_scanned": scan_filesystem,
            "last_successful_run": successful_runs[0].get("end_time") if successful_runs else None,
            "layer_stats": layer_stats,
            "array_store_count": len(self.arrays()) if scan_filesystem else 0,
        }

    @staticmethod
    def _registry_layer_stats(registries: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        assets = registries["assets"]
        paths = [Path(row["local_path"]) for row in assets if row.get("local_path")]
        source_directories = {parent for path in paths for parent in path.parents if parent.as_posix() not in {".", "source"}}
        source_modified = max((row.get("updated_at") or "" for row in assets), default="") or None
        stats = {
            layer: {
                "file_count": 0,
                "directory_count": 0,
                "byte_size": 0,
                "modified_at": None,
            }
            for layer in LAKE_LAYERS
        }
        stats["source"].update(
            file_count=len(paths),
            directory_count=len(source_directories),
            byte_size=sum(row.get("byte_size") or 0 for row in assets),
            modified_at=source_modified,
        )
        stats["registry"].update(
            file_count=sum(bool(rows) for rows in registries.values()),
            modified_at=max(
                (row.get("updated_at") or row.get("start_time") or "" for rows in registries.values() for row in rows),
                default="",
            ) or None,
        )
        stats["catalog"].update(
            file_count=len({row.get("source_item_id") for row in assets if row.get("source_item_id")}),
            modified_at=source_modified,
        )
        stats["manifests"].update(
            file_count=len(registries["processing_runs"]),
            modified_at=max((row.get("end_time") or row.get("start_time") or "" for row in registries["processing_runs"]), default="") or None,
        )
        return [{"layer": layer, **stats[layer]} for layer in LAKE_LAYERS]

    def protocol(self) -> dict[str, Any]:
        protocol_dir = self.root / "protocol"
        documents: dict[str, Any] = {}
        if protocol_dir.exists():
            for path in sorted(protocol_dir.rglob("*.json")):
                if path.name.startswith("._"):
                    continue
                documents[path.relative_to(protocol_dir).as_posix()] = self._read_json(path)
        return documents

    def _read_registry(self, table: str) -> list[dict[str, Any]]:
        now = time.monotonic()
        with self._registry_lock:
            cached = self._registry_cache.get(table)
            if cached and now < cached[1]:
                return cached[2]
            if cached:
                if table not in self._registry_refreshing:
                    self._registry_refreshing.add(table)
                    threading.Thread(
                        target=self._refresh_registry,
                        args=(table,),
                        name=f"lake-registry-{table}",
                        daemon=True,
                    ).start()
                return cached[2]
        fingerprint, rows = self._load_registry(table)
        with self._registry_lock:
            self._registry_cache[table] = (fingerprint, now + self.REGISTRY_CACHE_TTL_SECONDS, rows)
        return rows

    def _refresh_registry(self, table: str) -> None:
        try:
            with self._registry_lock:
                cached = self._registry_cache.get(table)
            with self._registry_io_lock:
                path = self.registry_dir / f"{table}.parquet"
                try:
                    stat = path.stat()
                    fingerprint: tuple[int, int] | None = (stat.st_size, stat.st_mtime_ns)
                except OSError:
                    fingerprint = None
                if cached and cached[0] == fingerprint:
                    rows = cached[2]
                else:
                    fingerprint, rows = self._load_registry(table)
            with self._registry_lock:
                self._registry_cache[table] = (
                    fingerprint,
                    time.monotonic() + self.REGISTRY_CACHE_TTL_SECONDS,
                    rows,
                )
        finally:
            with self._registry_lock:
                self._registry_refreshing.discard(table)

    def _load_registry(self, table: str) -> tuple[tuple[int, int] | None, list[dict[str, Any]]]:
        with self._registry_io_lock:
            path = self.registry_dir / f"{table}.parquet"
            try:
                stat = path.stat()
                fingerprint: tuple[int, int] | None = (stat.st_size, stat.st_mtime_ns)
            except OSError:
                return None, []
            try:
                return fingerprint, pq.read_table(path).to_pylist()
            except (OSError, ValueError):
                return fingerprint, []

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
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    def _spatial_feature(self, asset: dict[str, Any], *, exact: bool) -> dict[str, Any] | None:
        preview_coordinates = self._preview_coordinates(asset)
        previewable = self._previewable(asset, verify_file=exact)
        valid_data_geometry = self._valid_data_geometry(asset) if exact and previewable else None
        raster_geometry = self._coordinates_geometry(preview_coordinates)
        geometry = valid_data_geometry or raster_geometry or asset.get("geometry") or self._bbox_geometry(asset.get("bbox"))
        if not geometry:
            return None
        return {
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

    def _previewable(self, asset: dict[str, Any], *, verify_file: bool = True) -> bool:
        local_path = asset.get("local_path")
        if not isinstance(local_path, str) or not local_path.lower().endswith((".tif", ".tiff")):
            return False
        metadata = asset.get("raster_metadata")
        if not isinstance(metadata, dict) or not isinstance(metadata.get("width"), int) or not isinstance(metadata.get("height"), int):
            return False
        if not isinstance(metadata.get("transform"), list) or len(metadata["transform"]) != 6:
            return False
        relative_path = Path(local_path)
        if relative_path.is_absolute() or not relative_path.parts or relative_path.parts[0] != "source" or ".." in relative_path.parts:
            return False
        return not verify_file or (self.root / relative_path).is_file()

    def _preview_cache_key(self, asset: dict[str, Any]) -> str | None:
        checksum = asset.get("checksum_sha256")
        byte_size = asset.get("byte_size")
        if not isinstance(checksum, str) or not checksum:
            return None
        size_token = f"{byte_size:x}" if isinstance(byte_size, int) else "unknown"
        return f"{checksum[:24]}-{size_token}-preview-v1"

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
