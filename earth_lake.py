"""Earth Zarr Protocol 0.1 source-layer catalog and registry maintenance."""

import hashlib
import json
import mimetypes
import os
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pystac
import rasterio
from rasterio.errors import RasterioError
from pystac.utils import str_to_datetime

from protocol_commit import ProtocolCommit
from stac_core import classify_asset
from stac_identity import AssetIdentity, collection_identity, safe_component
from stac_integrity import IntegrityError, IntegrityGate, IntegrityResult

PROTOCOL_NAME = "EarthZarrProtocol"
PROTOCOL_VERSION = "0.1.0"
PROCESSING_VERSION = "2026.07.1"

REGISTRY_SCHEMAS: dict[str, pa.Schema] = {
    "sources": pa.schema(
        [
            ("source_id", pa.string()),
            ("catalog", pa.string()),
            ("provider", pa.string()),
            ("endpoint", pa.string()),
            ("auth_type", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
        ]
    ),
    "products": pa.schema(
        [
            ("product_id", pa.string()),
            ("source_id", pa.string()),
            ("collection_id", pa.string()),
            ("product_name", pa.string()),
            ("product_version", pa.string()),
            ("modality", pa.string()),
            ("processing_level", pa.string()),
            ("license", pa.string()),
            ("title", pa.string()),
            ("description", pa.string()),
            ("keywords_json", pa.string()),
            ("providers_json", pa.string()),
            ("temporal_start", pa.string()),
            ("temporal_end", pa.string()),
            ("documentation_urls_json", pa.string()),
            ("spatial_resolution_m", pa.float64()),
            ("collection_metadata_json", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
        ]
    ),
    "variables": pa.schema(
        [
            ("variable_id", pa.string()),
            ("canonical_name", pa.string()),
            ("source_name", pa.string()),
            ("long_name", pa.string()),
            ("standard_name", pa.string()),
            ("unit", pa.string()),
            ("dtype", pa.string()),
            ("scale_factor", pa.float64()),
            ("add_offset", pa.float64()),
            ("central_wavelength_nm", pa.float64()),
            ("bandwidth_nm", pa.float64()),
            ("valid_min", pa.float64()),
            ("valid_max", pa.float64()),
            ("fill_value", pa.string()),
            ("nodata", pa.string()),
            ("flag_values_json", pa.string()),
            ("flag_meanings", pa.string()),
            ("quality_flag_definition", pa.string()),
            ("profile_version", pa.string()),
            ("modality", pa.string()),
            ("role", pa.string()),
            ("temporal_type", pa.string()),
            ("temporal_support", pa.string()),
            ("spatial_support", pa.string()),
            ("quality_variable", pa.string()),
            ("source_product", pa.string()),
            ("processing_level", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
        ]
    ),
    "grids": pa.schema(
        [
            ("grid_id", pa.string()),
            ("crs_wkt", pa.string()),
            ("epsg", pa.int32()),
            ("axis_order", pa.string()),
            ("x_origin", pa.float64()),
            ("y_origin", pa.float64()),
            ("pixel_size_x", pa.float64()),
            ("pixel_size_y", pa.float64()),
            ("tile_width", pa.int32()),
            ("tile_height", pa.int32()),
            ("tile_scheme", pa.string()),
            ("spatial_support", pa.string()),
            ("transform_json", pa.string()),
            ("width", pa.int32()),
            ("height", pa.int32()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
        ]
    ),
    "assets": pa.schema(
        [
            ("asset_id", pa.string()),
            ("catalog", pa.string()),
            ("collection_id", pa.string()),
            ("product_id", pa.string()),
            ("grid_id", pa.string()),
            ("source_item_id", pa.string()),
            ("asset_key", pa.string()),
            ("source_url", pa.string()),
            ("local_path", pa.string()),
            ("media_type", pa.string()),
            ("byte_size", pa.int64()),
            ("checksum_sha256", pa.string()),
            ("expected_size", pa.int64()),
            ("source_checksum", pa.string()),
            ("source_checksum_algorithm", pa.string()),
            ("checksum_verified", pa.bool_()),
            ("integrity_status", pa.string()),
            ("integrity_checks_json", pa.string()),
            ("integrity_reason", pa.string()),
            ("datetime", pa.string()),
            ("bbox_json", pa.string()),
            ("geometry_json", pa.string()),
            ("raster_metadata_json", pa.string()),
            ("status", pa.string()),
            ("run_id", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
        ]
    ),
    "processing_runs": pa.schema(
        [
            ("run_id", pa.string()),
            ("code_commit", pa.string()),
            ("container_image", pa.string()),
            ("input_asset_ids", pa.string()),
            ("output_asset_ids", pa.string()),
            ("parameters_json", pa.string()),
            ("start_time", pa.string()),
            ("end_time", pa.string()),
            ("status", pa.string()),
            ("checksum", pa.string()),
        ]
    ),
}

REGISTRY_KEYS = {
    "sources": "source_id",
    "products": "product_id",
    "variables": "variable_id",
    "grids": "grid_id",
    "assets": "asset_id",
    "processing_runs": "run_id",
}

SOURCE_INFO = {
    "microsoft": ("Microsoft Planetary Computer", "https://planetarycomputer.microsoft.com/api/stac/v1", "signed_url"),
    "earth-search": ("Element 84 Earth Search", "https://earth-search.aws.element84.com/v1", "none"),
    "nasa": ("NASA Earthdata", "https://cmr.earthdata.nasa.gov/search", "earthdata_netrc"),
}

HLS_PRODUCT_PROFILES: dict[str, dict[str, Any]] = {
    "HLSL30_V2.0": {
        "profile_version": "hls-v2.0-2026.07",
        "title": "Harmonized Landsat Sentinel-2 L30 Version 2.0",
        "description": (
            "Harmonized Landsat and Sentinel-2 surface reflectance observations "
            "derived from Landsat 8/9 at 30 metre spatial resolution."
        ),
        "keywords": ["HLS", "Landsat", "surface reflectance", "30 m", "NASA"],
        "providers": ["NASA", "LP DAAC"],
        "documentation_urls": ["https://lpdaac.usgs.gov/products/hlsl30v002/"],
        "spatial_resolution_m": 30.0,
        "bands": {
            "B01": ("surface_reflectance_coastal_aerosol", "Coastal aerosol surface reflectance", 443.0, 20.0),
            "B02": ("surface_reflectance_blue", "Blue surface reflectance", 482.0, 65.0),
            "B03": ("surface_reflectance_green", "Green surface reflectance", 561.0, 57.0),
            "B04": ("surface_reflectance_red", "Red surface reflectance", 655.0, 37.0),
            "B05": ("surface_reflectance_nir", "Near-infrared surface reflectance", 865.0, 28.0),
            "B06": ("surface_reflectance_swir1", "Shortwave-infrared 1 surface reflectance", 1609.0, 85.0),
            "B07": ("surface_reflectance_swir2", "Shortwave-infrared 2 surface reflectance", 2201.0, 187.0),
            "B09": ("surface_reflectance_cirrus", "Cirrus surface reflectance", 1373.0, 20.0),
            "B10": ("brightness_temperature_tirs1", "Thermal infrared 1 brightness temperature", 10895.0, 590.0),
            "B11": ("brightness_temperature_tirs2", "Thermal infrared 2 brightness temperature", 12005.0, 1010.0),
            "Fmask": ("quality_fmask", "HLS quality bit mask", None, None),
            "SZA": ("solar_zenith_angle", "Solar zenith angle", None, None),
            "SAA": ("solar_azimuth_angle", "Solar azimuth angle", None, None),
            "VZA": ("view_zenith_angle", "View zenith angle", None, None),
            "VAA": ("view_azimuth_angle", "View azimuth angle", None, None),
        },
    },
    "HLSS30_V2.0": {
        "profile_version": "hls-v2.0-2026.07",
        "title": "Harmonized Landsat Sentinel-2 S30 Version 2.0",
        "description": (
            "Harmonized Landsat and Sentinel-2 surface reflectance observations "
            "derived from Sentinel-2 at 30 metre spatial resolution."
        ),
        "keywords": ["HLS", "Sentinel-2", "surface reflectance", "30 m", "NASA"],
        "providers": ["NASA", "LP DAAC"],
        "documentation_urls": ["https://lpdaac.usgs.gov/products/hlss30v002/"],
        "spatial_resolution_m": 30.0,
        "bands": {
            "B01": ("surface_reflectance_coastal_aerosol", "Coastal aerosol surface reflectance", 443.0, 21.0),
            "B02": ("surface_reflectance_blue", "Blue surface reflectance", 497.0, 98.0),
            "B03": ("surface_reflectance_green", "Green surface reflectance", 560.0, 45.0),
            "B04": ("surface_reflectance_red", "Red surface reflectance", 665.0, 38.0),
            "B05": ("surface_reflectance_red_edge_1", "Red-edge 1 surface reflectance", 704.0, 19.0),
            "B06": ("surface_reflectance_red_edge_2", "Red-edge 2 surface reflectance", 740.0, 18.0),
            "B07": ("surface_reflectance_red_edge_3", "Red-edge 3 surface reflectance", 783.0, 28.0),
            "B08": ("surface_reflectance_nir_broad", "Broad near-infrared surface reflectance", 835.0, 145.0),
            "B8A": ("surface_reflectance_nir", "Near-infrared surface reflectance", 865.0, 33.0),
            "B09": ("surface_reflectance_water_vapour", "Water-vapour surface reflectance", 945.0, 26.0),
            "B11": ("surface_reflectance_swir1", "Shortwave-infrared 1 surface reflectance", 1614.0, 91.0),
            "B12": ("surface_reflectance_swir2", "Shortwave-infrared 2 surface reflectance", 2202.0, 175.0),
            "Fmask": ("quality_fmask", "HLS quality bit mask", None, None),
            "SZA": ("solar_zenith_angle", "Solar zenith angle", None, None),
            "SAA": ("solar_azimuth_angle", "Solar azimuth angle", None, None),
            "VZA": ("view_zenith_angle", "View zenith angle", None, None),
            "VAA": ("view_azimuth_angle", "View azimuth angle", None, None),
        },
    },
}

HLS_VARIABLE_OVERRIDES: dict[str, dict[str, dict[str, Any]]] = {
    "HLSL30_V2.0": {
        "B10": {"unit": "K", "scale_factor": 0.01},
        "B11": {"unit": "K", "scale_factor": 0.01},
        "SZA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "SAA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "VZA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "VAA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
    },
    "HLSS30_V2.0": {
        "SZA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "SAA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "VZA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
        "VAA": {"unit": "degree", "scale_factor": 0.01, "role": "ancillary"},
    },
}

HLS_VARIABLES = {
    "HLSL30_V2.0": {
        "B02": ("surface_reflectance_blue", "Blue surface reflectance"),
        "B03": ("surface_reflectance_green", "Green surface reflectance"),
        "B04": ("surface_reflectance_red", "Red surface reflectance"),
        "B05": ("surface_reflectance_nir", "Near-infrared surface reflectance"),
        "B06": ("surface_reflectance_swir1", "Shortwave-infrared 1 surface reflectance"),
        "B07": ("surface_reflectance_swir2", "Shortwave-infrared 2 surface reflectance"),
        "Fmask": ("quality_fmask", "HLS quality bit mask"),
    },
    "HLSS30_V2.0": {
        "B02": ("surface_reflectance_blue", "Blue surface reflectance"),
        "B03": ("surface_reflectance_green", "Green surface reflectance"),
        "B04": ("surface_reflectance_red", "Red surface reflectance"),
        "B8A": ("surface_reflectance_nir", "Near-infrared surface reflectance"),
        "B11": ("surface_reflectance_swir1", "Shortwave-infrared 1 surface reflectance"),
        "B12": ("surface_reflectance_swir2", "Shortwave-infrared 2 surface reflectance"),
        "Fmask": ("quality_fmask", "HLS quality bit mask"),
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_raster(path: Path) -> dict[str, Any] | None:
    """Read native GeoTIFF metadata without making a failed probe fatal to ingestion."""
    try:
        with path.open("rb") as file:
            header = file.read(4)
        if header not in {b"II*\x00", b"MM\x00*", b"II+\x00", b"MM\x00+"}:
            return None
        with rasterio.open(path) as dataset:
            if dataset.driver != "GTiff":
                return None
            transform = dataset.transform
            epsg = dataset.crs.to_epsg() if dataset.crs else None
            tags = dataset.tags()
            return {
                "driver": dataset.driver,
                "crs_wkt": dataset.crs.to_wkt() if dataset.crs else None,
                "epsg": epsg,
                "transform": [transform.a, transform.b, transform.c, transform.d, transform.e, transform.f],
                "x_origin": transform.c,
                "y_origin": transform.f,
                "pixel_size_x": transform.a,
                "pixel_size_y": transform.e,
                "width": dataset.width,
                "height": dataset.height,
                "band_count": dataset.count,
                "dtype": dataset.dtypes[0] if dataset.count else None,
                "nodata": str(dataset.nodata) if dataset.nodata is not None else None,
                "description": dataset.descriptions[0] if dataset.descriptions else None,
                "tags": tags,
                "bounds": list(dataset.bounds),
            }
    except (RasterioError, OSError, ValueError):
        return None


def collection_details(collection: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize STAC Collection or CMR collection responses into product metadata."""
    profile = HLS_PRODUCT_PROFILES.get(collection, {})
    metadata = metadata or {}
    is_stac = metadata.get("type") == "Collection" or "extent" in metadata
    links = metadata.get("links", [])
    if is_stac:
        interval = metadata.get("extent", {}).get("temporal", {}).get("interval", [[None, None]])
        start, end = interval[0] if interval else (None, None)
        providers = [
            provider.get("name") if isinstance(provider, dict) else str(provider)
            for provider in metadata.get("providers", [])
        ]
        resolution = metadata.get("summaries", {}).get("gsd") or metadata.get("gsd")
        if isinstance(resolution, list):
            resolution = next((value for value in resolution if isinstance(value, (float, int))), None)
    else:
        start, end = metadata.get("time_start"), metadata.get("time_end")
        providers = metadata.get("providers") or metadata.get("organizations") or []
        resolution = metadata.get("spatial_resolution")
    documentation_urls = [
        link.get("href")
        for link in links
        if isinstance(link, dict)
        and link.get("href")
        and link.get("rel", "").lower() in {"about", "documentation", "describedby", "license", "metadata"}
    ]
    raw_keywords = metadata.get("keywords") or []
    keywords = [str(value) for value in raw_keywords] if isinstance(raw_keywords, list) else [str(raw_keywords)]
    return {
        "title": metadata.get("title") or profile.get("title") or collection,
        "description": metadata.get("description") or metadata.get("summary") or profile.get("description"),
        "license": metadata.get("license") or profile.get("license") or "unknown",
        "keywords": list(dict.fromkeys(keywords or profile.get("keywords", []))),
        "providers": list(dict.fromkeys([str(value) for value in providers if value] or profile.get("providers", []))),
        "temporal_start": start,
        "temporal_end": end,
        "documentation_urls": list(dict.fromkeys(documentation_urls or profile.get("documentation_urls", []))),
        "spatial_resolution_m": float(resolution) if isinstance(resolution, (float, int)) else profile.get("spatial_resolution_m"),
        "raw": metadata or profile,
    }


def optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class EarthLake:
    _lock = threading.RLock()
    _recovered_roots: set[Path] = set()

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.protocol_dir = self.root / "protocol"
        self.registry_dir = self.root / "registry"
        self.stac_dir = self.root / "catalog" / "stac"
        self.source_dir = self.root / "source"
        self._active_commit: ProtocolCommit | None = None
        self._recover_protocol_commits()
        self.initialize()

    def _recover_protocol_commits(self) -> None:
        with self._lock:
            if self.root not in self._recovered_roots:
                ProtocolCommit.recover(self.root)
                self._recovered_roots.add(self.root)

    def initialize(self) -> None:
        directories = [
            self.protocol_dir / "controlled_vocabularies",
            self.stac_dir / "collections",
            self.registry_dir,
            self.source_dir,
            self.root / "entities" / "basins",
            self.root / "entities" / "stations",
            self.root / "entities" / "rivers",
            self.root / "entities" / "patches",
            self.root / "arrays" / "observations",
            self.root / "arrays" / "forcing",
            self.root / "arrays" / "static",
            self.root / "arrays" / "hydrology",
            self.root / "virtual" / "kerchunk",
            self.root / "virtual" / "virtualizarr",
            self.root / "manifests" / "pretraining",
            self.root / "cache" / "embeddings",
            self.root / "cache" / "normalized",
            self.root / "cache" / "compiled_samples",
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        self._write_json_if_missing(
            self.protocol_dir / "earth_zarr_protocol.json",
            {
                "protocol": PROTOCOL_NAME,
                "protocol_version": PROTOCOL_VERSION,
                "storage_layers": ["source", "array_vector", "catalog", "semantic_registry", "sample"],
                "principles": [
                    "native_resolution",
                    "independent_time_axes",
                    "explicit_missing_modalities",
                    "immutable_source_assets",
                    "traceable_lineage",
                ],
                "source_root": "source/",
                "array_root": "arrays/",
                "catalog_root": "catalog/stac/",
                "registry_root": "registry/",
                "manifest_root": "manifests/",
            },
        )
        self._write_json_if_missing(
            self.protocol_dir / "schema_version.json",
            {"protocol": PROTOCOL_NAME, "version": PROTOCOL_VERSION, "processing_version": PROCESSING_VERSION},
        )
        vocab = self.protocol_dir / "controlled_vocabularies"
        self._write_json_if_missing(vocab / "modalities.json", ["observation", "forcing", "static", "hydrology"])
        self._write_json_if_missing(
            vocab / "temporal_types.json",
            ["instantaneous", "mean", "minimum", "maximum", "accumulation", "composite", "static"],
        )
        self._write_json_if_missing(
            vocab / "quality_flags.json",
            {
                "0": "missing",
                "1": "cloud",
                "2": "cloud_shadow",
                "3": "snow",
                "4": "saturation",
                "5": "low_quality",
                "6": "interpolated",
                "7": "temporally_filled",
            },
        )
        for name, schema in REGISTRY_SCHEMAS.items():
            self._ensure_registry_schema(
                name,
                schema,
                create_if_missing=name != "processing_runs",
            )
        self._migrate_asset_registry_identity()
        catalog_path = self.stac_dir / "catalog.json"
        if not catalog_path.exists():
            pystac.Catalog(
                id="earth-lake",
                description="Earth Zarr Protocol source and materialized asset catalog",
                title="Earth Lake",
            ).normalize_and_save(str(self.stac_dir), catalog_type=pystac.CatalogType.SELF_CONTAINED)

    def source_item_directory(self, catalog: str, collection: object, item_id: object) -> Path:
        path = (
            self.source_dir
            / safe_component(catalog)
            / safe_component(collection)
            / safe_component(item_id)
        )
        if self.root not in path.resolve().parents:
            raise ValueError("Source path escapes the Earth Lake root")
        return path

    def start_run(self, parameters: dict[str, Any], run_id: str | None = None) -> str:
        run_id = run_id or f"run-{uuid.uuid4()}"
        existing = self.processing_run(run_id)
        if existing:
            self._validate_acquisition_processing_association_for_id(existing, run_id)
            return run_id
        row = {
            "run_id": run_id,
            "code_commit": self._code_commit(),
            "container_image": os.environ.get("CONTAINER_IMAGE", ""),
            "input_asset_ids": "[]",
            "output_asset_ids": "[]",
            "parameters_json": json.dumps(parameters, sort_keys=True),
            "start_time": utc_now(),
            "end_time": None,
            "status": "running",
            "checksum": None,
        }
        self._validate_acquisition_processing_association_for_id(row, run_id)
        self._commit_processing_row(row, kind="processing_run_start")
        return run_id

    def output_asset_ids(self, run_id: str) -> list[str]:
        return sorted({str(row["asset_id"]) for row in self._read_rows("assets") if row.get("run_id") == run_id})

    def processing_run(self, run_id: str) -> dict[str, Any] | None:
        """Return the unique processing record or fail on duplicate identity."""

        rows = [row for row in self._read_rows("processing_runs") if row.get("run_id") == run_id]
        if len(rows) > 1:
            raise ValueError(f"processing registry contains duplicate run identity: {run_id}")
        return rows[0] if rows else None

    @staticmethod
    def _acquisition_run_id_for_processing_id(run_id: str) -> str | None:
        if run_id.startswith("acq-") and len(run_id) > len("acq-"):
            return run_id.removeprefix("acq-")
        return None

    def _validate_acquisition_processing_association_for_id(
        self,
        row: dict[str, Any],
        processing_run_id: str,
    ) -> None:
        acquisition_run_id = self._acquisition_run_id_for_processing_id(processing_run_id)
        if acquisition_run_id is None:
            return
        if str(row.get("run_id") or "") != processing_run_id:
            raise ValueError(
                f"processing run identity mismatch: expected {processing_run_id}, "
                f"found {row.get('run_id')!r}"
            )
        raw_parameters = row.get("parameters_json")
        try:
            parameters = json.loads(raw_parameters) if isinstance(raw_parameters, str) else None
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"processing run {processing_run_id} has malformed parameters_json"
            ) from exc
        if not isinstance(parameters, dict) or parameters.get("acquisition_run_id") != acquisition_run_id:
            raise ValueError(
                f"processing run {processing_run_id} is not associated with acquisition "
                f"run {acquisition_run_id}"
            )

    def finish_run(self, run_id: str, status: str, output_asset_ids: list[str]) -> None:
        row = self.processing_run(run_id)
        if not row:
            raise KeyError(f"Unknown processing run: {run_id}")
        self._validate_acquisition_processing_association_for_id(row, run_id)
        outputs = sorted({str(asset_id) for asset_id in output_asset_ids})
        row.update(
            output_asset_ids=json.dumps(outputs),
            end_time=utc_now(),
            status=status,
            checksum=hashlib.sha256("\n".join(outputs).encode()).hexdigest(),
        )
        self._commit_processing_row(row, kind="processing_run_finish")

    def reconcile_processing_run(
        self,
        run_id: str,
        status: str,
        output_asset_ids: list[str],
        *,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Rebuild or converge processing provenance without touching Assets."""

        row = self.processing_run(run_id)
        if row is None:
            self.start_run(
                parameters or {"interface": "acquisition", "acquisition_run_id": run_id.removeprefix("acq-")},
                run_id=run_id,
            )
        else:
            self._validate_acquisition_processing_association_for_id(row, run_id)
        self.finish_run(run_id, status, output_asset_ids)
        row = self.processing_run(run_id)
        if row is None:
            raise RuntimeError(f"processing finalization did not publish run {run_id}")

        expected_outputs = sorted({str(asset_id) for asset_id in output_asset_ids})
        try:
            recorded_outputs = sorted({str(asset_id) for asset_id in json.loads(row.get("output_asset_ids") or "[]")})
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(f"processing run {run_id} has invalid output_asset_ids") from exc
        expected_checksum = hashlib.sha256("\n".join(expected_outputs).encode()).hexdigest()
        if (
            row.get("status") != status
            or recorded_outputs != expected_outputs
            or row.get("checksum") != expected_checksum
        ):
            raise RuntimeError(f"processing finalization verification failed for run {run_id}")
        return row

    def _commit_processing_row(self, row: dict[str, Any], *, kind: str) -> None:
        """Publish one processing registry update through the lake protocol."""

        self._validate_acquisition_processing_association_for_id(row, str(row.get("run_id") or ""))
        self._recover_protocol_commits()
        metadata = {"run_id": str(row["run_id"]), "status": str(row.get("status") or "")}
        try:
            with self._lock, ProtocolCommit(self.root, kind=kind, metadata=metadata) as commit:
                previous_commit = self._active_commit
                self._active_commit = commit
                try:
                    self._upsert("processing_runs", row)
                finally:
                    self._active_commit = previous_commit
        except Exception:
            with self._lock:
                self._recovered_roots.discard(self.root)
            raise

    def reindex_source_assets(
        self,
        collection_metadata_by_id: dict[str, dict[str, Any]] | None = None,
    ) -> list[str]:
        """Backfill registry metadata from already downloaded source assets."""
        collection_metadata_by_id = collection_metadata_by_id or {}
        asset_ids: list[str] = []
        for asset in self._read_rows("assets"):
            path = self.root / asset["local_path"]
            if not path.is_file():
                continue
            item = self._read_item_metadata(path.parent / "metadata.json") or {
                "id": asset["source_item_id"],
                "collection": asset["product_id"],
                "properties": {"datetime": asset.get("datetime")},
            }
            collection = str(item.get("collection") or asset["product_id"])
            asset_ids.append(
                self.record_asset(
                    run_id=asset.get("run_id") or "reindex",
                    catalog=asset["local_path"].split("/", 2)[1] if asset["local_path"].startswith("source/") else "unknown",
                    item=item,
                    asset_key=asset["asset_key"],
                    source_url=asset["source_url"],
                    local_path=path,
                    status=asset["status"],
                    collection_metadata=collection_metadata_by_id.get(collection),
                )
            )
        referenced_grid_ids = {row.get("grid_id") for row in self._read_rows("assets") if row.get("grid_id")}
        grids = self._read_rows("grids")
        stale_hls_grids = [
            row
            for row in grids
            if row["grid_id"] not in referenced_grid_ids
            and row["grid_id"].startswith("hls_mgrs_30m")
            and row.get("epsg") is None
        ]
        if stale_hls_grids:
            self._write_table(
                self.registry_dir / "grids.parquet",
                [row for row in grids if row not in stale_hls_grids],
                REGISTRY_SCHEMAS["grids"],
            )
        return asset_ids

    def record_asset(
        self,
        *,
        run_id: str,
        catalog: str,
        item: dict[str, Any],
        asset_key: str,
        source_url: str,
        local_path: str | Path,
        status: str,
        collection_metadata: dict[str, Any] | None = None,
        integrity: IntegrityResult | dict[str, Any] | None = None,
        asset_category: str | None = None,
    ) -> str:
        self._recover_protocol_commits()
        metadata = {
            "run_id": run_id,
            "catalog": catalog,
            "collection": str(item.get("collection") or "unknown"),
            "source_item_id": str(item.get("id") or "unknown"),
            "asset_key": asset_key,
        }
        try:
            with self._lock, ProtocolCommit(self.root, kind="record_asset", metadata=metadata) as commit:
                commit.prepare_tree(self.stac_dir)
                self._active_commit = commit
                try:
                    return self._record_asset(
                        run_id=run_id,
                        catalog=catalog,
                        item=item,
                        asset_key=asset_key,
                        source_url=source_url,
                        local_path=local_path,
                        status=status,
                        collection_metadata=collection_metadata,
                        integrity=integrity,
                        asset_category=asset_category,
                    )
                finally:
                    self._active_commit = None
        except Exception:
            with self._lock:
                self._recovered_roots.discard(self.root)
            raise

    def _record_asset(
        self,
        *,
        run_id: str,
        catalog: str,
        item: dict[str, Any],
        asset_key: str,
        source_url: str,
        local_path: str | Path,
        status: str,
        collection_metadata: dict[str, Any] | None = None,
        integrity: IntegrityResult | dict[str, Any] | None = None,
        asset_category: str | None = None,
    ) -> str:
        path = Path(local_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        source_asset = (item.get("assets") or {}).get(asset_key)
        source_asset = source_asset if isinstance(source_asset, dict) else None
        asset_category = asset_category or classify_asset(asset_key, source_asset or {})
        if integrity is not None:
            integrity_data = integrity.to_dict() if isinstance(integrity, IntegrityResult) else dict(integrity)
            if not integrity_data.get("ok"):
                raise IntegrityError(
                    IntegrityResult(
                        ok=False,
                        checks=integrity_data.get("checks") or {},
                        expected_size=integrity_data.get("expected_size"),
                        actual_size=int(integrity_data.get("actual_size") or 0),
                        source_checksum=integrity_data.get("source_checksum"),
                        source_checksum_algorithm=integrity_data.get("source_checksum_algorithm"),
                        local_sha256=integrity_data.get("local_sha256"),
                        checksum_verified=integrity_data.get("checksum_verified"),
                        detected_type=integrity_data.get("detected_type"),
                        response_status=integrity_data.get("response_status"),
                        response_content_type=integrity_data.get("response_content_type"),
                        reason_code=integrity_data.get("reason_code"),
                        reason=integrity_data.get("reason"),
                        validated_path=integrity_data.get("validated_path"),
                        validated_size=integrity_data.get("validated_size"),
                        validated_mtime_ns=integrity_data.get("validated_mtime_ns"),
                        validated_sha256=integrity_data.get("validated_sha256"),
                    )
                )
            stat = path.stat()
            bound_path = integrity_data.get("validated_path")
            bound_size = integrity_data.get("validated_size")
            bound_mtime = integrity_data.get("validated_mtime_ns")
            bound_sha256 = integrity_data.get("validated_sha256") or integrity_data.get("local_sha256")
            current_sha256 = sha256_file(path) if isinstance(bound_sha256, str) else None
            if (
                bound_path != str(path)
                or bound_size != stat.st_size
                or bound_mtime != stat.st_mtime_ns
                or not isinstance(bound_sha256, str)
                or current_sha256 != bound_sha256
            ):
                # A passing result is reusable only for the exact published
                # file it validated. Re-run the gate when callers provide an
                # unbound or stale result instead of trusting its boolean.
                validation_asset = source_asset or {"href": source_url or path.name}
                rebound = IntegrityGate.validate(path, validation_asset)
                if not rebound.ok:
                    raise IntegrityError(rebound)
                integrity_data = rebound.to_dict()
        else:
            # Keep direct/reindex callers behind the same publication gate as
            # the acquisition manager.  Metadata unavailable to a legacy
            # caller is intentionally treated as optional, but the local file
            # must still be non-empty and structurally valid when its suffix
            # identifies a raster or JSON document.
            inferred_asset = {"href": source_url or path.name}
            integrity_result = IntegrityGate.validate(path, inferred_asset)
            if not integrity_result.ok:
                raise IntegrityError(integrity_result)
            integrity_data = integrity_result.to_dict()
        try:
            relative_path = path.relative_to(self.root).as_posix()
        except ValueError as exc:
            raise ValueError("Registered assets must be inside the Earth Lake root") from exc

        now = utc_now()
        identity = AssetIdentity.from_item(catalog, item, asset_key)
        collection = identity.collection
        item_id = identity.item_id
        source_id = catalog
        product_id = collection_identity(catalog, collection).lower()
        asset_id = identity.digest()
        existing_asset = self._find("assets", asset_id)
        lineage_run_id = existing_asset.get("run_id") if existing_asset else run_id
        provider, endpoint, auth_type = SOURCE_INFO.get(catalog, (catalog, "", "unknown"))
        modality, version, fallback_grid_id = self._product_semantics(collection)
        raster_metadata = inspect_raster(path)
        grid_id = self._grid_id(fallback_grid_id, raster_metadata)
        product_details = collection_details(collection, collection_metadata)

        self._upsert(
            "sources",
            {
                "source_id": source_id,
                "catalog": catalog,
                "provider": provider,
                "endpoint": endpoint,
                "auth_type": auth_type,
                "created_at": now,
                "updated_at": now,
            },
            preserve_created=True,
        )
        self._upsert(
            "products",
            {
                "product_id": product_id,
                "source_id": source_id,
                "collection_id": collection,
                "product_name": collection,
                "product_version": version,
                "modality": modality,
                "processing_level": "source",
                "license": product_details["license"],
                "title": product_details["title"],
                "description": product_details["description"],
                "keywords_json": json.dumps(product_details["keywords"]),
                "providers_json": json.dumps(product_details["providers"]),
                "temporal_start": product_details["temporal_start"],
                "temporal_end": product_details["temporal_end"],
                "documentation_urls_json": json.dumps(product_details["documentation_urls"]),
                "spatial_resolution_m": product_details["spatial_resolution_m"],
                "collection_metadata_json": json.dumps(product_details["raw"]),
                "created_at": now,
                "updated_at": now,
            },
            preserve_created=True,
        )
        self._register_grid(grid_id, collection, raster_metadata, now)
        self._register_variable(product_id, collection, asset_key, modality, raster_metadata, now)

        geometry = item.get("geometry")
        bbox = item.get("bbox")
        acquired = item.get("properties", {}).get("datetime")
        checksum = integrity_data.get("local_sha256") or sha256_file(path)
        self._upsert(
            "assets",
            {
                "asset_id": asset_id,
                "catalog": catalog,
                "collection_id": collection,
                "product_id": product_id,
                "grid_id": grid_id,
                "source_item_id": item_id,
                "asset_key": asset_key,
                "source_url": source_url,
                "local_path": relative_path,
                "media_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                "byte_size": path.stat().st_size,
                "checksum_sha256": checksum,
                "expected_size": integrity_data.get("expected_size"),
                "source_checksum": integrity_data.get("source_checksum"),
                "source_checksum_algorithm": integrity_data.get("source_checksum_algorithm"),
                "checksum_verified": integrity_data.get("checksum_verified"),
                "integrity_status": "passed" if integrity_data.get("ok") else "legacy_unverified",
                "integrity_checks_json": json.dumps(integrity_data.get("checks") or {}, sort_keys=True),
                "integrity_reason": integrity_data.get("reason"),
                "datetime": acquired,
                "bbox_json": json.dumps(bbox) if bbox is not None else None,
                "geometry_json": json.dumps(geometry) if geometry is not None else None,
                "raster_metadata_json": json.dumps(raster_metadata) if raster_metadata else None,
                "status": existing_asset.get("status", status) if existing_asset else status,
                "run_id": lineage_run_id,
                "created_at": now,
                "updated_at": now,
            },
            preserve_created=True,
        )
        self._update_stac(
            catalog=catalog,
            collection=collection,
            item_id=item_id,
            item=item,
            asset_key=asset_key,
            asset_id=asset_id,
            path=path,
            checksum=checksum,
            integrity=integrity_data,
            grid_id=grid_id,
            version=version,
            run_id=lineage_run_id,
            collection_details=product_details,
            asset_roles=(source_asset or {}).get("roles"),
            asset_media_type=(source_asset or {}).get("type") or (source_asset or {}).get("media_type"),
            asset_category=asset_category,
        )
        return asset_id

    @staticmethod
    def _grid_id(fallback_grid_id: str, raster_metadata: dict[str, Any] | None) -> str:
        if not raster_metadata:
            return fallback_grid_id
        fingerprint = json.dumps(
            {
                "epsg": raster_metadata.get("epsg"),
                "transform": raster_metadata.get("transform"),
                "width": raster_metadata.get("width"),
                "height": raster_metadata.get("height"),
            },
            sort_keys=True,
        )
        suffix = hashlib.sha1(fingerprint.encode()).hexdigest()[:12]
        projection = f"epsg{raster_metadata['epsg']}" if raster_metadata.get("epsg") else "native"
        return f"{fallback_grid_id}_{projection}_{suffix}"

    def _register_grid(
        self,
        grid_id: str,
        collection: str,
        raster_metadata: dict[str, Any] | None,
        now: str,
    ) -> None:
        is_hls = collection in HLS_PRODUCT_PROFILES
        raster_metadata = raster_metadata or {}
        transform = raster_metadata.get("transform")
        self._upsert(
            "grids",
            {
                "grid_id": grid_id,
                "crs_wkt": raster_metadata.get("crs_wkt") or ("MGRS/UTM per tile" if is_hls else "source-native"),
                "epsg": raster_metadata.get("epsg"),
                "axis_order": "x,y",
                "x_origin": raster_metadata.get("x_origin"),
                "y_origin": raster_metadata.get("y_origin"),
                "pixel_size_x": raster_metadata.get("pixel_size_x") or (30.0 if is_hls else None),
                "pixel_size_y": raster_metadata.get("pixel_size_y") or (-30.0 if is_hls else None),
                "tile_width": raster_metadata.get("width") or (3660 if is_hls else None),
                "tile_height": raster_metadata.get("height") or (3660 if is_hls else None),
                "tile_scheme": "MGRS" if is_hls else "source-native",
                "spatial_support": "pixel_area" if is_hls else None,
                "transform_json": json.dumps(transform) if transform else None,
                "width": raster_metadata.get("width"),
                "height": raster_metadata.get("height"),
                "created_at": now,
                "updated_at": now,
            },
            preserve_created=True,
        )

    def _register_variable(
        self,
        product_id: str,
        collection: str,
        asset_key: str,
        modality: str,
        raster_metadata: dict[str, Any] | None,
        now: str,
    ) -> None:
        profile = HLS_PRODUCT_PROFILES.get(collection, {})
        known_hls = profile.get("bands", {}).get(asset_key)
        override = HLS_VARIABLE_OVERRIDES.get(collection, {}).get(asset_key, {})
        canonical, long_name, wavelength, bandwidth = known_hls or (
            f"{product_id}_{asset_key.lower()}", asset_key, None, None
        )
        is_quality = asset_key.lower() == "fmask"
        raster_metadata = raster_metadata or {}
        source_tags = raster_metadata.get("tags", {})
        source_scale = source_tags.get("scale_factor")
        source_offset = source_tags.get("add_offset")
        self._upsert(
            "variables",
            {
                "variable_id": f"{product_id}:{asset_key}",
                "canonical_name": canonical,
                "source_name": asset_key,
                "long_name": long_name,
                "standard_name": None,
                "unit": None if is_quality else override.get("unit", "1" if known_hls else None),
                "dtype": raster_metadata.get("dtype") or (("uint8" if is_quality else "int16") if known_hls else None),
                "scale_factor": optional_float(source_scale) if source_scale is not None else override.get("scale_factor", 0.0001 if known_hls and not is_quality else None),
                "add_offset": optional_float(source_offset) if source_offset is not None else (0.0 if known_hls and not is_quality else None),
                "central_wavelength_nm": wavelength,
                "bandwidth_nm": bandwidth,
                "valid_min": None,
                "valid_max": None,
                "fill_value": None,
                "nodata": raster_metadata.get("nodata"),
                "flag_values_json": None,
                "flag_meanings": None,
                "quality_flag_definition": source_tags.get("Fmask bit description") if is_quality else None,
                "profile_version": profile.get("profile_version"),
                "modality": modality,
                "role": "quality" if is_quality else override.get("role", "data"),
                "temporal_type": "instantaneous" if known_hls else ("static" if modality == "static" else None),
                "temporal_support": None,
                "spatial_support": "pixel" if known_hls else None,
                "quality_variable": "quality_fmask" if known_hls and not is_quality else None,
                "source_product": product_id,
                "processing_level": "source",
                "created_at": now,
                "updated_at": now,
            },
            preserve_created=True,
        )

    @staticmethod
    def _product_semantics(collection: str) -> tuple[str, str, str]:
        lower = collection.lower()
        if collection in HLS_PRODUCT_PROFILES:
            return "observation", "2.0", "hls_mgrs_30m"
        if any(token in lower for token in ("sentinel", "landsat", "modis", "smap", "grace")):
            return "observation", "unknown", f"{safe_component(lower)}_native"
        if any(token in lower for token in ("era5", "nldas", "gpm")):
            return "forcing", "unknown", f"{safe_component(lower)}_native"
        if any(token in lower for token in ("dem", "soil", "geology", "landcover")):
            return "static", "unknown", f"{safe_component(lower)}_native"
        return "observation", "unknown", f"{safe_component(lower)}_native"

    def _update_stac(
        self,
        *,
        catalog: str,
        collection: str,
        item_id: str,
        item: dict[str, Any],
        asset_key: str,
        asset_id: str,
        path: Path,
        checksum: str,
        integrity: dict[str, Any],
        grid_id: str,
        version: str,
        run_id: str,
        collection_details: dict[str, Any],
        asset_roles: list[str] | None = None,
        asset_media_type: str | None = None,
        asset_category: str | None = None,
    ) -> None:
        with self._lock:
            stac_dir = (
                self._active_commit.prepare_tree(self.stac_dir)
                if self._active_commit
                else self.stac_dir
            )
            stac_catalog = pystac.Catalog.from_file(str(stac_dir / "catalog.json"))
            collection_id = collection_identity(catalog, collection).lower()
            stac_collection = next(
                (value for value in stac_catalog.get_collections() if value.id == collection_id), None
            )
            if stac_collection is None:
                stac_collection = pystac.Collection(
                    id=collection_id,
                    description=collection_details.get("description") or f"Source assets for {collection}",
                    extent=pystac.Extent(
                        pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
                        pystac.TemporalExtent([[None, None]]),
                    ),
                    license=collection_details.get("license") or "various",
                    title=collection_details.get("title"),
                )
                stac_catalog.add_child(stac_collection)
            else:
                stac_collection.description = collection_details.get("description") or stac_collection.description
                stac_collection.title = collection_details.get("title") or stac_collection.title
                stac_collection.license = collection_details.get("license") or stac_collection.license

            stac_item = next(stac_collection.get_items(safe_component(item_id)), None)
            if stac_item is None:
                acquired = item.get("properties", {}).get("datetime")
                stac_item = pystac.Item(
                    id=safe_component(item_id),
                    geometry=item.get("geometry"),
                    bbox=item.get("bbox"),
                    datetime=str_to_datetime(acquired) if acquired else datetime.now(timezone.utc),
                    properties={
                        "product": collection,
                        "version": version,
                        "grid_id": grid_id,
                        "earthzarr:protocol": PROTOCOL_VERSION,
                    },
                )
                stac_collection.add_item(stac_item)
            category_roles = {
                "data": ["data"],
                "auxiliary": ["auxiliary"],
                "visual": ["visual"],
                "thumbnail": ["thumbnail"],
                "metadata": ["metadata"],
            }
            resolved_roles = (
                list(asset_roles)
                if asset_roles is not None
                else category_roles.get(asset_category or "", [])
            )
            stac_item.add_asset(
                safe_component(asset_key, "data"),
                pystac.Asset(
                    href=str(path),
                    media_type=asset_media_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                    roles=resolved_roles,
                    extra_fields={
                        "earthzarr:asset_id": asset_id,
                        "earthzarr:checksum_sha256": checksum,
                        "earthzarr:lineage_id": run_id,
                        "earthzarr:integrity_status": "passed" if integrity.get("ok") else "failed",
                        "earthzarr:integrity_checks": integrity.get("checks") or {},
                        "earthzarr:checksum_verified": integrity.get("checksum_verified"),
                        "earthzarr:resolved_category": asset_category,
                    },
                ),
            )
            stac_catalog.normalize_and_save(str(stac_dir), catalog_type=pystac.CatalogType.SELF_CONTAINED)

    def registered_asset(self, asset_id: str) -> dict[str, Any] | None:
        """Return one canonical Registry Asset for recovery checks."""

        return self._find("assets", asset_id)

    def _find(self, table: str, key_value: str) -> dict[str, Any] | None:
        key = REGISTRY_KEYS[table]
        for row in self._read_rows(table):
            if row[key] == key_value:
                return row
        return None

    def _upsert(self, table: str, row: dict[str, Any], preserve_created: bool = False) -> None:
        with self._lock:
            key = REGISTRY_KEYS[table]
            rows = self._read_rows(table)
            existing = next((value for value in rows if value[key] == row[key]), None)
            if existing and preserve_created and "created_at" in row:
                row["created_at"] = existing.get("created_at") or row["created_at"]
            rows = [value for value in rows if value[key] != row[key]]
            rows.append(row)
            self._write_table(self.registry_dir / f"{table}.parquet", rows, REGISTRY_SCHEMAS[table])

    def _read_rows(self, table: str) -> list[dict[str, Any]]:
        path = self.registry_dir / f"{table}.parquet"
        if self._active_commit:
            staged = self._active_commit.staged_path(path)
            if staged.exists():
                path = staged
        return pq.read_table(path).to_pylist() if path.exists() else []

    def _ensure_registry_schema(
        self,
        table: str,
        schema: pa.Schema,
        *,
        create_if_missing: bool = True,
    ) -> None:
        path = self.registry_dir / f"{table}.parquet"
        if not path.exists():
            if not create_if_missing:
                return
            self._write_table(path, [], schema)
            return
        current = pq.read_table(path)
        if current.schema.equals(schema, check_metadata=False):
            return
        self._write_table(path, current.to_pylist(), schema)

    def _migrate_asset_registry_identity(self) -> None:
        """Backfill canonical asset identity fields in older registries.

        Existing rows are retained. A duplicate canonical identity is treated
        as an ambiguity and stops initialization rather than merging records.
        """

        rows = self._read_rows("assets")
        if not rows:
            return
        mapping: dict[str, str] = {}
        seen: dict[str, dict[str, Any]] = {}
        changed = False
        for row in rows:
            local_path = str(row.get("local_path") or "")
            local_path_obj = Path(local_path)
            if local_path_obj.is_absolute():
                try:
                    local_path = local_path_obj.resolve().relative_to(self.root).as_posix()
                except ValueError:
                    local_path = ""
            parts = Path(local_path).parts
            catalog = str(row.get("catalog") or "")
            collection = str(row.get("collection_id") or "")
            if parts and parts[0] == "source" and len(parts) >= 3:
                catalog = catalog or parts[1]
                collection = collection or parts[2]
            if not catalog or not collection or not row.get("source_item_id") or not row.get("asset_key"):
                raise ValueError(
                    "registry contains legacy asset with ambiguous canonical identity; "
                    f"asset_id={row.get('asset_id')!r}, local_path={row.get('local_path')!r}"
                )
            identity = AssetIdentity(
                catalog,
                collection,
                str(row["source_item_id"]),
                str(row["asset_key"]),
            )
            canonical_id = identity.digest()
            previous = seen.get(canonical_id)
            if previous:
                raise ValueError(
                    f"registry contains duplicate canonical asset identity {canonical_id}"
                )
            seen[canonical_id] = row
            old_id = str(row.get("asset_id") or "")
            if old_id and old_id != canonical_id:
                mapping[old_id] = canonical_id
                changed = True
            if row.get("catalog") != catalog or row.get("collection_id") != collection:
                changed = True
            row["asset_id"] = canonical_id
            row["catalog"] = catalog
            row["collection_id"] = collection
        if changed:
            with ProtocolCommit(
                self.root,
                kind="registry_identity_migration",
                metadata={"asset_count": len(rows)},
            ) as commit:
                previous_commit = self._active_commit
                self._active_commit = commit
                try:
                    self._write_table(self.registry_dir / "assets.parquet", rows, REGISTRY_SCHEMAS["assets"])
                    if mapping:
                        processing_rows = self._read_rows("processing_runs")
                        processing_changed = False
                        for row in processing_rows:
                            try:
                                asset_ids = json.loads(row.get("output_asset_ids") or "[]")
                            except json.JSONDecodeError:
                                continue
                            updated = [mapping.get(str(asset_id), str(asset_id)) for asset_id in asset_ids]
                            if updated != asset_ids:
                                row["output_asset_ids"] = json.dumps(updated)
                                row["checksum"] = hashlib.sha256("\n".join(updated).encode()).hexdigest()
                                processing_changed = True
                        if processing_changed:
                            self._write_table(
                                self.registry_dir / "processing_runs.parquet",
                                processing_rows,
                                REGISTRY_SCHEMAS["processing_runs"],
                            )
                        self._migrate_stac_asset_ids(mapping)
                finally:
                    self._active_commit = previous_commit

    def _migrate_stac_asset_ids(self, mapping: dict[str, str]) -> None:
        stac_dir = (
            self._active_commit.prepare_tree(self.stac_dir)
            if self._active_commit
            else self.stac_dir
        )
        catalog_path = stac_dir / "catalog.json"
        if not catalog_path.exists():
            return
        catalog = pystac.Catalog.from_file(str(catalog_path))
        changed = False
        for item in catalog.get_all_items():
            for asset in item.assets.values():
                old_id = asset.extra_fields.get("earthzarr:asset_id")
                if old_id in mapping:
                    asset.extra_fields["earthzarr:asset_id"] = mapping[old_id]
                    changed = True
        if changed:
            catalog.normalize_and_save(str(stac_dir), catalog_type=pystac.CatalogType.SELF_CONTAINED)

    @staticmethod
    def _read_item_metadata(path: Path) -> dict[str, Any] | None:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _write_table(self, path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
        if self._active_commit:
            path = self._active_commit.staged_path(path)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary)
        os.replace(temporary, path)

    @staticmethod
    def _write_json_if_missing(path: Path, payload: Any) -> None:
        if path.exists():
            return
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, path)

    @staticmethod
    def _code_commit() -> str:
        try:
            return subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=Path(__file__).parent,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return "unknown"
