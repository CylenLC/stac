"""Materialize local hydrology datasets into Earth Lake entities and arrays."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from protocol_commit import ProtocolCommit


DEFAULT_DATA_ROOT = Path("/Volumes/Untitled/data")
DEFAULT_LAKE_ROOT = Path(os.environ.get("EARTH_LAKE_ROOT", "/Volumes/Untitled/stac"))
DEFAULT_EXISTING_ZARR_ROOT = Path("/Volumes/Untitled/zarr-v3")
DEFAULT_HYDRODATASET_PROJECT = Path("/Users/cylenlc/work/hydrodataset")

DATASET_ALIASES = {
    "attributes": "camels_us",
    "bull": "bull",
    "camels_aus": "camels_aus",
    "camels_br": "camels_br",
    "camels_ch": "camels_ch",
    "camels_cl": "camels_cl",
    "camels_col": "camels_col",
    "camels_de": "camels_de",
    "camels_dk": "camels_dk",
    "camels_fi": "camels_fi",
    "camels_fr": "camels_fr",
    "camels_gb": "camels_gb",
    "camels_ind": "camels_ind",
    "camels_lux": "camels_lux",
    "camels_nz": "camels_nz",
    "camels_se": "camels_se",
    "camels_sk": "camels_sk",
    "camels_us": "camels_us",
    "camelsh": "camelsh",
    "caravan_dk": "caravan_dk",
    "estream": "estreams",
    "frshp": "camels_fr",
    "grdccaravan": "grdc_caravan",
    "hysets": "hysets",
    "lamahce": "lamah_ce",
    "lamahice": "lamah_ice",
    "sanxia": "sanxia",
}

ATTRIBUTE_WORDS = {
    "attribute",
    "attributes",
    "climate",
    "climatic",
    "geology",
    "geologic",
    "hydrologic",
    "hydrogeology",
    "landcover",
    "landuse",
    "metadata",
    "physiographic",
    "signature",
    "soil",
    "topographic",
    "topography",
}

SOURCE_SCAN_PRUNE_PATTERNS = (
    "*timeseries*",
    "*time_series*",
    "*streamflow*",
    "*forcing*",
    "*hydrometeorological*",
    "hourly",
    "daily",
    "model_output*",
)


@dataclass
class MaterializationRecord:
    source: str
    source_fingerprint: str
    output: str
    dataset_id: str
    kind: str
    status: str
    logical_asset_id: str
    row_count: int | None = None
    byte_size: int | None = None
    content_sha256: str | None = None
    file_count: int | None = None
    directory_count: int | None = None
    crs: str | None = None
    bbox: list[float] | None = None
    columns: list[str] | None = None
    updated_at: str | None = None
    error: str | None = None


class ManifestError(ValueError):
    """Raised when the durable materialization manifest cannot be trusted."""


def _utc_now() -> str:
    return datetime.now().astimezone().isoformat()


def _safe_component(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", value.strip().lower()).strip("-.")
    return cleaned or "unknown"


def _dataset_id(path: Path, data_root: Path) -> str:
    relative = path.relative_to(data_root)
    first = relative.parts[0].casefold()
    if len(relative.parts) == 1 and first.endswith("_d.nc"):
        first = first.removesuffix("_d.nc")
    if first in DATASET_ALIASES:
        return DATASET_ALIASES[first]
    component = _safe_component(first).replace("-", "_")
    if component != first:
        component = f"{component}__{hashlib.sha1(first.encode()).hexdigest()[:10]}"
    return component


def _logical_asset_id(source: Path, data_root: Path, existing_zarr_root: Path, kind: str) -> str:
    for root in (data_root, existing_zarr_root):
        try:
            relative = source.resolve().relative_to(root.resolve()).as_posix()
            break
        except ValueError:
            continue
    else:
        raise ValueError("materialization source must be inside a configured source root")
    payload = f"hydro-materialization|{kind}|{relative}"
    return f"hydro:{hashlib.sha256(payload.encode()).hexdigest()}"


def _validate_relative_output(output: Any, lake_root: Path) -> str:
    if (
        not isinstance(output, str)
        or not output
        or Path(output).is_absolute()
        or any(part in {".", ".."} for part in Path(output).parts)
    ):
        raise ManifestError("manifest output must be a non-empty relative path")
    candidate = (lake_root / output).resolve()
    try:
        candidate.relative_to(lake_root.resolve())
    except ValueError as exc:
        raise ManifestError("manifest output escapes the lake root") from exc
    return output


def _validate_manifest_records(
    records: Iterable[MaterializationRecord],
    lake_root: Path,
) -> dict[str, MaterializationRecord]:
    validated: dict[str, MaterializationRecord] = {}
    claimed_outputs: dict[str, str] = {}
    for record in records:
        if not isinstance(record, MaterializationRecord):
            raise ManifestError("manifest record is not a materialization record")
        if not isinstance(record.logical_asset_id, str) or not record.logical_asset_id:
            raise ManifestError("manifest record is missing logical_asset_id")
        if not isinstance(record.source, str) or not record.source:
            raise ManifestError(f"manifest record {record.logical_asset_id} is missing source")
        if not isinstance(record.dataset_id, str) or not record.dataset_id:
            raise ManifestError(f"manifest record {record.logical_asset_id} is missing dataset_id")
        if not isinstance(record.kind, str) or not record.kind:
            raise ManifestError(f"manifest record {record.logical_asset_id} is missing kind")
        if record.status != "materialized":
            raise ManifestError(
                f"manifest record {record.logical_asset_id} has invalid status {record.status!r}"
            )
        if not isinstance(record.source_fingerprint, str) or not record.source_fingerprint:
            raise ManifestError(f"manifest record {record.logical_asset_id} has no source fingerprint")
        if not isinstance(record.content_sha256, str) or not record.content_sha256:
            raise ManifestError(f"manifest record {record.logical_asset_id} has no content checksum")
        for field in ("byte_size", "file_count", "directory_count"):
            value = getattr(record, field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ManifestError(f"manifest record {record.logical_asset_id} has invalid {field}")
        if record.row_count is not None and (
            not isinstance(record.row_count, int)
            or isinstance(record.row_count, bool)
            or record.row_count < 0
        ):
            raise ManifestError(f"manifest record {record.logical_asset_id} has invalid row_count")
        if record.columns is not None and (
            not isinstance(record.columns, list)
            or not all(isinstance(column, str) for column in record.columns)
        ):
            raise ManifestError(f"manifest record {record.logical_asset_id} has invalid columns")
        if record.bbox is not None and (
            not isinstance(record.bbox, list)
            or not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in record.bbox)
        ):
            raise ManifestError(f"manifest record {record.logical_asset_id} has invalid bbox")
        if record.updated_at is not None and not isinstance(record.updated_at, str):
            raise ManifestError(f"manifest record {record.logical_asset_id} has invalid updated_at")
        if record.error is not None and not isinstance(record.error, str):
            raise ManifestError(f"manifest record {record.logical_asset_id} has invalid error")
        output = _validate_relative_output(record.output, lake_root)
        existing = validated.get(record.logical_asset_id)
        if existing is not None:
            raise ManifestError(f"duplicate logical_asset_id: {record.logical_asset_id}")
        owner = claimed_outputs.get(output)
        if owner is not None and owner != record.logical_asset_id:
            raise ManifestError(f"physical output is claimed by multiple logical IDs: {output}")
        validated[record.logical_asset_id] = record
        claimed_outputs[output] = record.logical_asset_id
    return validated


def load_materialization_manifest(
    manifest_path: str | Path,
    lake_root: str | Path,
) -> dict[str, MaterializationRecord]:
    """Load a materialization manifest without turning corruption into empty state."""

    path = Path(manifest_path)
    root = Path(lake_root).resolve()
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read materialization manifest: {exc}") from exc
    if not isinstance(payload, dict):
        raise ManifestError("materialization manifest must be a JSON object")
    if payload.get("protocol") != "EarthZarrProtocol":
        raise ManifestError("materialization manifest has an incompatible protocol")
    if payload.get("protocol_version") != "0.1.0":
        raise ManifestError("materialization manifest has an incompatible protocol version")
    if payload.get("kind") != "hydrodataset_materialization":
        raise ManifestError("materialization manifest has an incompatible kind")
    records_payload = payload.get("records")
    if not isinstance(records_payload, list):
        raise ManifestError("materialization manifest records must be a list")
    records: list[MaterializationRecord] = []
    for index, item in enumerate(records_payload):
        if not isinstance(item, dict):
            raise ManifestError(f"manifest record {index} must be an object")
        try:
            record = MaterializationRecord(**item)
        except (TypeError, ValueError) as exc:
            raise ManifestError(f"malformed manifest record {index}: {exc}") from exc
        records.append(record)
    return _validate_manifest_records(records, root)


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return sys.maxsize


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_stats(path: Path) -> tuple[int, int, int]:
    """Return byte, file, and directory counts without reading file contents."""

    if not path.is_dir():
        raise ValueError(f"expected a directory tree: {path}")
    byte_size = 0
    file_count = 0
    directory_count = 0
    for current_root, directory_names, file_names in os.walk(path):
        directory_names[:] = [
            name
            for name in directory_names
            if not name.startswith("._") and name != ".DS_Store"
        ]
        directory_count += len(directory_names)
        for name in file_names:
            if name.startswith("._") or name == ".DS_Store":
                continue
            byte_size += (Path(current_root) / name).stat().st_size
            file_count += 1
    return byte_size, file_count, directory_count


def tree_integrity(path: Path) -> tuple[str, int, int, int]:
    """Return a deterministic content digest and stats for a directory tree.

    Zarr is a directory representation, so ``IntegrityGate`` cannot validate it
    as a single file.  This is the directory equivalent used by both
    materialization reuse and the offline health auditor.  Metadata, chunk
    paths, sizes, and bytes all participate in the digest; a path that merely
    exists is never considered a valid materialization.
    """

    if not path.is_dir():
        raise ValueError(f"expected a directory tree: {path}")
    digest = hashlib.sha256()
    byte_size = 0
    file_count = 0
    directory_count = 0
    for current_root, directory_names, file_names in os.walk(path):
        directory_names[:] = sorted(
            name
            for name in directory_names
            if not name.startswith("._") and name != ".DS_Store"
        )
        directory_count += len(directory_names)
        for name in sorted(file_names):
            if name.startswith("._") or name == ".DS_Store":
                continue
            source = Path(current_root) / name
            relative = source.relative_to(path).as_posix()
            stat = source.stat()
            checksum = file_sha256(source)
            digest.update(relative.encode("utf-8", errors="surrogateescape"))
            digest.update(b"\0")
            digest.update(str(stat.st_size).encode())
            digest.update(b"\0")
            digest.update(checksum.encode())
            digest.update(b"\n")
            byte_size += stat.st_size
            file_count += 1
    return digest.hexdigest(), byte_size, file_count, directory_count


def _fingerprint(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted({item.resolve() for item in paths}, key=lambda item: (item.name, item.suffix)):
        try:
            stat = path.stat()
        except OSError:
            continue
        # The fingerprint describes content, not the machine-specific mount
        # point.  Logical ownership is keyed separately by ``logical_asset_id``.
        digest.update(path.name.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        if path.is_dir():
            digest.update(tree_integrity(path)[0].encode())
            continue
        digest.update(str(stat.st_size).encode())
        digest.update(file_sha256(path).encode())
    return digest.hexdigest()


@contextmanager
def _source_lock(lake_root: Path, source: Path, *, logical_asset_id: str | None = None):
    """Serialize duplicate materialization of one logical source.

    The SQLite run scheduler is single-process, but the CLI and API can still
    be invoked concurrently.  A per-source advisory lock prevents two callers
    from writing the same temporary output or publishing competing manifest
    records at the same time.  The lock file is hidden and remains as a small,
    harmless coordination artifact.
    """

    lock_root = lake_root / "cache" / "materialization-staging" / "locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    lock_identity = logical_asset_id or str(source.resolve())
    token = hashlib.sha256(lock_identity.encode()).hexdigest()
    lock_path = lock_root / f"{token}.lock"
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _shapefile_parts(path: Path) -> list[Path]:
    return [
        candidate
        for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qpj")
        if (candidate := path.with_suffix(suffix)).is_file() and not candidate.name.startswith("._")
    ]


def _output_token(path: Path, data_root: Path) -> str:
    relative = path.relative_to(data_root).as_posix()
    return f"{_safe_component(path.stem)}-{hashlib.sha1(relative.encode()).hexdigest()[:10]}"


def _is_attribute_file(path: Path) -> bool:
    if path.name.startswith(("._", "~$")) or path.suffix.casefold() not in {".csv", ".txt", ".xlsx", ".xls"}:
        return False
    normalized_path = path.as_posix().casefold().replace("-", "_").replace(" ", "_")
    if any(
        token in normalized_path
        for token in (
            "timeseries",
            "time_series",
            "streamflow",
            "forcing",
            "hydrometeorological",
            "hourly",
            "daily",
        )
    ):
        return False
    if path.stem.casefold() in {"readme", "dataset_sources", "variable_description", "variables_description"}:
        return False
    tokens = {
        token
        for part in path.parts
        for token in re.split(r"[^a-z0-9]+", part.casefold())
        if token
    }
    return bool(tokens.intersection(ATTRIBUTE_WORDS)) or any(
        part.casefold() in {"attributes", "attributes_csv", "catchment properties"}
        for part in path.parts
    )


def _entity_type(path: Path) -> str:
    text = path.as_posix().casefold()
    if any(word in text for word in ("station", "gauge", "gauging", "outlet", "basinoutlet")):
        return "stations"
    if any(word in text for word in ("river", "stream_network", "network")):
        return "rivers"
    if any(word in text for word in ("grid", "patch")):
        return "patches"
    return "basins"


def _fiona_arrow_type(value: str) -> pa.DataType:
    normalized = value.casefold().split(":", 1)[0]
    if normalized.startswith("int"):
        return pa.int64()
    if normalized.startswith("float") or normalized.startswith("double") or normalized.startswith("real"):
        return pa.float64()
    if normalized.startswith("bool"):
        return pa.bool_()
    if normalized.startswith("bytes") or normalized.startswith("binary"):
        return pa.binary()
    return pa.string()


def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_string(arrow_type):
        if isinstance(value, (date, datetime, time)):
            return value.isoformat()
        if isinstance(value, (list, tuple, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value)
    if pa.types.is_binary(arrow_type):
        return bytes(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_boolean(arrow_type):
        return bool(value)
    return value


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class HydroMaterializer:
    """Discover and materialize local hydrology datasets without changing sources."""

    def __init__(
        self,
        data_root: str | Path = DEFAULT_DATA_ROOT,
        lake_root: str | Path = DEFAULT_LAKE_ROOT,
        *,
        existing_zarr_root: str | Path = DEFAULT_EXISTING_ZARR_ROOT,
        hydrodataset_project: str | Path = DEFAULT_HYDRODATASET_PROJECT,
        on_record=None,
        check_control=None,
    ):
        self.data_root = Path(data_root).expanduser().resolve()
        self.lake_root = Path(lake_root).expanduser().resolve()
        self.existing_zarr_root = Path(existing_zarr_root).expanduser().resolve()
        self.hydrodataset_project = Path(hydrodataset_project).expanduser().resolve()
        self.on_record = on_record
        self.check_control = check_control
        self.manifest_path = self.lake_root / "manifests" / "materializations" / "hydrodatasets.json"
        self.records = self._load_manifest()
        self._discovery_cache: dict[frozenset[str] | None, tuple[list[Path], list[Path], list[Path]]] = {}

    def inventory(self, *, include_paths: bool = False) -> dict[str, Any]:
        shapefiles, attributes, netcdf = self._discover_sources()
        zarr_stores = sorted(path for path in self.existing_zarr_root.glob("*.zarr") if path.is_dir()) if self.existing_zarr_root.exists() else []
        dataset_ids = sorted({_dataset_id(path, self.data_root) for path in [*shapefiles, *attributes, *netcdf]})
        result = {
            "data_root": str(self.data_root),
            "lake_root": str(self.lake_root),
            "datasets": dataset_ids,
            "shapefile_count": len(shapefiles),
            "attribute_table_count": len(attributes),
            "standard_netcdf_count": len(netcdf),
            "existing_zarr_count": len(zarr_stores),
        }
        if include_paths:
            result.update(
                shapefiles=[str(path) for path in shapefiles],
                attribute_tables=[str(path) for path in attributes],
                standard_netcdf=[str(path) for path in netcdf],
                existing_zarr=[str(path) for path in zarr_stores],
            )
        return result

    def _discover_sources(self, datasets: set[str] | None = None) -> tuple[list[Path], list[Path], list[Path]]:
        cache_key = frozenset(datasets) if datasets else None
        if cache_key in self._discovery_cache:
            return self._discovery_cache[cache_key]
        shapefiles: list[Path] = []
        attributes: list[Path] = []
        netcdf: list[Path] = []
        for path in self._candidate_source_files(datasets):
            name = path.name
            if name.startswith(("._", "~$")) or name == ".DS_Store":
                continue
            suffix = path.suffix.casefold()
            if suffix == ".shp":
                shapefiles.append(path)
            elif name.endswith("_D.nc") and len(path.relative_to(self.data_root).parts) in {1, 2}:
                netcdf.append(path)
            elif suffix in {".csv", ".txt", ".xlsx", ".xls"} and _is_attribute_file(path):
                attributes.append(path)
        discovered = (sorted(shapefiles), sorted(attributes), sorted(netcdf))
        self._discovery_cache[cache_key] = discovered
        return discovered

    def _candidate_source_files(self, datasets: set[str] | None = None) -> Iterable[Path]:
        roots = [self.data_root]
        if datasets:
            candidates = {
                candidate
                for source_name, dataset_id in DATASET_ALIASES.items()
                if dataset_id in datasets
                for candidate in (
                    self.data_root / source_name,
                    self.data_root / f"{source_name}_D.nc",
                )
                if candidate.exists()
            }
            roots = sorted(candidates)
            if not roots:
                return ()
        find = shutil.which("find")
        if find:
            command = [find, *(str(root) for root in roots), "(", "-type", "d", "("]
            for index, pattern in enumerate(SOURCE_SCAN_PRUNE_PATTERNS):
                if index:
                    command.append("-o")
                command.extend(("-iname", pattern))
            command.extend((")", "-prune", ")", "-o", "(", "-type", "f", "("))
            patterns = ("*.shp", "*.csv", "*.txt", "*.xlsx", "*.xls", "*_D.nc")
            for index, pattern in enumerate(patterns):
                if index:
                    command.append("-o")
                command.extend(("-iname", pattern))
            command.extend((")", "-print0", ")"))
            completed = subprocess.run(command, check=True, stdout=subprocess.PIPE)
            return (
                Path(value.decode("utf-8", errors="surrogateescape"))
                for value in completed.stdout.split(b"\0")
                if value
            )
        return (
            Path(current_root) / name
            for current_root, _, file_names in os.walk(self.data_root)
            for name in file_names
            if Path(name).suffix.casefold() in {".shp", ".csv", ".txt", ".xlsx", ".xls", ".nc"}
        )

    def materialize(
        self,
        *,
        kinds: set[str],
        datasets: set[str] | None = None,
        limit: int | None = None,
        convert_netcdf: bool = False,
        publish_manifest: bool = True,
    ) -> dict[str, Any]:
        self._ensure_output_roots()
        result = {"materialized": 0, "skipped": 0, "failed": 0, "records": []}
        try:
            if "entities" in kinds:
                self._materialize_entities(result, datasets=datasets, limit=limit)
            if "arrays" in kinds:
                self._materialize_arrays(
                    result,
                    datasets=datasets,
                    limit=limit,
                    convert_netcdf=convert_netcdf,
                )
        finally:
            if publish_manifest:
                self._save_manifest()
        return result

    def publish_manifest(self) -> None:
        self._save_manifest()

    def _ensure_output_roots(self) -> None:
        for path in (
            self.lake_root / "entities" / "basins",
            self.lake_root / "entities" / "stations",
            self.lake_root / "entities" / "rivers",
            self.lake_root / "entities" / "patches",
            self.lake_root / "arrays" / "hydrology" / "basin_timeseries",
            self.lake_root / "arrays" / "static" / "basin_attributes",
            self.lake_root / "cache" / "materialization-staging",
        ):
            path.mkdir(parents=True, exist_ok=True)

    def _materialize_entities(self, result: dict[str, Any], *, datasets: set[str] | None, limit: int | None) -> None:
        shapefiles, attributes, _ = self._discover_sources(datasets)
        geometries = [("geometry", path) for path in shapefiles]
        tables = [("attributes", path) for path in attributes]
        selected = [
            item
            for item in [*geometries, *tables]
            if not datasets or _dataset_id(item[1], self.data_root) in datasets
        ]
        if limit is not None:
            geometries = sorted((item for item in selected if item[0] == "geometry"), key=lambda item: _file_size(item[1]))
            tables = sorted((item for item in selected if item[0] == "attributes"), key=lambda item: _file_size(item[1]))
            selected = []
            while len(selected) < limit and (geometries or tables):
                if geometries:
                    selected.append(geometries.pop(0))
                if tables and len(selected) < limit:
                    selected.append(tables.pop(0))
        for kind, source in selected:
            if self.check_control:
                self.check_control()
            try:
                record, changed = self._materialize_geometry(source) if kind == "geometry" else self._materialize_attributes(source)
                self._track(result, record, changed=changed)
            except Exception as exc:
                self._track_failure(result, source, kind, exc)

    def _materialize_geometry(self, source: Path) -> tuple[MaterializationRecord, bool]:
        logical_asset_id = _logical_asset_id(
            source,
            self.data_root,
            self.existing_zarr_root,
            f"entity_{_entity_type(source)}",
        )
        with _source_lock(self.lake_root, source, logical_asset_id=logical_asset_id):
            return self._materialize_geometry_locked(source)

    def _materialize_geometry_locked(self, source: Path) -> tuple[MaterializationRecord, bool]:
        try:
            import fiona
            from pyproj import CRS
            from shapely.geometry import shape
        except ImportError as exc:
            raise RuntimeError("Geometry materialization requires fiona, pyproj, and shapely") from exc

        dataset_id = _dataset_id(source, self.data_root)
        entity_type = _entity_type(source)
        kind = f"entity_{entity_type}"
        logical_asset_id = _logical_asset_id(source, self.data_root, self.existing_zarr_root, kind)
        output = self.lake_root / "entities" / entity_type / dataset_id / f"{_output_token(source, self.data_root)}.geoparquet"
        fingerprint = _fingerprint(_shapefile_parts(source))
        if self._is_current(logical_asset_id, output, fingerprint):
            return self._current_record(logical_asset_id, source), False
        if output.exists() and not output.is_file():
            raise FileExistsError(f"materialization target is not a file: {output}")

        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        temporary.unlink(missing_ok=True)
        row_count = 0
        try:
            with fiona.open(source) as collection:
                property_fields = [(name, _fiona_arrow_type(kind)) for name, kind in collection.schema["properties"].items()]
                schema = pa.schema(
                    [
                        pa.field("_earthzarr_dataset_id", pa.string()),
                        pa.field("_earthzarr_source_feature_id", pa.string()),
                        *(pa.field(name, kind) for name, kind in property_fields),
                        pa.field("geometry", pa.binary()),
                    ]
                )
                crs_wkt = collection.crs.to_wkt() if collection.crs else None
                crs_json = CRS.from_wkt(crs_wkt).to_json_dict() if crs_wkt else None
                geometry_type = collection.schema.get("geometry")
                bbox = [float(value) for value in collection.bounds]
                geo_metadata = {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": [geometry_type] if geometry_type and geometry_type != "Unknown" else [],
                            "crs": crs_json,
                            "bbox": bbox,
                        }
                    },
                }
                metadata = dict(schema.metadata or {})
                metadata[b"geo"] = json.dumps(geo_metadata, separators=(",", ":")).encode()
                metadata[b"earthzarr"] = json.dumps(
                    {"dataset_id": dataset_id, "source": str(source), "entity_type": entity_type},
                    separators=(",", ":"),
                ).encode()
                schema = schema.with_metadata(metadata)
                writer = pq.ParquetWriter(temporary, schema, compression="zstd")
                try:
                    batch: list[dict[str, Any]] = []
                    for feature in collection:
                        properties = dict(feature["properties"])
                        row = {
                            "_earthzarr_dataset_id": dataset_id,
                            "_earthzarr_source_feature_id": str(feature.id),
                            **{
                                name: _coerce_value(properties.get(name), arrow_type)
                                for name, arrow_type in property_fields
                            },
                            "geometry": shape(feature["geometry"]).wkb if feature["geometry"] else None,
                        }
                        batch.append(row)
                        if len(batch) >= 10_000:
                            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                            row_count += len(batch)
                            batch.clear()
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        row_count += len(batch)
                finally:
                    writer.close()
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
        record = MaterializationRecord(
            source=str(source),
            source_fingerprint=fingerprint,
            output=output.relative_to(self.lake_root).as_posix(),
            dataset_id=dataset_id,
            kind=kind,
            status="materialized",
            logical_asset_id=logical_asset_id,
            row_count=row_count,
            byte_size=output.stat().st_size,
            content_sha256=file_sha256(output),
            file_count=1,
            directory_count=0,
            crs=crs_wkt,
            bbox=bbox,
            columns=[field.name for field in schema],
            updated_at=_utc_now(),
        )
        self._store_record(record)
        return record, True

    def _materialize_attributes(self, source: Path) -> tuple[MaterializationRecord, bool]:
        logical_asset_id = _logical_asset_id(
            source, self.data_root, self.existing_zarr_root, "entity_basin_attributes"
        )
        with _source_lock(self.lake_root, source, logical_asset_id=logical_asset_id):
            return self._materialize_attributes_locked(source)

    def _materialize_attributes_locked(self, source: Path) -> tuple[MaterializationRecord, bool]:
        dataset_id = _dataset_id(source, self.data_root)
        kind = "entity_basin_attributes"
        logical_asset_id = _logical_asset_id(source, self.data_root, self.existing_zarr_root, kind)
        output = self.lake_root / "entities" / "basins" / dataset_id / "attributes" / f"{_output_token(source, self.data_root)}.parquet"
        fingerprint = _fingerprint([source])
        if self._is_current(logical_asset_id, output, fingerprint):
            return self._current_record(logical_asset_id, source), False
        if output.exists() and not output.is_file():
            raise FileExistsError(f"materialization target is not a file: {output}")
        if source.stat().st_size > 512 * 1024 * 1024:
            raise ValueError("attribute source exceeds the 512 MiB safety limit")

        table = self._read_attribute_table(source)
        if "_earthzarr_dataset_id" not in table.column_names:
            table = table.append_column(
                "_earthzarr_dataset_id",
                pa.array([dataset_id] * table.num_rows, type=pa.string()),
            )
        metadata = dict(table.schema.metadata or {})
        metadata[b"earthzarr"] = json.dumps(
            {"dataset_id": dataset_id, "source": str(source), "entity_type": "basin_attributes"},
            separators=(",", ":"),
        ).encode()
        table = table.replace_schema_metadata(metadata)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        try:
            pq.write_table(table, temporary, compression="zstd")
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
        record = MaterializationRecord(
            source=str(source),
            source_fingerprint=fingerprint,
            output=output.relative_to(self.lake_root).as_posix(),
            dataset_id=dataset_id,
            kind=kind,
            status="materialized",
            logical_asset_id=logical_asset_id,
            row_count=table.num_rows,
            byte_size=output.stat().st_size,
            content_sha256=file_sha256(output),
            file_count=1,
            directory_count=0,
            columns=table.column_names,
            updated_at=_utc_now(),
        )
        self._store_record(record)
        return record, True

    @staticmethod
    def _read_attribute_table(source: Path) -> pa.Table:
        if source.suffix.casefold() in {".xlsx", ".xls"}:
            try:
                import pandas as pd
            except ImportError as exc:
                raise RuntimeError("Excel attribute materialization requires pandas and openpyxl/xlrd") from exc
            return pa.Table.from_pandas(pd.read_excel(source), preserve_index=False)

        raw = source.read_bytes()[:64 * 1024]
        encoding = "utf-8"
        try:
            sample = raw.decode(encoding)
        except UnicodeDecodeError:
            encoding = "latin1"
            sample = raw.decode(encoding)
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
        except csv.Error:
            delimiter = ","
        header = next(csv.reader(sample.splitlines(), delimiter=delimiter), [])
        identifier_types = {
            name: pa.string()
            for name in header
            if any(token in name.casefold() for token in ("id", "gage", "gauge", "station", "basin", "catchment"))
        }
        try:
            return pa_csv.read_csv(
                source,
                read_options=pa_csv.ReadOptions(encoding=encoding),
                parse_options=pa_csv.ParseOptions(delimiter=delimiter),
                convert_options=pa_csv.ConvertOptions(column_types=identifier_types),
            )
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            try:
                import pandas as pd
            except ImportError as exc:
                raise RuntimeError("Irregular attribute tables require pandas") from exc
            frame = pd.read_csv(source, sep=None, engine="python", encoding=encoding)
            return pa.Table.from_pandas(frame, preserve_index=False)

    def _materialize_arrays(
        self,
        result: dict[str, Any],
        *,
        datasets: set[str] | None,
        limit: int | None,
        convert_netcdf: bool,
    ) -> None:
        existing = sorted(self.existing_zarr_root.glob("*.zarr")) if self.existing_zarr_root.exists() else []
        selected_existing = [path for path in existing if not datasets or self._zarr_dataset_id(path) in datasets]
        if limit is not None:
            selected_existing = selected_existing[:limit]
        for source in selected_existing:
            if self.check_control:
                self.check_control()
            try:
                record, changed = self._clone_existing_zarr(source)
                self._track(result, record, changed=changed)
            except Exception as exc:
                self._track_failure(result, source, "array_zarr", exc)

        if not convert_netcdf:
            return
        _, _, netcdf = self._discover_sources(datasets)
        selected_netcdf = [path for path in netcdf if not datasets or _dataset_id(path, self.data_root) in datasets]
        if limit is not None:
            selected_netcdf = selected_netcdf[:limit]
        for source in selected_netcdf:
            if self.check_control:
                self.check_control()
            try:
                record, changed = self._convert_standard_netcdf(source)
                self._track(result, record, changed=changed)
            except Exception as exc:
                self._track_failure(result, source, "array_basin_timeseries", exc)

    @staticmethod
    def _zarr_dataset_id(path: Path) -> str:
        return path.name.removesuffix(".zarr").removesuffix("_timeseries").removesuffix("_attributes")

    def _timeseries_output(self, dataset_id: str) -> Path:
        return self.lake_root / "arrays" / "hydrology" / "basin_timeseries" / f"dataset={dataset_id}" / "daily.zarr"

    def _attributes_output(self, dataset_id: str) -> Path:
        return self.lake_root / "arrays" / "static" / "basin_attributes" / f"dataset={dataset_id}" / "attributes.zarr"

    def _clone_existing_zarr(self, source: Path) -> tuple[MaterializationRecord, bool]:
        kind = "array_basin_attributes" if source.name.endswith("_attributes.zarr") else "array_basin_timeseries"
        logical_asset_id = _logical_asset_id(
            source, self.data_root, self.existing_zarr_root, kind
        )
        with _source_lock(self.lake_root, source, logical_asset_id=logical_asset_id):
            return self._clone_existing_zarr_locked(source)

    def _clone_existing_zarr_locked(self, source: Path) -> tuple[MaterializationRecord, bool]:
        dataset_id = self._zarr_dataset_id(source)
        is_attributes = source.name.endswith("_attributes.zarr")
        kind = "array_basin_attributes" if is_attributes else "array_basin_timeseries"
        logical_asset_id = _logical_asset_id(source, self.data_root, self.existing_zarr_root, kind)
        output = self._attributes_output(dataset_id) if is_attributes else self._timeseries_output(dataset_id)
        fingerprint, _source_byte_size, source_file_count, source_directory_count = tree_integrity(source)
        if self._is_current(logical_asset_id, output, fingerprint):
            return self._current_record(logical_asset_id, source), False
        if output.exists() and not output.is_dir():
            raise FileExistsError(f"materialization target is not a directory: {output}")
        output_relative = output.relative_to(self.lake_root).as_posix()
        conflicting_records = [
            record
            for record in self.records.values()
            if record.output == output_relative and record.logical_asset_id != logical_asset_id
        ]
        if conflicting_records:
            raise FileExistsError(
                f"materialization target is already owned by another logical source: {output}"
            )
        if output.is_dir():
            existing_integrity = tree_integrity(output)
            if existing_integrity[0] == fingerprint:
                record = MaterializationRecord(
                    source=str(source),
                    source_fingerprint=fingerprint,
                    output=output.relative_to(self.lake_root).as_posix(),
                    dataset_id=dataset_id,
                    kind=kind,
                    status="materialized",
                    logical_asset_id=logical_asset_id,
                    byte_size=existing_integrity[1],
                    content_sha256=existing_integrity[0],
                    file_count=existing_integrity[2],
                    directory_count=existing_integrity[3],
                    updated_at=_utc_now(),
                )
                self._store_record(record)
                return record, True
            if logical_asset_id not in self.records:
                raise FileExistsError(
                    f"refusing to replace an unclaimed non-matching array store: {output}"
                )
        output.parent.mkdir(parents=True, exist_ok=True)
        partials = sorted(
            output.parent.glob(f".{output.name}.*.partial"),
            key=lambda path: path.stat().st_mtime_ns,
        )
        temporary = partials[0] if partials else output.with_name(f".{output.name}.resume.partial")
        byte_size = self._clone_tree(source, temporary)
        output_integrity = tree_integrity(temporary)
        if output_integrity[0] != fingerprint:
            raise ValueError(f"staged array store failed integrity verification: {temporary}")
        quarantine = None
        if output.exists():
            quarantine = output.with_name(f".{output.name}.{os.getpid()}.corrupt.partial")
            shutil.rmtree(quarantine, ignore_errors=True)
            os.replace(output, quarantine)
        try:
            os.replace(temporary, output)
        except Exception:
            if quarantine is not None and not output.exists():
                os.replace(quarantine, output)
            raise
        if quarantine is not None:
            shutil.rmtree(quarantine, ignore_errors=True)
        record = MaterializationRecord(
            source=str(source),
            source_fingerprint=fingerprint,
            output=output.relative_to(self.lake_root).as_posix(),
            dataset_id=dataset_id,
            kind=kind,
            status="materialized",
            logical_asset_id=logical_asset_id,
            byte_size=byte_size,
            content_sha256=output_integrity[0],
            file_count=source_file_count,
            directory_count=source_directory_count,
            updated_at=_utc_now(),
        )
        self._store_record(record)
        return record, True

    def _clone_tree(self, source: Path, destination: Path) -> int:
        destination.mkdir(parents=True, exist_ok=True)
        use_hardlinks = self._supports_hardlinks(source, destination)
        byte_size = 0
        for root, directory_names, file_names in os.walk(source):
            directory_names[:] = [name for name in directory_names if not name.startswith("._")]
            relative = Path(root).relative_to(source)
            target_root = destination / relative
            target_root.mkdir(parents=True, exist_ok=True)
            for name in file_names:
                if self.check_control:
                    self.check_control()
                if name.startswith("._") or name == ".DS_Store":
                    continue
                src = Path(root) / name
                dst = target_root / name
                source_size = src.stat().st_size
                if dst.exists() and dst.is_file() and dst.stat().st_size == source_size:
                    if file_sha256(dst) == file_sha256(src):
                        byte_size += source_size
                        continue
                dst.unlink(missing_ok=True)
                if use_hardlinks:
                    os.link(src, dst)
                else:
                    partial_file = dst.with_name(f".{dst.name}.copying")
                    partial_file.unlink(missing_ok=True)
                    shutil.copy2(src, partial_file)
                    os.replace(partial_file, dst)
                byte_size += source_size
        source_files = {
            path.relative_to(source).as_posix()
            for current_root, directory_names, file_names in os.walk(source)
            for name in file_names
            if not name.startswith("._") and name != ".DS_Store"
            for path in [Path(current_root) / name]
        }
        for current_root, directory_names, file_names in os.walk(destination, topdown=False):
            for name in file_names:
                if name.startswith("._") or name == ".DS_Store":
                    continue
                path = Path(current_root) / name
                if path.relative_to(destination).as_posix() not in source_files:
                    path.unlink(missing_ok=True)
            for name in directory_names:
                directory = Path(current_root) / name
                if not any(directory.iterdir()):
                    directory.rmdir()
        return byte_size

    @staticmethod
    def _supports_hardlinks(source: Path, destination: Path) -> bool:
        probe_source = source / "zarr.json"
        probe_destination = destination / ".earthzarr-hardlink-probe"
        if not probe_source.is_file():
            return False
        probe_destination.unlink(missing_ok=True)
        try:
            os.link(probe_source, probe_destination)
        except OSError:
            return False
        else:
            return True
        finally:
            probe_destination.unlink(missing_ok=True)

    def _convert_standard_netcdf(self, source: Path) -> tuple[MaterializationRecord, bool]:
        logical_asset_id = _logical_asset_id(
            source, self.data_root, self.existing_zarr_root, "array_basin_timeseries"
        )
        with _source_lock(self.lake_root, source, logical_asset_id=logical_asset_id):
            return self._convert_standard_netcdf_locked(source)

    def _convert_standard_netcdf_locked(self, source: Path) -> tuple[MaterializationRecord, bool]:
        if self.hydrodataset_project.is_dir() and str(self.hydrodataset_project) not in sys.path:
            sys.path.insert(0, str(self.hydrodataset_project))
        try:
            import xarray as xr
            import zarr
            from hydrodataset.converters.zarr_v3_builder import PilotDataset, _write_timeseries
        except ImportError as exc:
            raise RuntimeError("NetCDF conversion requires the hydrodataset environment with xarray and zarr") from exc

        dataset_id = _dataset_id(source, self.data_root)
        kind = "array_basin_timeseries"
        logical_asset_id = _logical_asset_id(source, self.data_root, self.existing_zarr_root, kind)
        output = self._timeseries_output(dataset_id)
        fingerprint = _fingerprint([source])
        if self._is_current(logical_asset_id, output, fingerprint):
            return self._current_record(logical_asset_id, source), False
        if output.exists():
            raise FileExistsError(f"Refusing to replace existing array store: {output}")
        with xr.open_dataset(source, decode_times=False) as dataset:
            basin_count = len(dataset.data_vars)
            time_count = int(dataset.sizes.get("time", 0))
            dynamic_count = int(dataset.sizes.get("dynamic_features", 0))
        if not basin_count or not time_count or not dynamic_count:
            raise ValueError("standard NetCDF must contain basin variables, time, and dynamic_features")
        staging = self.lake_root / "cache" / "materialization-staging" / dataset_id
        staging.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f"earthzarr-{dataset_id}-") as local_directory:
            local_source = Path(local_directory) / source.name
            shutil.copyfile(source, local_source)
            spec = PilotDataset(
                dataset_id=dataset_id,
                timeseries_source=local_source,
                attributes_source=None,
                output_root=staging,
                timeseries_chunk=(min(128, basin_count), min(3650, time_count)),
                attributes_chunk=min(1024, basin_count),
                resume=True,
            )
            built = _write_timeseries(spec)
            zarr.consolidate_metadata(built, zarr_format=3)
            output.parent.mkdir(parents=True, exist_ok=True)
            os.replace(built, output)
            built_manifest = built.with_name(f"{built.name}.manifest.json")
            output_manifest = output.with_name("daily.zarr.manifest.json")
            built_manifest.replace(output_manifest)
            manifest_payload = json.loads(output_manifest.read_text(encoding="utf-8"))
            manifest_payload["source"] = str(source)
            _atomic_json(output_manifest, manifest_payload)
        output_integrity = tree_integrity(output)
        record = MaterializationRecord(
            source=str(source),
            source_fingerprint=fingerprint,
            output=output.relative_to(self.lake_root).as_posix(),
            dataset_id=dataset_id,
            kind=kind,
            status="materialized",
            logical_asset_id=logical_asset_id,
            row_count=basin_count,
            byte_size=sum(path.stat().st_size for path in output.rglob("*") if path.is_file()),
            content_sha256=output_integrity[0],
            file_count=output_integrity[2],
            directory_count=output_integrity[3],
            columns=["basin", "time", "dynamic_features"],
            updated_at=_utc_now(),
        )
        self._store_record(record)
        return record, True

    def _store_record(self, record: MaterializationRecord) -> None:
        existing = self.records.get(record.logical_asset_id)
        if existing is not None and existing.output != record.output:
            raise ManifestError(
                f"logical_asset_id is already bound to another output: {record.logical_asset_id}"
            )
        for owner in self.records.values():
            if owner.logical_asset_id != record.logical_asset_id and owner.output == record.output:
                raise ManifestError(f"physical output is already owned by another logical ID: {record.output}")
        self.records[record.logical_asset_id] = record

    def _current_record(self, logical_asset_id: str, source: Path) -> MaterializationRecord:
        record = self.records[logical_asset_id]
        if record.source != str(source):
            record = replace(record, source=str(source), updated_at=_utc_now())
            self.records[logical_asset_id] = record
        return record

    def _is_current(self, logical_asset_id: str, output: Path, fingerprint: str) -> bool:
        record = self.records.get(logical_asset_id)
        if not record or record.source_fingerprint != fingerprint or (self.lake_root / record.output) != output:
            return False
        if not output.exists() or not record.content_sha256:
            return False
        try:
            if output.is_file():
                actual = (file_sha256(output), output.stat().st_size, 1, 0)
            elif output.is_dir():
                actual = tree_integrity(output)
            else:
                return False
        except (OSError, ValueError):
            return False
        return (
            actual[0] == record.content_sha256
            and actual[1] == record.byte_size
            and actual[2] == record.file_count
            and actual[3] == record.directory_count
        )

    def _track(self, result: dict[str, Any], record: MaterializationRecord, *, changed: bool) -> None:
        result["materialized" if changed else "skipped"] += 1
        payload = asdict(record)
        result["records"].append(payload)
        if self.on_record:
            self.on_record(payload, "materialized" if changed else "skipped")

    def _track_failure(self, result: dict[str, Any], source: Path, kind: str, error: Exception) -> None:
        result["failed"] += 1
        try:
            dataset_id = _dataset_id(source, self.data_root)
        except ValueError:
            dataset_id = self._zarr_dataset_id(source)
        payload = asdict(
            MaterializationRecord(
                source=str(source),
                source_fingerprint=_fingerprint([source]),
                output="",
                dataset_id=dataset_id,
                kind=kind,
                status="failed",
                logical_asset_id=_logical_asset_id(source, self.data_root, self.existing_zarr_root, kind),
                updated_at=_utc_now(),
                error=str(error),
            )
        )
        result["records"].append(payload)
        if self.on_record:
            self.on_record(payload, "failed")

    def _load_manifest(self) -> dict[str, MaterializationRecord]:
        return load_materialization_manifest(self.manifest_path, self.lake_root)

    def _save_manifest(self) -> None:
        with ProtocolCommit(
            self.lake_root,
            kind="materialization_manifest",
            metadata={"record_count": len(self.records)},
        ) as commit:
            # ProtocolCommit serializes publication, but each materializer may
            # have loaded the manifest before another process committed its
            # record.  Merge from the live manifest while holding the commit
            # lock so concurrent runs cannot silently drop one another's
            # provenance.
            live_records = self._load_manifest()
            for logical_asset_id, record in self.records.items():
                if logical_asset_id != record.logical_asset_id:
                    raise ManifestError("in-memory materialization ownership key is inconsistent")
                existing = live_records.get(logical_asset_id)
                if existing is not None and existing.output != record.output:
                    raise ManifestError(
                        f"logical_asset_id is already bound to another output: {logical_asset_id}"
                    )
                live_records[logical_asset_id] = record
            live_records = _validate_manifest_records(live_records.values(), self.lake_root)
            self.records = live_records
            payload = {
                "protocol": "EarthZarrProtocol",
                "protocol_version": "0.1.0",
                "kind": "hydrodataset_materialization",
                "data_root": str(self.data_root),
                "ownership_key": "logical_asset_id",
                "generated_at": _utc_now(),
                "records": [
                    asdict(record)
                    for record in sorted(
                        self.records.values(),
                        key=lambda item: (item.dataset_id, item.kind, item.output),
                    )
                ],
            }
            _atomic_json(commit.staged_path(self.manifest_path), payload)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inventory", "materialize"))
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--existing-zarr-root", type=Path, default=DEFAULT_EXISTING_ZARR_ROOT)
    parser.add_argument("--hydrodataset-project", type=Path, default=DEFAULT_HYDRODATASET_PROJECT)
    parser.add_argument("--kind", choices=("entities", "arrays", "all"), default="all")
    parser.add_argument("--dataset", action="append", default=[], help="Dataset id; repeat to select multiple datasets")
    parser.add_argument("--limit", type=int, help="Safety limit per materialization category")
    parser.add_argument("--convert-netcdf", action="store_true", help="Convert standardized *_D.nc files missing a published Zarr")
    parser.add_argument("--verbose", action="store_true", help="Include every discovered source path in inventory output")
    return parser


def main() -> None:
    args = _parser().parse_args()
    materializer = HydroMaterializer(
        args.data_root,
        args.lake_root,
        existing_zarr_root=args.existing_zarr_root,
        hydrodataset_project=args.hydrodataset_project,
    )
    if args.command == "inventory":
        result = materializer.inventory(include_paths=args.verbose)
    else:
        kinds = {"entities", "arrays"} if args.kind == "all" else {args.kind}
        result = materializer.materialize(
            kinds=kinds,
            datasets=set(args.dataset) or None,
            limit=args.limit,
            convert_netcdf=args.convert_netcdf,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
