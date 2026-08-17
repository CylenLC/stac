"""Shared catalog search and download helpers for the API and CLI."""

import json
import hashlib
import mimetypes
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import unquote, urlparse

import planetary_computer as pc
import requests
from pystac_client import Client
from requests.adapters import HTTPAdapter
from shapely.wkt import loads as load_wkt
from urllib3.util.retry import Retry

from stac_identity import AssetIdentity, safe_component
from stac_integrity import IntegrityError, IntegrityGate, IntegrityResult, TransferMetadata

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


DATA = "data"
AUXILIARY_DATA = "auxiliary"
VISUAL = "visual"
THUMBNAIL = "thumbnail"
METADATA = "metadata"
UNKNOWN = "unknown"

_PREVIEW_MEDIA_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "text/html",
    "application/json",
    "application/xml",
    "text/xml",
}
_SCIENTIFIC_MEDIA_TYPES = {
    "image/tiff",
    "application/x-netcdf",
    "application/netcdf",
    "application/vnd+zarr",
    "application/x-zarr",
}
_EXCLUDED_ROLES = {VISUAL, "overview", THUMBNAIL, METADATA}
_NEGATIVE_ASSET_TOKENS = {
    "thumbnail", "thumb", "preview", "browse", "quicklook", "overview",
    "visual", "render", "rendered",
}
_AUXILIARY_ROLES = {AUXILIARY_DATA, "quality", "mask", "cloud", "cloud-mask"}
_KNOWN_DATA_KEYS = {
    "data", "red", "green", "blue", "nir", "swir", "swir1", "swir2",
    "fmask", "quality", "mask", "b01", "b02", "b03", "b04", "b05",
    "b06", "b07", "b08", "b8a", "b09", "b10", "b11", "b12",
}
_COLLECTION_POLICIES = {
    "HLSL30_V2.0": ("B04", "B05", "Fmask"),
    "HLSS30_V2.0": ("B04", "B8A", "Fmask"),
}


@dataclass(frozen=True)
class AssetDecision:
    asset_key: str
    category: str
    accepted: bool
    reason: str


@dataclass(frozen=True)
class AssetResolution:
    selected: tuple[tuple[str, dict[str, Any]], ...]
    decisions: tuple[AssetDecision, ...]
    policy: str


class AssetResolutionError(ValueError):
    """Raised when an Asset selection cannot be made without guessing."""

    def __init__(self, message: str, *, decisions: Iterable[AssetDecision] = ()):
        super().__init__(message)
        self.decisions = tuple(decisions)


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


def asset_filename(
    asset_key: str,
    asset: dict[str, Any],
    identity: AssetIdentity | None = None,
) -> str:
    """Return a collision-safe filename for a canonical asset.

    ``identity`` is required by acquisition paths. The legacy fallback keeps
    this helper source-compatible for callers that only need a display name;
    it must not be used as a cache key.
    """

    path = unquote(urlparse(asset.get("href", "")).path)
    basename = safe_component(Path(path).name, f"{safe_component(asset_key, 'asset')}.data")
    if identity is None:
        return basename

    media_type = str(asset.get("type") or asset.get("media_type") or "")
    extension = Path(basename).suffix.lower()
    if media_type:
        guessed = mimetypes.guess_extension(media_type.split(";", 1)[0].strip())
        if guessed:
            extension = guessed
    if not extension or extension == ".data":
        extension = ".data"
    return f"{safe_component(asset_key, 'asset')}__{identity.digest(16)}{extension}"


def canonical_asset_path(root: str | Path, identity: AssetIdentity, asset: dict[str, Any]) -> Path:
    """Build the one authoritative local path for a canonical Asset."""

    base = Path(root).resolve()
    path = (
        base
        / "source"
        / safe_component(identity.catalog)
        / safe_component(identity.collection)
        / safe_component(identity.item_id)
        / asset_filename(identity.asset_key, asset, identity)
    ).resolve()
    if base not in path.parents:
        raise ValueError("Canonical Asset path escapes the Earth Lake root")
    return path


def item_directory(
    output_dir: str,
    catalog: object,
    collection: object | None = None,
    item_id: object | None = None,
) -> Path:
    """Build a source Item directory including catalog and Collection.

    The three-argument form is retained for older callers and places those
    paths under an explicit ``legacy`` catalog namespace.
    """

    if item_id is None:
        item_id = collection
        collection = catalog
        catalog = "legacy"
    base = Path(output_dir).resolve()
    path = base / safe_component(catalog, "unknown") / safe_component(collection, "unknown") / safe_component(item_id, "unknown")
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


def _asset_extension(asset: dict[str, Any]) -> str:
    href = asset.get("href")
    if not isinstance(href, str):
        return ""
    return Path(unquote(urlparse(href).path)).suffix.lower()


def _asset_key_token(asset_key: str) -> str:
    return "".join(character for character in asset_key.lower() if character.isalnum())


def _negative_asset_signal(asset_key: str, asset: dict[str, Any]) -> str | None:
    """Return a preview/browse signal before weak data heuristics run."""

    key_token = _asset_key_token(asset_key)
    if key_token in _NEGATIVE_ASSET_TOKENS:
        return f"asset key={asset_key}"
    values = [
        str(asset.get("title") or ""),
        str(asset.get("description") or ""),
        Path(unquote(urlparse(str(asset.get("href") or "")).path)).stem,
    ]
    for value in values:
        matches = re.findall(r"[a-z0-9]+", value.lower())
        tokens = set(matches)
        normalized = "".join(matches)
        if tokens & _NEGATIVE_ASSET_TOKENS:
            signal = sorted(tokens & _NEGATIVE_ASSET_TOKENS)[0]
            return f"preview/browse signal={signal}"
        if normalized in _NEGATIVE_ASSET_TOKENS:
            return f"preview/browse signal={normalized}"
    return None


def _inspect_asset(asset_key: str, asset: Any) -> tuple[str, bool, int, str]:
    """Classify one Asset and return category, accepted, confidence, reason."""

    if not isinstance(asset, dict):
        return UNKNOWN, False, 0, "asset metadata is not an object"

    raw_roles = asset.get("roles") or []
    roles = {str(role).lower() for role in raw_roles if role is not None}
    negative_signal = _negative_asset_signal(asset_key, asset)
    if negative_signal:
        category = THUMBNAIL if _asset_key_token(asset_key) in {"thumbnail", "thumb", "preview"} else VISUAL
        return category, False, 3, negative_signal
    if "data" in roles:
        return DATA, True, 3, "role=data"
    if roles & _AUXILIARY_ROLES:
        return AUXILIARY_DATA, True, 3, f"role={sorted(roles & _AUXILIARY_ROLES)[0]}"
    if roles & _EXCLUDED_ROLES:
        role = sorted(roles & _EXCLUDED_ROLES)[0]
        category = {
            VISUAL: VISUAL,
            "overview": VISUAL,
            THUMBNAIL: THUMBNAIL,
            METADATA: METADATA,
        }[role]
        return category, False, 3, f"role={role}"

    media_type = str(asset.get("type") or asset.get("media_type") or "").split(";", 1)[0].strip().lower()
    if media_type in _PREVIEW_MEDIA_TYPES:
        category = THUMBNAIL if media_type in {"image/jpeg", "image/png", "image/gif"} else METADATA
        return category, False, 3, f"media_type={media_type}"

    extension = _asset_extension(asset)
    key_token = _asset_key_token(asset_key)
    if media_type in _SCIENTIFIC_MEDIA_TYPES:
        return DATA, True, 2, f"media_type={media_type}"
    if media_type == "application/octet-stream" and extension in DATA_EXTENSIONS:
        return DATA, True, 2, f"media_type={media_type}, extension={extension}"
    if key_token in {"fmask", "quality", "mask"}:
        return AUXILIARY_DATA, True, 1, "quality-like asset key"
    if extension in DATA_EXTENSIONS:
        return DATA, True, 2, f"extension={extension}"
    if key_token in {_asset_key_token(key) for key in _KNOWN_DATA_KEYS}:
        return DATA, True, 1, "data-like asset key"
    return UNKNOWN, False, 0, "no scientific data signal"


def _item_context(item: dict[str, Any], catalog: str) -> tuple[str, str, str]:
    return (
        str(catalog or item.get("catalog") or "unknown"),
        str(item.get("collection") or "unknown"),
        str(item.get("id") or "unknown"),
    )


def _resolution_error(
    item: dict[str, Any],
    catalog: str,
    selector: str,
    reason: str,
    decisions: Iterable[AssetDecision],
) -> AssetResolutionError:
    catalog_id, collection, item_id = _item_context(item, catalog)
    available = ", ".join(sorted(str(key) for key in (item.get("assets") or {}).keys())) or "<none>"
    decision_list = tuple(decisions)
    decision_text = "; ".join(
        f"{decision.asset_key}: {decision.reason}" for decision in decision_list
    )
    if decision_text:
        reason = f"{reason}; decisions: {decision_text}"
    return AssetResolutionError(
        f"Asset resolution failed for catalog={catalog_id}, collection={collection}, "
        f"item={item_id}, selector={selector}: {reason}; available assets: {available}",
        decisions=decision_list,
    )


def _validate_selected_hrefs(
    item: dict[str, Any],
    catalog: str,
    selector: str,
    selected_keys: Iterable[str],
    decisions: Iterable[AssetDecision],
) -> None:
    invalid = [
        key for key in selected_keys
        if not isinstance((item.get("assets") or {}).get(key, {}).get("href"), str)
        or not (item.get("assets") or {}).get(key, {}).get("href", "").strip()
    ]
    if invalid:
        raise _resolution_error(
            item,
            catalog,
            selector,
            f"selected Asset(s) have missing or invalid href: {', '.join(invalid)}",
            decisions,
        )


def resolve_assets(
    item: dict[str, Any],
    catalog: str,
    *,
    mode: str = "main",
    asset_keys: Iterable[str] | None = None,
) -> AssetResolution:
    """Resolve STAC Assets using one deterministic, fail-closed policy.

    Automatic modes select only scientific or auxiliary data. Explicit keys are
    authoritative and may select preview or metadata Assets intentionally.
    This function does not sign or request URLs; URL resolution remains a
    separate transfer concern.
    """

    if mode not in {"main", "all", "explicit"}:
        raise ValueError(f"unsupported Asset selection mode: {mode}")
    assets = item.get("assets") or {}
    if not isinstance(assets, dict):
        raise _resolution_error(item, catalog, mode, "Item assets is not an object", ())

    if asset_keys is not None:
        requested = list(dict.fromkeys(str(key) for key in asset_keys if str(key).strip()))
        if not requested:
            raise _resolution_error(item, catalog, "explicit", "no Asset keys were requested", ())
        selected: list[tuple[str, dict[str, Any]]] = []
        decisions: list[AssetDecision] = []
        for key in requested:
            if key not in assets:
                decisions.append(AssetDecision(key, UNKNOWN, False, "requested Asset key is missing"))
                continue
            asset = assets[key]
            category, _accepted, _confidence, reason = _inspect_asset(key, asset)
            if not isinstance(asset, dict):
                decisions.append(AssetDecision(key, category, False, reason))
                continue
            href = asset.get("href")
            if not isinstance(href, str) or not href.strip():
                decisions.append(AssetDecision(key, category, False, "selected Asset has no valid href"))
                continue
            decisions.append(AssetDecision(key, category, True, "explicit Asset key"))
            selected.append((key, asset))
        if len(selected) != len(requested):
            raise _resolution_error(
                item, catalog, "explicit", "one or more requested Assets are unavailable or invalid", decisions
            )
        return AssetResolution(tuple(selected), tuple(decisions), "explicit")

    decisions_by_key: dict[str, AssetDecision] = {}
    inspected: dict[str, tuple[str, bool, int, str]] = {}
    for key in sorted(assets):
        category, accepted, confidence, reason = _inspect_asset(key, assets[key])
        inspected[key] = category, accepted, confidence, reason
        decisions_by_key[key] = AssetDecision(key, category, accepted, reason)

    collection = str(item.get("collection") or "")
    preferred = _COLLECTION_POLICIES.get(collection)
    if mode == "main" and preferred:
        selected_keys = [
            key for key in preferred
            if key in assets and inspected[key][1] and inspected[key][0] in {DATA, AUXILIARY_DATA}
        ]
        if selected_keys:
            _validate_selected_hrefs(item, catalog, mode, selected_keys, decisions_by_key.values())
            selected = tuple((key, assets[key]) for key in selected_keys)
            return AssetResolution(selected, tuple(decisions_by_key[key] for key in sorted(decisions_by_key)), f"collection:{collection}")
        raise _resolution_error(item, catalog, mode, f"Collection policy {collection} found no usable representative Asset", decisions_by_key.values())

    if mode == "all":
        selected_keys = [
            key for key in sorted(assets)
            if inspected[key][1] and inspected[key][0] in {DATA, AUXILIARY_DATA}
        ]
    else:
        candidates = [
            key for key in sorted(assets)
            if inspected[key][1] and inspected[key][0] in {DATA, AUXILIARY_DATA}
        ]
        if candidates:
            highest_confidence = max(inspected[key][2] for key in candidates)
            strongest = [key for key in candidates if inspected[key][2] == highest_confidence]
            selected_keys = strongest if len(strongest) == 1 else []
        else:
            selected_keys = []

    if not selected_keys:
        reason = "no policy-approved scientific or auxiliary Asset"
        if mode == "main" and len(assets) > 1:
            reason = "scientific Asset selection is ambiguous; no deterministic candidate"
        raise _resolution_error(item, catalog, mode, reason, decisions_by_key.values())
    _validate_selected_hrefs(item, catalog, mode, selected_keys, decisions_by_key.values())
    return AssetResolution(
        tuple((key, assets[key]) for key in selected_keys),
        tuple(decisions_by_key[key] for key in sorted(decisions_by_key)),
        f"collection:{collection}" if preferred else "generic",
    )


def is_main_asset(asset_key: str, asset: dict[str, Any], asset_count: int) -> bool:
    """Compatibility predicate backed by the provider-aware classifier."""

    category, accepted, _confidence, _reason = _inspect_asset(asset_key, asset)
    return accepted and category in {DATA, AUXILIARY_DATA}


def classify_asset(asset_key: str, asset: dict[str, Any]) -> str:
    """Expose the Resolver's category for downstream provenance."""

    return _inspect_asset(asset_key, asset)[0]


def selected_assets(item: dict[str, Any], only_main: bool) -> Iterable[tuple[str, dict[str, Any]]]:
    """Compatibility wrapper for callers that still use the old boolean API."""

    mode = "main" if only_main else "all"
    return resolve_assets(item, str(item.get("catalog") or "unknown"), mode=mode).selected


def get_asset_size(session: requests.Session, url: str) -> int:
    try:
        response = session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return int(response.headers.get("content-length", 0))
    except (requests.RequestException, ValueError):
        return 0


def _part_metadata_path(temporary: Path) -> Path:
    return temporary.with_name(f"{temporary.name}.meta")


def _read_part_metadata(path: Path) -> dict[str, str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _response_total(response: TransferMetadata) -> int | None:
    if response.status_code == 206 and response.content_range:
        return response.content_range[2]
    if response.content_length is not None and not response.content_length_invalid:
        return response.content_length
    return None


def _write_part_metadata(
    path: Path,
    response: TransferMetadata,
    *,
    identity: AssetIdentity | None,
    destination: Path,
    expected_total: int | None = None,
) -> None:
    if not path.is_file():
        return
    metadata_path = _part_metadata_path(path)
    temporary = metadata_path.with_name(f".{metadata_path.name}.tmp")
    temporary.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "identity": identity.digest() if identity else None,
                "destination": str(destination.resolve()),
                "partial_size": path.stat().st_size,
                "partial_sha256": _file_sha256(path),
                "remote_total": _response_total(response) or expected_total,
                "etag": response.etag,
                "last_modified": response.last_modified,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, metadata_path)


def _remove_partial_state(temporary: Path) -> None:
    temporary.unlink(missing_ok=True)
    _part_metadata_path(temporary).unlink(missing_ok=True)


def _transfer_failure(code: str, reason: str, status_code: int | None = None) -> IntegrityError:
    return IntegrityError(
        IntegrityResult(
            ok=False,
            checks={code: "failed"},
            expected_size=None,
            actual_size=0,
            source_checksum=None,
            source_checksum_algorithm=None,
            local_sha256=None,
            checksum_verified=None,
            detected_type=None,
            response_status=status_code,
            response_content_type=None,
            reason_code=code,
            reason=reason,
        )
    )


def download_asset_checked(
    session: requests.Session,
    url: str,
    destination: Path,
    on_chunk: Callable[[int], None] | None = None,
    *,
    asset_metadata: dict[str, Any] | None = None,
    expected_size: int | None = None,
    asset_identity: AssetIdentity | None = None,
) -> IntegrityResult:
    """Download one Asset with conservative, validator-bound resume semantics.

    A partial file without a saved remote validator is restarted from byte zero.
    Appending is allowed only after the server confirms both the saved validator
    and the exact Content-Range start/length. This intentionally favors
    correctness over saving an ambiguous partial transfer.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.resolve()
    temporary = destination.with_name(f"{destination.name}.part")
    for _ in range(2):
        existing = temporary.stat().st_size if temporary.exists() else 0
        part_metadata = _read_part_metadata(_part_metadata_path(temporary)) if existing else {}
        if existing:
            metadata_matches = (
                asset_identity is not None
                and
                part_metadata.get("schema_version") == 1
                and part_metadata.get("identity") == (asset_identity.digest() if asset_identity else None)
                and part_metadata.get("destination") == str(destination)
                and part_metadata.get("partial_size") == existing
                and part_metadata.get("partial_sha256") == _file_sha256(temporary)
            )
            if not metadata_matches:
                _remove_partial_state(temporary)
                existing = 0
                part_metadata = {}

        expected_total = part_metadata.get("remote_total") if existing else None
        if existing and isinstance(expected_total, int) and expected_total == existing:
            promotion_expected = expected_size if expected_size is not None else expected_total
            complete_result = IntegrityGate.validate(
                temporary,
                asset_metadata,
                expected_size=promotion_expected,
            )
            if complete_result.ok:
                os.replace(temporary, destination)
                _part_metadata_path(temporary).unlink(missing_ok=True)
                return complete_result.bind_path(destination)
            _remove_partial_state(temporary)
            existing = 0
            part_metadata = {}

        saved_validator = part_metadata.get("etag") or part_metadata.get("last_modified")
        if existing and not saved_validator:
            _remove_partial_state(temporary)
            existing = 0
            part_metadata = {}

        requested_resume = existing > 0
        headers: dict[str, str] = {}
        if requested_resume:
            headers["Range"] = f"bytes={existing}-"
            headers["If-Range"] = saved_validator

        try:
            downloaded = 0
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers) as response:
                status_code = int(getattr(response, "status_code", 0) or 0)
                response_headers = {
                    str(key): str(value)
                    for key, value in getattr(response, "headers", {}).items()
                }
                response.raise_for_status()
                if status_code not in {200, 206}:
                    raise _transfer_failure("http_status", f"unexpected HTTP status {status_code}", status_code)

                preliminary = TransferMetadata(
                    status_code=status_code,
                    headers=response_headers,
                    bytes_received=0,
                    resumed=requested_resume and status_code == 206,
                )
                if not requested_resume and status_code == 206:
                    raise _transfer_failure(
                        "unexpected_partial_response",
                        "HTTP 206 was returned without a validated partial file",
                        status_code,
                    )
                mode = "ab" if requested_resume and status_code == 206 else "wb"
                if requested_resume and status_code == 206:
                    if preliminary.content_range_invalid or preliminary.content_range is None:
                        raise _transfer_failure(
                            "invalid_content_range",
                            "resumed response did not provide a valid Content-Range",
                            status_code,
                        )
                    start, _end, _total = preliminary.content_range
                    if start != existing:
                        raise _transfer_failure(
                            "content_range_mismatch",
                            f"requested resume at byte {existing}, server returned byte {start}",
                            status_code,
                        )
                    if saved_validator and (
                        preliminary.etag is None and preliminary.last_modified is None
                    ):
                        _remove_partial_state(temporary)
                        continue
                    if part_metadata.get("etag") and preliminary.etag != part_metadata["etag"]:
                        _remove_partial_state(temporary)
                        continue
                    if part_metadata.get("last_modified") and preliminary.last_modified != part_metadata["last_modified"]:
                        _remove_partial_state(temporary)
                        continue

                try:
                    with open(temporary, mode) as file:
                        for chunk in response.iter_content(chunk_size=64 * 1024):
                            if chunk:
                                file.write(chunk)
                                downloaded += len(chunk)
                                if on_chunk:
                                    on_chunk(len(chunk))
                except Exception:
                    # A transport interruption may leave a useful prefix. It is
                    # resumable only when the prefix can be bound to the same
                    # canonical Asset, destination, and remote validator.
                    partial_validator = TransferMetadata(
                        status_code=status_code,
                        headers=response_headers,
                        bytes_received=downloaded,
                        resumed=requested_resume and status_code == 206,
                    )
                    if asset_identity is not None and (
                        partial_validator.etag or partial_validator.last_modified
                    ):
                        _write_part_metadata(
                            temporary,
                            partial_validator,
                            identity=asset_identity,
                            destination=destination,
                            expected_total=expected_size,
                        )
                    raise
                response_metadata = TransferMetadata(
                    status_code=status_code,
                    headers=response_headers,
                    bytes_received=downloaded,
                    resumed=requested_resume and status_code == 206,
                )
            result = IntegrityGate.validate(
                temporary,
                asset_metadata,
                response=response_metadata,
                expected_size=expected_size,
            )
            if not result.ok:
                raise IntegrityError(result)
            _write_part_metadata(
                temporary,
                response_metadata,
                identity=asset_identity,
                destination=destination,
                expected_total=expected_size if expected_size is not None else result.expected_size,
            )
            os.replace(temporary, destination)
            _part_metadata_path(temporary).unlink(missing_ok=True)
            return result.bind_path(destination)
        except IntegrityError:
            _remove_partial_state(temporary)
            raise
        except Exception:
            # A transport interruption is resumable only when the validator
            # companion was written before the interruption. Without it, the
            # next invocation safely restarts rather than appending blindly.
            raise


def download_asset(
    session: requests.Session,
    url: str,
    destination: Path,
    on_chunk: Callable[[int], None] | None = None,
    *,
    asset_metadata: dict[str, Any] | None = None,
    expected_size: int | None = None,
    asset_identity: AssetIdentity | None = None,
) -> int:
    """Backward-compatible transfer helper.

    Production acquisition paths use ``download_asset_checked`` with the
    resolved Asset metadata. This wrapper preserves the historical integer
    return value for low-level callers and tests.
    """

    temporary = destination.with_name(f"{destination.name}.part")
    existing = temporary.stat().st_size if temporary.exists() else 0
    result = download_asset_checked(
        session,
        url,
        destination,
        on_chunk,
        asset_metadata=asset_metadata,
        expected_size=expected_size,
        asset_identity=asset_identity,
    )
    return result.actual_size - existing if result.response_status == 206 else result.actual_size
