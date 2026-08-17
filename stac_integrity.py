"""Transfer-time integrity checks for STAC Assets.

This module deliberately does not resolve Asset identity or choose Assets. It
only answers whether a transferred local file is safe to publish.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import re
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

import rasterio
from rasterio.errors import RasterioError


_SCIENTIFIC_MEDIA_TYPES = {
    "image/tiff",
    "image/geotiff",
    "application/x-netcdf",
    "application/netcdf",
    "application/vnd+zarr",
    "application/x-zarr",
}
_TEXT_ERROR_TYPES = {
    "text/html",
    "text/plain",
    "application/json",
    "application/xml",
    "text/xml",
}
_SUPPORTED_CHECKSUMS = {"sha256": hashlib.sha256, "md5": hashlib.md5}
_FORMAT_BY_MEDIA_TYPE = {
    "image/tiff": "GeoTIFF",
    "image/geotiff": "GeoTIFF",
    "application/json": "JSON",
    "application/geo+json": "JSON",
    "application/x-netcdf": "NetCDF",
    "application/netcdf": "NetCDF",
    "application/vnd+zarr": "Zarr",
    "application/x-zarr": "Zarr",
}
_FORMAT_BY_EXTENSION = {
    ".tif": "GeoTIFF",
    ".tiff": "GeoTIFF",
    ".json": "JSON",
    ".nc": "NetCDF",
    ".nc4": "NetCDF",
    ".cdf": "NetCDF",
    ".zarr": "Zarr",
}
_UNKNOWN_SCIENTIFIC_TOKENS = {"hdf", "hdf4", "hdf5", "netcdf", "zarr", "grib", "scientific"}


@dataclass(frozen=True)
class TransferMetadata:
    status_code: int | None
    headers: dict[str, str]
    bytes_received: int
    resumed: bool = False

    def _header(self, name: str) -> str | None:
        value = next(
            (raw for key, raw in self.headers.items() if key.lower() == name.lower()),
            None,
        )
        return None if value is None else str(value)

    def has_header(self, name: str) -> bool:
        return any(key.lower() == name.lower() for key in self.headers)

    @property
    def content_length(self) -> int | None:
        value = self._header("content-length")
        try:
            parsed = int(value) if value is not None else None
            return parsed if parsed is not None and parsed >= 0 else None
        except (TypeError, ValueError):
            return None

    @property
    def content_length_invalid(self) -> bool:
        if not self.has_header("content-length"):
            return False
        value = self._header("content-length")
        try:
            parsed = int(value) if value is not None else None
            return parsed is None or parsed < 0
        except (TypeError, ValueError):
            return True

    @property
    def content_type(self) -> str | None:
        value = self._header("content-type")
        return str(value).split(";", 1)[0].strip().lower() if value else None

    @property
    def content_range(self) -> tuple[int, int, int] | None:
        value = self._header("content-range")
        if not value:
            return None
        match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", value.strip(), re.IGNORECASE)
        if not match:
            return None
        start, end, total = (int(part) for part in match.groups())
        if end < start or total <= end:
            return None
        return start, end, total

    @property
    def content_range_invalid(self) -> bool:
        return self.has_header("content-range") and self.content_range is None

    @property
    def etag(self) -> str | None:
        return self._header("etag")

    @property
    def last_modified(self) -> str | None:
        return self._header("last-modified")


@dataclass(frozen=True)
class IntegrityResult:
    ok: bool
    checks: dict[str, str]
    expected_size: int | None
    actual_size: int
    source_checksum: str | None
    source_checksum_algorithm: str | None
    local_sha256: str | None
    checksum_verified: bool | None
    detected_type: str | None
    response_status: int | None
    response_content_type: str | None
    reason_code: str | None = None
    reason: str | None = None
    validated_path: str | None = None
    validated_size: int | None = None
    validated_mtime_ns: int | None = None
    validated_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def bind_path(self, path: str | Path) -> "IntegrityResult":
        """Bind a passing result to the exact published file stat.

        The binding is intentionally metadata, not part of the canonical STAC
        identity. It lets the Registry reject a result produced for a
        different path or for bytes that were subsequently replaced.
        """

        target = Path(path).resolve()
        stat = target.stat()
        if not target.is_file() or stat.st_size != self.actual_size:
            raise ValueError("integrity result cannot be bound to a different file")
        return replace(
            self,
            validated_path=str(target),
            validated_size=stat.st_size,
            validated_mtime_ns=stat.st_mtime_ns,
            validated_sha256=self.local_sha256,
        )


class IntegrityError(ValueError):
    """Raised when an Asset fails the authoritative publication gate."""

    def __init__(self, result: IntegrityResult):
        self.result = result
        code = result.reason_code or "integrity_failed"
        detail = result.reason or "download failed integrity validation"
        super().__init__(f"{code}: {detail}")


def _media_type(asset: Mapping[str, Any] | None, path: Path) -> str | None:
    declared = str((asset or {}).get("type") or (asset or {}).get("media_type") or "")
    if declared:
        return declared.split(";", 1)[0].strip().lower()
    return mimetypes.guess_type(path.name)[0]


def _extension(path: Path, asset: Mapping[str, Any] | None) -> str:
    href = str((asset or {}).get("href") or path.name)
    return Path(unquote(urlparse(href).path)).suffix.lower() or path.suffix.lower()


def _source_size(asset: Mapping[str, Any] | None) -> tuple[int | None, bool]:
    asset = asset or {}
    if "file:size" not in asset:
        return None, False
    value = asset.get("file:size")
    try:
        if isinstance(value, bool) or value is None:
            return None, True
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
            parsed = int(value.strip())
        else:
            return None, True
        return (parsed, False) if parsed >= 0 else (None, True)
    except (TypeError, ValueError):
        return None, True


def _source_checksum(asset: Mapping[str, Any] | None) -> tuple[str | None, str | None, bool]:
    asset = asset or {}
    for algorithm in ("sha256", "md5"):
        for key in (f"checksum:{algorithm}", f"file:checksum:{algorithm}"):
            value = asset.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower(), algorithm, True

    checksum = asset.get("checksum")
    if isinstance(checksum, dict):
        algorithm = str(checksum.get("algorithm") or "").lower()
        value = checksum.get("value")
        if isinstance(value, str) and value.strip():
            return value.strip().lower(), algorithm or None, True
    if isinstance(checksum, str) and checksum.strip():
        return checksum.strip().lower(), None, True
    return None, None, False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _failed(
    *,
    checks: dict[str, str],
    expected_size: int | None,
    actual_size: int,
    source_checksum: str | None,
    source_algorithm: str | None,
    local_sha256: str | None,
    checksum_verified: bool | None,
    detected_type: str | None,
    response: TransferMetadata | None,
    code: str,
    reason: str,
) -> IntegrityResult:
    checks[code] = "failed"
    return IntegrityResult(
        ok=False,
        checks=checks,
        expected_size=expected_size,
        actual_size=actual_size,
        source_checksum=source_checksum,
        source_checksum_algorithm=source_algorithm,
        local_sha256=local_sha256,
        checksum_verified=checksum_verified,
        detected_type=detected_type,
        response_status=response.status_code if response else None,
        response_content_type=response.content_type if response else None,
        reason_code=code,
        reason=reason,
    )


def _format_kind(declared_type: str | None, extension: str) -> str:
    declared_kind = _FORMAT_BY_MEDIA_TYPE.get(declared_type or "")
    extension_kind = _FORMAT_BY_EXTENSION.get(extension)
    normalized = (declared_type or "").lower()
    if declared_kind and extension_kind and declared_kind != extension_kind:
        return "CONFLICT"
    if declared_kind:
        return declared_kind
    if any(token in normalized for token in _UNKNOWN_SCIENTIFIC_TOKENS):
        return "UNKNOWN_SCIENTIFIC"
    if extension in {".hdf", ".h4", ".hdf4", ".h5", ".hdf5", ".grib", ".grb"}:
        return "UNKNOWN_SCIENTIFIC"
    if extension_kind:
        return extension_kind
    return "GenericBinary"


def _validate_netcdf(path: Path) -> tuple[bool, str | None, str]:
    try:
        import xarray as xr
    except ImportError:
        return False, "NetCDF validation requires the optional xarray/netCDF4 dependencies", "unsupported_asset_format"
    try:
        dataset = xr.open_dataset(path, decode_cf=False, mask_and_scale=False)
        try:
            _ = tuple(dataset.dims)
            _ = tuple(dataset.variables)
        finally:
            dataset.close()
        return True, None, ""
    except Exception as exc:  # xarray exposes backend-specific exception types.
        return False, str(exc), "invalid_netcdf"


class IntegrityGate:
    """Single PASS/FAIL decision used before final-file publication."""

    @staticmethod
    def validate(
        path: str | Path,
        asset: Mapping[str, Any] | None = None,
        *,
        response: TransferMetadata | None = None,
        expected_size: int | None = None,
    ) -> IntegrityResult:
        path = Path(path)
        metadata_provided = asset is not None
        asset = asset or {}
        checks: dict[str, str] = {}
        actual_size = path.stat().st_size if path.is_file() else 0
        source_checksum, source_algorithm, checksum_available = _source_checksum(asset)
        source_size, source_size_invalid = _source_size(asset)
        if source_size_invalid:
            return _failed(
                checks=checks, expected_size=None, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="invalid_source_size",
                reason="Asset file:size is present but is not a non-negative integer",
            )
        if expected_size is None:
            expected_size = source_size
        elif isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size < 0:
            return _failed(
                checks=checks, expected_size=None, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="invalid_expected_size",
                reason="expected_size must be a non-negative integer",
            )

        declared_type = _media_type(asset, path)
        extension = _extension(path, asset)
        # The legacy low-level transfer helper may intentionally omit STAC
        # metadata. Keep that compatibility path transport-only; a suffix is
        # a format signal only when the resolver supplied Asset metadata.
        format_kind = _format_kind(declared_type, extension) if metadata_provided else "GenericBinary"
        scientific = format_kind in {"GeoTIFF", "NetCDF", "Zarr", "CONFLICT", "UNKNOWN_SCIENTIFIC"}

        if response is not None:
            if response.status_code not in {200, 206}:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="http_status",
                    reason=f"unexpected HTTP status {response.status_code}",
                )
            checks["http_status"] = "passed"
            if response.content_length_invalid:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="invalid_content_length",
                    reason="Content-Length is present but is not a non-negative integer",
                )
            if response.content_length is not None:
                if response.bytes_received != response.content_length:
                    return _failed(
                        checks=checks, expected_size=expected_size, actual_size=actual_size,
                        source_checksum=source_checksum, source_algorithm=source_algorithm,
                        local_sha256=None, checksum_verified=None, detected_type=None,
                        response=response, code="content_length_mismatch",
                        reason=f"response declared {response.content_length} bytes but received {response.bytes_received}",
                    )
                checks["content_length"] = "passed"
            else:
                checks["content_length"] = "unavailable"

            if response.resumed:
                if response.status_code != 206 or response.content_range is None or response.content_range_invalid:
                    return _failed(
                        checks=checks, expected_size=expected_size, actual_size=actual_size,
                        source_checksum=source_checksum, source_algorithm=source_algorithm,
                        local_sha256=None, checksum_verified=None, detected_type=None,
                        response=response, code="invalid_content_range",
                        reason="resumed transfer did not provide a valid Content-Range",
                    )
                start, end, total = response.content_range
                expected_range_length = end - start + 1
                if response.bytes_received != expected_range_length or actual_size != total:
                    return _failed(
                        checks=checks, expected_size=expected_size, actual_size=actual_size,
                        source_checksum=source_checksum, source_algorithm=source_algorithm,
                        local_sha256=None, checksum_verified=None, detected_type=None,
                        response=response, code="content_range_mismatch",
                        reason=f"Content-Range bytes {start}-{end}/{total} does not match the received/final size",
                    )
                checks["content_range"] = "passed"
            elif response.status_code == 206:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="unexpected_partial_response",
                    reason="HTTP 206 is only valid for a validated resumed transfer",
                )
            else:
                checks["content_range"] = "not_applicable"

            if scientific and response.content_type in _TEXT_ERROR_TYPES:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="unexpected_content_type",
                    reason=f"scientific Asset returned {response.content_type}",
                )
            checks["content_type"] = "passed" if response.content_type else "unavailable"

        if format_kind == "CONFLICT":
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="asset_format_conflict",
                reason="STAC media type conflicts with the Asset href extension",
            )
        if format_kind == "UNKNOWN_SCIENTIFIC":
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="unsupported_asset_format",
                reason="scientific Asset format has no structural validator",
            )

        if format_kind == "Zarr":
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="unsupported_asset_format",
                reason="Zarr requires directory/object-store materialization and is not a single-file transfer",
            )

        if actual_size == 0:
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="zero_byte_file", reason="Asset file is empty",
            )
        if expected_size is not None and actual_size != expected_size:
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="source_size_mismatch",
                reason=f"expected {expected_size} bytes but found {actual_size}",
            )
        checks["size"] = "passed" if expected_size is not None else "unavailable"

        with path.open("rb") as file:
            prefix = file.read(4096)
        stripped = prefix.lstrip().lower()
        if scientific and (
            stripped.startswith(b"<html")
            or stripped.startswith(b"<!doctype html")
            or stripped.startswith(b"<?xml")
            or (stripped.startswith(b"{") and b"\"error\"" in stripped[:1024])
        ):
            return _failed(
                checks=checks, expected_size=expected_size, actual_size=actual_size,
                source_checksum=source_checksum, source_algorithm=source_algorithm,
                local_sha256=None, checksum_verified=None, detected_type=None,
                response=response, code="error_document",
                reason="scientific Asset contains an HTML/XML/JSON error document",
            )
        checks["content"] = "passed"

        detected_type: str | None = None
        if format_kind == "GeoTIFF":
            try:
                with rasterio.open(path) as dataset:
                    if dataset.driver != "GTiff" or dataset.width <= 0 or dataset.height <= 0 or dataset.count <= 0:
                        raise ValueError("invalid GeoTIFF dimensions or band count")
                detected_type = "GTiff"
                checks["format"] = "passed"
            except (RasterioError, OSError, ValueError) as exc:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="invalid_geotiff", reason=str(exc),
                )
        elif format_kind == "NetCDF":
            valid, reason, code = _validate_netcdf(path)
            if not valid:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code=code, reason=reason or "NetCDF validation failed",
                )
            detected_type = "NetCDF"
            checks["format"] = "passed"
        elif format_kind == "JSON":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                detected_type = "JSON"
                checks["format"] = "passed"
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                return _failed(
                    checks=checks, expected_size=expected_size, actual_size=actual_size,
                    source_checksum=source_checksum, source_algorithm=source_algorithm,
                    local_sha256=None, checksum_verified=None, detected_type=None,
                    response=response, code="invalid_json", reason=str(exc),
                )
        else:
            checks["format"] = "unavailable"

        local_sha256 = _sha256(path)
        checksum_verified: bool | None = None
        if checksum_available:
            if source_algorithm not in _SUPPORTED_CHECKSUMS:
                checks["checksum"] = "unsupported"
                checksum_verified = False
            else:
                function = _SUPPORTED_CHECKSUMS[source_algorithm]
                digest = function()
                with path.open("rb") as file:
                    for chunk in iter(lambda: file.read(1024 * 1024), b""):
                        digest.update(chunk)
                actual_checksum = digest.hexdigest().lower()
                checksum_verified = actual_checksum == source_checksum
                checks["checksum"] = "passed" if checksum_verified else "failed"
                if not checksum_verified:
                    return _failed(
                        checks=checks, expected_size=expected_size, actual_size=actual_size,
                        source_checksum=source_checksum, source_algorithm=source_algorithm,
                        local_sha256=local_sha256, checksum_verified=False, detected_type=detected_type,
                        response=response, code="source_checksum_mismatch",
                        reason=f"expected {source_checksum} but found {actual_checksum}",
                    )
        else:
            checks["checksum"] = "unavailable"

        result = IntegrityResult(
            ok=True,
            checks=checks,
            expected_size=expected_size,
            actual_size=actual_size,
            source_checksum=source_checksum,
            source_checksum_algorithm=source_algorithm,
            local_sha256=local_sha256,
            checksum_verified=checksum_verified,
            detected_type=detected_type,
            response_status=response.status_code if response else None,
            response_content_type=response.content_type if response else None,
        )
        return result.bind_path(path)
