"""Canonical STAC Item and Asset identity helpers.

This module is deliberately dependency-free so storage, acquisition, and
registry code all use the same identity and path component rules.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any


_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_IDENTITY_FIELDS = ("catalog", "collection", "item_id", "asset_key")


def _text(value: object, fallback: str) -> str:
    value = str(value) if value is not None else ""
    return value if value else fallback


def stable_hash(value: str, length: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def safe_component(value: object, fallback: str = "unknown", max_length: int = 120) -> str:
    """Return a readable, path-safe, collision-resistant component.

    A component that needed path stripping, sanitization, or truncation gets a
    digest of its original value. This keeps common names readable while
    preventing values such as ``A/B`` and ``A:B`` from silently collapsing to
    the same path after normalization.
    """

    raw = _text(value, fallback)
    normalized = raw.replace("\\", "/")
    basename = PurePosixPath(normalized).name
    if basename in {"", ".", ".."}:
        basename = fallback
    sanitized = _SAFE_CHARS.sub("_", basename).strip("._") or fallback
    truncated = sanitized[:max_length]
    changed = normalized != basename or sanitized != basename or len(sanitized) > max_length
    if changed:
        suffix = stable_hash(raw)
        keep = max(1, max_length - len(suffix) - 2)
        return f"{truncated[:keep]}__{suffix}"
    return truncated


def collection_identity(catalog: object, collection: object) -> str:
    """Return the stable readable ID used for a catalog/Collection pair."""

    return f"{safe_component(catalog)}__{safe_component(collection)}"


@dataclass(frozen=True)
class AssetIdentity:
    """The canonical logical identity of one STAC Asset."""

    catalog: str
    collection: str
    item_id: str
    asset_key: str

    def __post_init__(self) -> None:
        for field in _IDENTITY_FIELDS:
            value = getattr(self, field)
            if not isinstance(value, str) or not value:
                raise ValueError(f"canonical asset identity requires a non-empty {field}")

    @classmethod
    def from_item(cls, catalog: object, item: dict[str, Any], asset_key: object) -> "AssetIdentity":
        collection = item.get("collection")
        item_id = item.get("id")
        if not collection or not item_id or not asset_key:
            raise ValueError("STAC Item must include collection, id, and asset key")
        return cls(str(catalog), str(collection), str(item_id), str(asset_key))

    def as_dict(self) -> dict[str, str]:
        return {field: getattr(self, field) for field in _IDENTITY_FIELDS}

    def serialize(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def digest(self, length: int = 64) -> str:
        return hashlib.sha256(self.serialize().encode("utf-8")).hexdigest()[:length]

    def attempt_digest(self, run_id: str, length: int = 64) -> str:
        payload = {"run_id": str(run_id), **self.as_dict()}
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:length]
