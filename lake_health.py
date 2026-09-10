"""Persistent integrity audits for an Earth Lake protocol root."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from acquisition.store import AcquisitionStore
from earth_lake import PROCESSING_VERSION, PROTOCOL_NAME, PROTOCOL_VERSION, REGISTRY_SCHEMAS
from hydro_materializer import file_sha256, load_materialization_manifest, tree_integrity, tree_stats
from materialization import MaterializationStore


TERMINAL_AUDIT_STATUSES = {"completed", "failed", "interrupted"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quote_sqlite_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_affinity(declared_type: str) -> str:
    normalized = declared_type.upper()
    if "INT" in normalized:
        return "INTEGER"
    if any(token in normalized for token in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if "BLOB" in normalized or not normalized:
        return "BLOB"
    if any(token in normalized for token in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    return "NUMERIC"


_SQLITE_NUMERIC_LITERAL = re.compile(
    r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\Z"
)


def _strip_sqlite_outer_parentheses(value: str) -> str:
    """Remove only balanced parentheses surrounding the whole expression."""

    while len(value) >= 2 and value[0] == "(" and value[-1] == ")":
        depth = 0
        quote: str | None = None
        closes_at_end = True
        index = 0
        while index < len(value):
            character = value[index]
            if quote:
                if character == quote:
                    if index + 1 < len(value) and value[index + 1] == quote:
                        index += 1
                    else:
                        quote = None
            elif character in {"'", '"'}:
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0 and index != len(value) - 1:
                    closes_at_end = False
                    break
                if depth < 0:
                    closes_at_end = False
                    break
            index += 1
        if quote or depth != 0 or not closes_at_end:
            return value
        value = value[1:-1].strip()
    return value


def _normalize_sqlite_default(value: Any) -> tuple[str, Any]:
    """Normalize the literal default forms used by the runtime stores.

    SQLite exposes ``PRAGMA table_info`` defaults as SQL source text.  The
    runtime contracts use NULL, numeric literals, and quoted text; those forms
    are normalized without attempting to parse general SQL. Unsupported
    expressions remain exact apart from surrounding whitespace.
    """

    if value is None:
        return ("absent", None)
    text = _strip_sqlite_outer_parentheses(str(value).strip())
    if text.upper() == "NULL":
        return ("null", None)
    if len(text) >= 2 and text[0] in {"'", '"'} and text[-1] == text[0]:
        quote = text[0]
        return ("text", text[1:-1].replace(quote + quote, quote))
    if _SQLITE_NUMERIC_LITERAL.fullmatch(text):
        try:
            return ("number", Decimal(text))
        except InvalidOperation:
            pass
    return ("expression", re.sub(r"\s+", " ", text))


def _sqlite_defaults_equal(expected: Any, actual: Any) -> bool:
    return _normalize_sqlite_default(expected) == _normalize_sqlite_default(actual)


def _skip_sqlite_quoted(sql: str, index: int) -> int:
    quote = sql[index]
    closing = "]" if quote == "[" else quote
    index += 1
    while index < len(sql):
        if sql[index] == closing:
            if closing != "]" and index + 1 < len(sql) and sql[index + 1] == closing:
                index += 2
                continue
            return index + 1
        index += 1
    return len(sql)


def _skip_sqlite_trivia(sql: str, index: int) -> int:
    while index < len(sql):
        if sql[index].isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            return len(sql) if newline < 0 else _skip_sqlite_trivia(sql, newline + 1)
        if sql.startswith("/*", index):
            comment_end = sql.find("*/", index + 2)
            return len(sql) if comment_end < 0 else _skip_sqlite_trivia(sql, comment_end + 2)
        break
    return index


def _sqlite_table_has_check_constraint(sql: str) -> bool:
    """Detect real CHECK tokens in table DDL without parsing SQL expressions."""

    index = 0
    while index < len(sql):
        if sql[index] in {"'", '"', "`", "["}:
            index = _skip_sqlite_quoted(sql, index)
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            comment_end = sql.find("*/", index + 2)
            index = len(sql) if comment_end < 0 else comment_end + 2
            continue
        if sql[index].isalpha() or sql[index] == "_":
            start = index
            index += 1
            while index < len(sql) and (sql[index].isalnum() or sql[index] in {"_", "$"}):
                index += 1
            if sql[start:index].upper() == "CHECK":
                candidate = _skip_sqlite_trivia(sql, index)
                if candidate < len(sql) and sql[candidate] == "(":
                    return True
            continue
        index += 1
    return False


def _validate_sqlite_runtime_schema(
    connection: sqlite3.Connection,
    schema: dict[str, dict[str, Any]],
) -> None:
    """Validate a store-owned schema contract without opening the store.

    The connection is supplied by readiness in SQLite read-only mode.  The
    contract intentionally permits additive columns/tables, while rejecting
    any missing or incompatible structure that the runtime store relies on.
    """

    for table, table_schema in schema.items():
        identifier = _quote_sqlite_identifier(table)
        rows = connection.execute(f"PRAGMA table_info({identifier})").fetchall()
        if not rows:
            raise ValueError(f"required table is missing: {table}")
        ddl_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not ddl_row or not isinstance(ddl_row[0], str):
            raise ValueError(f"table DDL is unavailable: {table}")
        if _sqlite_table_has_check_constraint(ddl_row[0]):
            raise ValueError(f"unexpected CHECK constraint on runtime table: {table}")
        actual_columns = {str(row[1]): row for row in rows}
        expected_columns = table_schema["columns"]
        missing_columns = sorted(set(expected_columns) - set(actual_columns))
        if missing_columns:
            raise ValueError(
                f"required columns missing from {table}: {', '.join(missing_columns)}"
            )

        expected_primary_key = tuple(
            column
            for column, expected in expected_columns.items()
            if "primary_key" in expected
        )
        actual_primary_key = tuple(
            str(row[1])
            for row in sorted(
                (row for row in rows if int(row[5]) > 0),
                key=lambda row: int(row[5]),
            )
        )
        if actual_primary_key != expected_primary_key:
            raise ValueError(
                f"incompatible primary key for {table}: "
                f"expected ({', '.join(expected_primary_key)}), "
                f"got ({', '.join(actual_primary_key)})"
            )

        for column, expected in expected_columns.items():
            actual = actual_columns[column]
            expected_affinity = _sqlite_affinity(str(expected["type"]))
            actual_affinity = _sqlite_affinity(str(actual[2] or ""))
            if actual_affinity != expected_affinity:
                raise ValueError(
                    f"incompatible type for {table}.{column}: "
                    f"expected {expected_affinity}, got {actual_affinity}"
                )
            if "not_null" not in expected:
                raise ValueError(f"runtime schema omits nullability for {table}.{column}")
            if bool(actual[3]) != bool(expected["not_null"]):
                expected_state = "NOT NULL" if expected["not_null"] else "nullable"
                actual_state = "NOT NULL" if actual[3] else "nullable"
                raise ValueError(
                    f"incompatible nullability for {table}.{column}: "
                    f"expected {expected_state}, got {actual_state}"
                )
            if "primary_key" in expected and int(actual[5]) != int(expected["primary_key"]):
                raise ValueError(
                    f"incompatible primary key position for {table}.{column}: "
                    f"expected {expected['primary_key']}, got {actual[5]}"
                )
            if "default" in expected and not _sqlite_defaults_equal(expected["default"], actual[4]):
                raise ValueError(
                    f"incompatible default for {table}.{column}: "
                    f"expected {expected['default']!r}, got {actual[4]!r}"
                )

        for column, actual in actual_columns.items():
            if column in expected_columns:
                continue
            if bool(actual[3]) and _normalize_sqlite_default(actual[4])[0] in {"absent", "null"}:
                raise ValueError(
                    f"incompatible extra column on {table}: "
                    f"{column} is NOT NULL without a usable default"
                )

        unique_signatures: list[tuple[tuple[str, ...], bool]] = []
        for index in connection.execute(f"PRAGMA index_list({identifier})").fetchall():
            if not index[2]:
                continue
            index_name = _quote_sqlite_identifier(str(index[1]))
            indexed_columns = tuple(
                str(row[2])
                for row in sorted(
                    connection.execute(f"PRAGMA index_info({index_name})").fetchall(),
                    key=lambda row: int(row[0]),
                )
            )
            if indexed_columns:
                partial = bool(index[4]) if len(index) > 4 else False
                unique_signatures.append((indexed_columns, partial))
        for expected_unique in table_schema.get("unique_constraints", ()):
            expected_signature = tuple(expected_unique)
            if not any(
                indexed_columns == expected_signature and not partial
                for indexed_columns, partial in unique_signatures
            ):
                raise ValueError(
                    f"required unique constraint is missing from {table}: "
                    f"{', '.join(expected_unique)}"
                )

        actual_foreign_keys: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        grouped_foreign_keys: dict[int, list[tuple[int, str, str, str]]] = {}
        for row in connection.execute(f"PRAGMA foreign_key_list({identifier})").fetchall():
            grouped_foreign_keys.setdefault(int(row[0]), []).append(
                (int(row[1]), str(row[3]), str(row[2]), str(row[4]))
            )
        for rows_for_key in grouped_foreign_keys.values():
            ordered = sorted(rows_for_key, key=lambda row: row[0])
            referenced_table = ordered[0][2]
            columns = tuple(
                (row[1], row[3]) for row in ordered
            )
            actual_foreign_keys.add((referenced_table, columns))
        for expected_foreign_key in table_schema.get("foreign_keys", ()):
            if expected_foreign_key not in actual_foreign_keys:
                referenced_table, columns = expected_foreign_key
                raise ValueError(
                    f"required foreign key is missing from {table}: "
                    f"{referenced_table}({', '.join(source for source, _ in columns)})"
                )


class LakeHealthAuditor:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.manifest_root = self.root / "manifests" / "health"

    def run(self, run_id: str, *, full_checksum: bool = False) -> dict[str, Any]:
        report = {
            "run_id": run_id,
            "status": "running",
            "mode": "full" if full_checksum else "quick",
            "root": str(self.root),
            "started_at": _now(),
            "finished_at": None,
            "issues": [],
            "stats": {},
        }
        self._save(report)
        try:
            self._audit(report, full_checksum=full_checksum)
            report["status"] = "completed"
        except Exception as exc:
            report["status"] = "failed"
            report["error"] = str(exc)
        report["finished_at"] = _now()
        self._summarize(report)
        self._save(report)
        _atomic_json(self.manifest_root / "latest.json", report)
        return report

    def _audit(self, report: dict[str, Any], *, full_checksum: bool) -> None:
        issues: list[dict[str, Any]] = report["issues"]
        registries: dict[str, list[dict[str, Any]]] = {}
        for table in REGISTRY_SCHEMAS:
            path = self.root / "registry" / f"{table}.parquet"
            if not path.is_file():
                if table == "processing_runs":
                    # EarthLake intentionally creates this registry lazily on
                    # the first Processing Run; an empty fresh lake is still
                    # ready to accept normal work.
                    registries[table] = []
                    continue
                self._issue(issues, "registry_missing", "critical", "registry", path, f"Missing {table} registry", "reinitialize_registry")
                registries[table] = []
                continue
            try:
                table_data = pq.read_table(path)
                if not table_data.schema.equals(REGISTRY_SCHEMAS[table], check_metadata=False):
                    self._issue(issues, "registry_schema_mismatch", "error", "registry", path, f"{table} schema does not match protocol", "migrate_registry")
                registries[table] = table_data.to_pylist()
            except Exception as exc:
                self._issue(issues, "registry_unreadable", "critical", "registry", path, f"Cannot read {table}: {exc}", "restore_or_reindex")
                registries[table] = []

        assets = registries.get("assets", [])
        registered_paths: set[str] = set()
        registered_ids: set[str] = set()
        checked_bytes = 0
        for asset in assets:
            relative = asset.get("local_path")
            asset_id = asset.get("asset_id")
            if isinstance(asset_id, str):
                registered_ids.add(asset_id)
            if not isinstance(relative, str) or not relative:
                self._issue(issues, "asset_path_missing", "error", "asset", None, f"Asset {asset_id or 'unknown'} has no local path", "reindex_asset")
                continue
            registered_paths.add(relative)
            path = (self.root / relative).resolve()
            try:
                path.relative_to(self.root)
            except ValueError:
                self._issue(issues, "asset_path_escape", "critical", "asset", path, "Registered path escapes the lake root", "remove_registry_row")
                continue
            if not path.is_file():
                self._issue(issues, "asset_file_missing", "error", "asset", path, "Registered source asset is missing", "mark_missing_or_redownload")
                continue
            size = path.stat().st_size
            checked_bytes += size
            expected_size = asset.get("byte_size")
            if expected_size is not None and int(expected_size) != size:
                self._issue(issues, "asset_size_mismatch", "error", "asset", path, f"Expected {expected_size} bytes, found {size}", "redownload_or_reindex")
            if full_checksum and asset.get("checksum_sha256"):
                checksum = _sha256(path)
                if checksum != asset["checksum_sha256"]:
                    self._issue(issues, "asset_checksum_mismatch", "critical", "asset", path, "SHA-256 does not match registry", "redownload")

        source_files: set[str] = set()
        source_root = self.root / "source"
        if source_root.exists():
            for current_root, directory_names, file_names in os.walk(source_root):
                directory_names[:] = [name for name in directory_names if not name.startswith(".")]
                for name in file_names:
                    if name.startswith(".") or name == "metadata.json":
                        continue
                    path = Path(current_root) / name
                    relative = path.relative_to(self.root).as_posix()
                    if name.endswith(".part"):
                        self._issue(issues, "partial_download", "warning", "source", path, "Resumable partial download remains", "resume_or_cancel_run")
                        continue
                    source_files.add(relative)
        for relative in sorted(source_files - registered_paths):
            self._issue(issues, "unregistered_source", "warning", "source", self.root / relative, "Source file is not registered", "reindex_asset")

        stac_ids: set[str] = set()
        stac_root = self.root / "catalog" / "stac"
        if stac_root.exists():
            for path in stac_root.rglob("*.json"):
                if path.name.startswith("._") or path.name == ".DS_Store":
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    self._issue(issues, "stac_unreadable", "error", "stac", path, f"Invalid STAC JSON: {exc}", "rebuild_stac")
                    continue
                for asset in (payload.get("assets") or {}).values():
                    if isinstance(asset, dict) and isinstance(asset.get("earthzarr:asset_id"), str):
                        stac_ids.add(asset["earthzarr:asset_id"])
        for asset_id in sorted(registered_ids - stac_ids):
            self._issue(issues, "stac_asset_missing", "error", "stac", None, f"Registry asset {asset_id} is absent from STAC", "reindex_asset")
        for asset_id in sorted(stac_ids - registered_ids):
            self._issue(issues, "orphan_stac_asset", "warning", "stac", None, f"STAC asset {asset_id} is absent from Registry", "rebuild_stac")

        protocol_commit_count = 0
        pending_protocol_commits = 0
        protocol_commit_root = self.root / "manifests" / "protocol_commits"
        if protocol_commit_root.exists():
            journal_ids: set[str] = set()
            for path in protocol_commit_root.glob("*.json"):
                if path.name.startswith("._"):
                    continue
                protocol_commit_count += 1
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    self._issue(issues, "protocol_commit_unreadable", "critical", "protocol_commit", path, f"Invalid Protocol Commit journal: {exc}", "restore_commit_journal")
                    continue
                commit_id = payload.get("commit_id")
                if isinstance(commit_id, str):
                    journal_ids.add(commit_id)
                if payload.get("status") in {"preparing", "publishing"}:
                    pending_protocol_commits += 1
                    self._issue(issues, "protocol_commit_pending", "critical", "protocol_commit", path, f"Protocol Commit remains {payload.get('status')}", "recover_protocol_commit")
            staging_root = protocol_commit_root / ".staging"
            if staging_root.exists():
                for stage in staging_root.iterdir():
                    if stage.is_dir() and stage.name not in journal_ids:
                        self._issue(issues, "protocol_commit_orphan_stage", "warning", "protocol_commit", stage, "Protocol Commit staging directory has no journal", "remove_or_adopt_stage")

        materialized_outputs: set[str] = set()
        materialization_manifest = self.root / "manifests" / "materializations" / "hydrodatasets.json"
        if materialization_manifest.is_file():
            try:
                records = load_materialization_manifest(materialization_manifest, self.root)
                for record in records.values():
                    output = record.output
                    candidate = (self.root / output).resolve()
                    materialized_outputs.add(output)
                    if not candidate.exists():
                        self._issue(issues, "materialized_output_missing", "error", "materialization", self.root / output, "Manifest output is missing", "rematerialize")
                        continue
                    try:
                        if candidate.is_file():
                            actual_size = candidate.stat().st_size
                            actual_file_count = 1
                            actual_directory_count = 0
                        elif candidate.is_dir():
                            actual_size, actual_file_count, actual_directory_count = tree_stats(candidate)
                        else:
                            raise ValueError("output is neither a regular file nor a directory")
                    except (OSError, ValueError) as exc:
                        self._issue(issues, "materialized_output_unreadable", "error", "materialization", candidate, f"Cannot validate materialized output: {exc}", "rematerialize")
                        continue
                    expected_checksum = record.content_sha256
                    expected_size = record.byte_size
                    expected_file_count = record.file_count
                    try:
                        size_mismatch = int(expected_size) != actual_size
                        file_count_mismatch = int(expected_file_count) != actual_file_count
                        directory_count_mismatch = int(record.directory_count) != actual_directory_count
                    except (TypeError, ValueError):
                        size_mismatch = file_count_mismatch = directory_count_mismatch = True
                    checksum_mismatch = False
                    if full_checksum:
                        actual_checksum = file_sha256(candidate) if candidate.is_file() else tree_integrity(candidate)[0]
                        checksum_mismatch = expected_checksum != actual_checksum
                    if checksum_mismatch or size_mismatch or file_count_mismatch or directory_count_mismatch:
                        self._issue(issues, "materialized_output_integrity_mismatch", "critical", "materialization", candidate, "Materialization manifest does not match published content", "rematerialize")
            except Exception as exc:
                self._issue(issues, "materialization_manifest_unreadable", "critical", "materialization", materialization_manifest, f"Cannot read materialization manifest: {exc}", "rebuild_manifest")

        actual_outputs: set[str] = set()
        entity_root = self.root / "entities"
        if entity_root.exists():
            actual_outputs.update(
                path.relative_to(self.root).as_posix()
                for path in entity_root.rglob("*")
                if path.is_file()
                and not path.name.startswith("._")
                and path.suffix.lower() in {".parquet", ".geoparquet", ".geojson"}
            )
        array_root = self.root / "arrays"
        if array_root.exists():
            for current_root, directory_names, _ in os.walk(array_root):
                directory_names[:] = [
                    name
                    for name in directory_names
                    if not name.startswith(".") and not name.endswith(".partial")
                ]
                current = Path(current_root)
                if current.name.endswith(".zarr"):
                    actual_outputs.add(current.relative_to(self.root).as_posix())
                    directory_names.clear()
        for relative in sorted(actual_outputs - materialized_outputs):
            self._issue(issues, "unmanifested_materialization", "warning", "materialization", self.root / relative, "Materialized output has no manifest record", "adopt_output")

        try:
            disk = os.statvfs(self.root)
            report["stats"]["disk_total_bytes"] = disk.f_frsize * disk.f_blocks
            report["stats"]["disk_free_bytes"] = disk.f_frsize * disk.f_bavail
        except OSError:
            pass
        report["stats"].update(
            registered_assets=len(registered_ids),
            source_files=len(source_files),
            checked_bytes=checked_bytes,
            stac_assets=len(stac_ids),
            materialized_outputs=len(actual_outputs),
            protocol_commits=protocol_commit_count,
            pending_protocol_commits=pending_protocol_commits,
        )

    @staticmethod
    def _issue(issues: list[dict[str, Any]], code: str, severity: str, category: str, path: Path | None, message: str, repair: str) -> None:
        issues.append({
            "issue_id": hashlib.sha256(f"{code}|{path}|{message}".encode()).hexdigest()[:20],
            "code": code,
            "severity": severity,
            "category": category,
            "path": str(path) if path else None,
            "message": message,
            "repair": repair,
        })

    @staticmethod
    def _summarize(report: dict[str, Any]) -> None:
        counts = {severity: 0 for severity in ("critical", "error", "warning", "info")}
        for issue in report.get("issues", []):
            counts[issue.get("severity", "info")] = counts.get(issue.get("severity", "info"), 0) + 1
        report["summary"] = {**counts, "total": sum(counts.values())}
        report["health_status"] = (
            "unhealthy"
            if counts["critical"] or counts["error"]
            else "degraded"
            if counts["warning"]
            else "healthy"
        )

    def readiness(self) -> dict[str, Any]:
        """Perform a bounded, read-only local readiness check.

        Readiness is deliberately narrower than a full lake audit.  It checks
        only the protocol schema, critical Parquet registries, and local state
        stores needed to accept normal API work.  It never contacts STAC,
        downloads, materializes, reconciles, or repairs anything.
        """

        checks: dict[str, dict[str, Any]] = {}
        if not self.root.is_dir() or not os.access(self.root, os.R_OK | os.X_OK):
            checks["lake_root"] = {"status": "failed", "reason": "lake root is not readable"}
        else:
            checks["lake_root"] = {"status": "ok"}

        schema_path = self.root / "protocol" / "schema_version.json"
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            if not isinstance(schema, dict):
                raise ValueError("schema must be a JSON object")
            expected = {
                "protocol": PROTOCOL_NAME,
                "version": PROTOCOL_VERSION,
                "processing_version": PROCESSING_VERSION,
            }
            for field, expected_value in expected.items():
                value = schema.get(field)
                if not isinstance(value, str) or value != expected_value:
                    raise ValueError(f"{field} is incompatible with the running protocol")
            checks["protocol_schema"] = {"status": "ok", "version": schema["version"]}
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, AttributeError, TypeError, ValueError) as exc:
            checks["protocol_schema"] = {"status": "failed", "reason": f"schema unavailable: {exc}"}

        for table, expected_schema in REGISTRY_SCHEMAS.items():
            path = self.root / "registry" / f"{table}.parquet"
            try:
                if table == "processing_runs" and not path.exists():
                    checks[f"registry:{table}"] = {"status": "ok", "state": "not_initialized"}
                    continue
                actual_schema = pq.ParquetFile(path).schema_arrow
                if not actual_schema.equals(expected_schema, check_metadata=False):
                    raise ValueError("schema does not match protocol")
                checks[f"registry:{table}"] = {"status": "ok"}
            except (OSError, ValueError) as exc:
                checks[f"registry:{table}"] = {"status": "failed", "reason": str(exc)}

        stores = {
            "acquisition_state.sqlite": (AcquisitionStore.SCHEMA_VERSION, AcquisitionStore.RUNTIME_SCHEMA),
            "materialization_state.sqlite": (
                MaterializationStore.SCHEMA_VERSION,
                MaterializationStore.RUNTIME_SCHEMA,
            ),
        }
        for name, (expected_version, runtime_schema) in stores.items():
            path = self.root / "registry" / name
            try:
                connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
                try:
                    result = connection.execute("PRAGMA quick_check").fetchone()
                    if not result or result[0] != "ok":
                        raise ValueError(f"quick_check returned {result[0] if result else 'no result'}")
                    actual_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                    if actual_version != expected_version:
                        raise ValueError(
                            f"schema version {actual_version} is incompatible; expected {expected_version}"
                        )
                    _validate_sqlite_runtime_schema(connection, runtime_schema)
                finally:
                    connection.close()
                checks[f"store:{name}"] = {"status": "ok"}
            except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                checks[f"store:{name}"] = {"status": "failed", "reason": str(exc)}

        failed = [name for name, value in checks.items() if value.get("status") != "ok"]
        return {
            "status": "ready" if not failed else "not_ready",
            "health_status": "healthy" if not failed else "unhealthy",
            "checks": checks,
            "failed_checks": failed,
            "external_stac": {"status": "not_checked", "critical": False},
        }

    def _save(self, report: dict[str, Any]) -> None:
        _atomic_json(self.manifest_root / f"{report['run_id']}.json", report)


class LakeHealthManager:
    def __init__(self, root: str | Path):
        self.auditor = LakeHealthAuditor(root)
        self.root = self.auditor.root
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._recover_interrupted()

    def start(self, *, full_checksum: bool = False) -> dict[str, Any]:
        run_id = f"audit-{uuid.uuid4()}"

        def execute() -> None:
            try:
                self.auditor.run(run_id, full_checksum=full_checksum)
            finally:
                with self._lock:
                    self._threads.pop(run_id, None)

        thread = threading.Thread(target=execute, name=f"lake-health-{run_id}", daemon=True)
        with self._lock:
            if self._threads:
                raise ValueError("A data health audit is already running")
            queued = {
                "run_id": run_id,
                "status": "queued",
                "mode": "full" if full_checksum else "quick",
                "root": str(self.root),
                "started_at": None,
                "finished_at": None,
                "issues": [],
                "stats": {},
                "health_status": "unknown",
                "summary": {"critical": 0, "error": 0, "warning": 0, "info": 0, "total": 0},
            }
            self.auditor._save(queued)
            self._threads[run_id] = thread
        thread.start()
        return queued

    def get(self, run_id: str) -> dict[str, Any]:
        path = self.auditor.manifest_root / f"{run_id}.json"
        if not path.is_file():
            raise KeyError(run_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def latest(self) -> dict[str, Any] | None:
        path = self.auditor.manifest_root / "latest.json"
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def list(self, *, limit: int = 20) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        for path in sorted(self.auditor.manifest_root.glob("audit-*.json"), reverse=True):
            try:
                reports.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
            if len(reports) >= limit:
                break
        return reports

    def _recover_interrupted(self) -> None:
        for path in self.auditor.manifest_root.glob("audit-*.json"):
            try:
                report = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if report.get("status") in {"queued", "running"}:
                report.update(status="interrupted", finished_at=_now(), error="Audit interrupted by process restart")
                LakeHealthAuditor._summarize(report)
                _atomic_json(path, report)
