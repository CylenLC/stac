"""Durable execution and control of hydrology materialization runs."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hydro_materializer import (
    DEFAULT_DATA_ROOT,
    DEFAULT_EXISTING_ZARR_ROOT,
    DEFAULT_HYDRODATASET_PROJECT,
    HydroMaterializer,
)


TERMINAL_STATUSES = {"completed", "partial", "failed", "cancelled"}
ACTIVE_STATUSES = {"queued", "inventory", "running", "finalizing"}
CONTROL_STATUSES = {"queued", "inventory", "running"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


class MaterializationInterrupted(BaseException):
    pass


class MaterializationStore:
    SCHEMA_VERSION = 1
    RUNTIME_SCHEMA = {
        "materialization_runs": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": False, "primary_key": 1},
                "request_json": {"type": "TEXT", "not_null": True},
                "status": {"type": "TEXT", "not_null": True},
                "message": {"type": "TEXT", "not_null": True},
                "created_at": {"type": "TEXT", "not_null": True},
                "updated_at": {"type": "TEXT", "not_null": True},
                "started_at": {"type": "TEXT", "not_null": False},
                "finished_at": {"type": "TEXT", "not_null": False},
                "total_sources": {"type": "INTEGER", "not_null": True, "default": "0"},
                "completed_sources": {"type": "INTEGER", "not_null": True, "default": "0"},
                "materialized_count": {"type": "INTEGER", "not_null": True, "default": "0"},
                "skipped_count": {"type": "INTEGER", "not_null": True, "default": "0"},
                "failed_count": {"type": "INTEGER", "not_null": True, "default": "0"},
                "current_source": {"type": "TEXT", "not_null": False},
                "result_json": {"type": "TEXT", "not_null": True, "default": "'{}'"},
                "error": {"type": "TEXT", "not_null": False},
            },
            "checks": (),
        },
    }

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.path = self.root / "registry" / "materialization_state.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with self.transaction() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS materialization_runs (
                    run_id TEXT PRIMARY KEY,
                    request_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    total_sources INTEGER NOT NULL DEFAULT 0,
                    completed_sources INTEGER NOT NULL DEFAULT 0,
                    materialized_count INTEGER NOT NULL DEFAULT 0,
                    skipped_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    current_source TEXT,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS materialization_runs_created_idx
                    ON materialization_runs(created_at DESC, run_id DESC);
                """
            )
            db.execute(
                """
                UPDATE materialization_runs
                SET status='queued', message='Recovered after process restart.',
                    updated_at=?, completed_sources=0, materialized_count=0,
                    skipped_count=0, failed_count=0, current_source=NULL,
                    result_json='{}', error=NULL, finished_at=NULL
                WHERE status IN ('running','inventory','finalizing')
                """,
                (_now(),),
            )
            schema_version = db.execute("PRAGMA user_version").fetchone()[0]
            if schema_version not in {0, self.SCHEMA_VERSION}:
                raise sqlite3.DatabaseError(
                    f"unsupported materialization schema version: {schema_version}"
                )
            db.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")

    @contextmanager
    def transaction(self):
        connection = None
        for attempt in range(6):
            try:
                connection = sqlite3.connect(self.path, timeout=30)
                break
            except sqlite3.OperationalError:
                if attempt == 5:
                    raise
                time.sleep(0.25 * (attempt + 1))
        assert connection is not None
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 120000")
        connection.execute("PRAGMA synchronous = NORMAL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def create(self, request: dict[str, Any]) -> str:
        run_id = f"mat-{uuid.uuid4()}"
        timestamp = _now()
        with self._lock, self.transaction() as db:
            db.execute(
                """
                INSERT INTO materialization_runs(
                    run_id,request_json,status,message,created_at,updated_at
                ) VALUES(?,?,'queued','Materialization queued.',?,?)
                """,
                (run_id, json.dumps(request, sort_keys=True), timestamp, timestamp),
            )
        return run_id

    def get(self, run_id: str) -> dict[str, Any] | None:
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM materialization_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return self._row(row) if row else None

    def list(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute(
                """
                SELECT * FROM materialization_runs
                ORDER BY created_at DESC, run_id DESC LIMIT ?
                """,
                (max(1, min(limit, 200)),),
            ).fetchall()
        return [self._row(row) for row in rows]

    def queued_ids(self) -> list[str]:
        with self.transaction() as db:
            rows = db.execute(
                """
                SELECT run_id FROM materialization_runs
                WHERE status='queued' ORDER BY created_at, run_id
                """
            ).fetchall()
        return [str(row[0]) for row in rows]

    def update(self, run_id: str, **values: Any) -> None:
        values["updated_at"] = _now()
        assignments = ", ".join(f"{key}=?" for key in values)
        with self._lock, self.transaction() as db:
            cursor = db.execute(
                f"UPDATE materialization_runs SET {assignments} WHERE run_id=?",
                (*values.values(), run_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(run_id)

    def transition(self, run_id: str, expected: set[str], **values: Any) -> bool:
        if not expected:
            return False
        values["updated_at"] = _now()
        assignments = ", ".join(f"{key}=?" for key in values)
        placeholders = ",".join("?" for _ in expected)
        with self._lock, self.transaction() as db:
            cursor = db.execute(
                f"UPDATE materialization_runs SET {assignments} "
                f"WHERE run_id=? AND status IN ({placeholders})",
                (*values.values(), run_id, *sorted(expected)),
            )
        return cursor.rowcount == 1

    @staticmethod
    def _row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["request"] = json.loads(result.pop("request_json"))
        result["result"] = json.loads(result.pop("result_json"))
        total = result["total_sources"]
        result["progress"] = min(100.0, round(result["completed_sources"] / total * 100, 2)) if total else (100.0 if result["status"] == "completed" else 0.0)
        return result


class MaterializationManager:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.store = MaterializationStore(self.root)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def create_run(self, request: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_request(request)
        run_id = self.store.create(normalized)
        self._wake.set()
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def list_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self.store.list(limit=limit)

    def pause_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            raise ValueError(f"cannot pause {run['status']} run")
        if not self.store.transition(
            run_id,
            CONTROL_STATUSES,
            status="paused",
            message="Materialization paused by user.",
        ):
            raise ValueError(f"cannot pause {self.get_run(run_id)['status']} run")
        return self.get_run(run_id)

    def resume_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"paused", "failed", "partial"}:
            raise ValueError(f"cannot resume {run['status']} run")
        if not self.store.transition(
            run_id,
            {"paused", "failed", "partial"},
            status="queued",
            message="Materialization queued for resume.",
            completed_sources=0,
            materialized_count=0,
            skipped_count=0,
            failed_count=0,
            result_json="{}",
            error=None,
            finished_at=None,
        ):
            raise ValueError(f"cannot resume {self.get_run(run_id)['status']} run")
        self._wake.set()
        return self.get_run(run_id)

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            if run["status"] == "cancelled":
                return run
            raise ValueError(f"cannot cancel {run['status']} run")
        if not self.store.transition(
            run_id,
            CONTROL_STATUSES | {"paused"},
            status="cancelled",
            message="Materialization cancelled by user.",
            finished_at=_now(),
        ):
            raise ValueError(f"cannot cancel {self.get_run(run_id)['status']} run")
        return self.get_run(run_id)

    def retry_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"failed", "partial"}:
            raise ValueError(f"cannot retry {run['status']} run")
        if not self.store.transition(
            run_id,
            {"failed", "partial"},
            status="queued",
            message="Materialization queued for retry.",
            completed_sources=0,
            materialized_count=0,
            skipped_count=0,
            failed_count=0,
            result_json="{}",
            error=None,
            finished_at=None,
        ):
            raise ValueError(f"cannot retry {self.get_run(run_id)['status']} run")
        self._wake.set()
        return self.get_run(run_id)

    def run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES or run["status"] == "paused":
            return run
        request = run["request"]
        try:
            materializer = HydroMaterializer(
                request["data_root"],
                self.root,
                existing_zarr_root=request["existing_zarr_root"],
                hydrodataset_project=request["hydrodataset_project"],
                on_record=lambda record, outcome: self._record_progress(run_id, record, outcome),
                check_control=lambda: self._check_control(run_id),
            )
            if not self.store.transition(
                run_id,
                {"queued"},
                status="inventory",
                message="Discovering materialization sources.",
                started_at=run["started_at"] or _now(),
            ):
                return self.get_run(run_id)
            total = self._source_count(materializer, request)
            if not self.store.transition(
                run_id,
                {"inventory"},
                total_sources=total,
                status="running",
                message=f"Materializing {total} sources.",
            ):
                return self.get_run(run_id)
            result = materializer.materialize(
                kinds=set(request["kinds"]),
                datasets=set(request["datasets"]) or None,
                limit=request["limit"],
                convert_netcdf=request["convert_netcdf"],
                publish_manifest=False,
            )
            status = "partial" if result["failed"] and (result["materialized"] or result["skipped"]) else "failed" if result["failed"] else "completed"
            self._check_control(run_id)
            if not self.store.transition(
                run_id,
                {"running"},
                status="finalizing",
                message="Publishing materialization manifest.",
                result_json=json.dumps(result),
            ):
                return self.get_run(run_id)
            materializer.publish_manifest()
            self.store.transition(
                run_id,
                {"finalizing"},
                status=status,
                message=f"Finished: {result['materialized']} materialized, {result['skipped']} reused, {result['failed']} failed.",
                finished_at=_now(),
                current_source=None,
            )
        except MaterializationInterrupted:
            pass
        except Exception as exc:
            self.store.transition(
                run_id,
                ACTIVE_STATUSES,
                status="failed",
                message=f"Materialization failed: {exc}",
                error=str(exc),
                finished_at=_now(),
            )
        return self.get_run(run_id)

    def start_scheduler(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()

        def loop() -> None:
            while not self._stop.is_set():
                self._wake.clear()
                try:
                    for run_id in self.store.queued_ids():
                        self.run(run_id)
                except (OSError, sqlite3.OperationalError) as exc:
                    import logging
                    logging.getLogger(__name__).warning("Materialization scheduler retrying: %s", exc)
                self._wake.wait(5.0)

        self._thread = threading.Thread(target=loop, name="materialization-scheduler", daemon=True)
        self._thread.start()

    def stop_scheduler(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _record_progress(self, run_id: str, record: dict[str, Any], outcome: str) -> None:
        run = self.get_run(run_id)
        values = {
            "completed_sources": run["completed_sources"] + 1,
            "current_source": record.get("source"),
            "materialized_count": run["materialized_count"] + (outcome == "materialized"),
            "skipped_count": run["skipped_count"] + (outcome == "skipped"),
            "failed_count": run["failed_count"] + (outcome == "failed"),
        }
        self.store.update(run_id, **values)

    def _check_control(self, run_id: str) -> None:
        if self.get_run(run_id)["status"] in {"paused", "cancelled"}:
            raise MaterializationInterrupted()

    @staticmethod
    def _source_count(materializer: HydroMaterializer, request: dict[str, Any]) -> int:
        datasets = set(request["datasets"]) or None
        shapefiles, attributes, netcdf = materializer._discover_sources(datasets)
        entities = len(shapefiles) + len(attributes) if "entities" in request["kinds"] else 0
        existing = len([
            path for path in materializer.existing_zarr_root.glob("*.zarr")
            if path.is_dir() and (not datasets or materializer._zarr_dataset_id(path) in datasets)
        ]) if "arrays" in request["kinds"] and materializer.existing_zarr_root.exists() else 0
        arrays = existing + (len(netcdf) if request["convert_netcdf"] and "arrays" in request["kinds"] else 0)
        if request["limit"] is not None:
            entities = min(entities, request["limit"])
            existing = min(existing, request["limit"])
            arrays = existing + (min(len(netcdf), request["limit"]) if request["convert_netcdf"] and "arrays" in request["kinds"] else 0)
        return entities + arrays

    @staticmethod
    def _normalize_request(request: dict[str, Any]) -> dict[str, Any]:
        kinds = sorted(set(request.get("kinds") or ["entities", "arrays"]))
        if not set(kinds).issubset({"entities", "arrays"}) or not kinds:
            raise ValueError("kinds must contain entities and/or arrays")
        limit = request.get("limit")
        if limit is not None and int(limit) < 1:
            raise ValueError("limit must be at least 1")
        data_root = Path(request.get("data_root") or DEFAULT_DATA_ROOT).expanduser().resolve()
        if not data_root.is_dir():
            raise ValueError(f"data_root is not a readable directory: {data_root}")
        return {
            "data_root": str(data_root),
            "existing_zarr_root": str(Path(request.get("existing_zarr_root") or DEFAULT_EXISTING_ZARR_ROOT).expanduser().resolve()),
            "hydrodataset_project": str(Path(request.get("hydrodataset_project") or DEFAULT_HYDRODATASET_PROJECT).expanduser().resolve()),
            "kinds": kinds,
            "datasets": sorted(set(request.get("datasets") or [])),
            "limit": int(limit) if limit is not None else None,
            "convert_netcdf": bool(request.get("convert_netcdf", False)),
        }
