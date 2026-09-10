import json
import sqlite3
import threading
import hashlib
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from stac_core import canonical_asset_path
from stac_identity import AssetIdentity


def now() -> str:
    return datetime.now(UTC).isoformat()


class AcquisitionStore:
    IDENTITY_SCHEMA_VERSION = 3
    SCHEMA_VERSION = IDENTITY_SCHEMA_VERSION
    RUNTIME_SCHEMA = {
        "acquisition_runs": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": False, "primary_key": 1},
                "idempotency_key": {"type": "TEXT", "not_null": True},
                "request_json": {"type": "TEXT", "not_null": True},
                "status": {"type": "TEXT", "not_null": True},
                "message": {"type": "TEXT", "not_null": True, "default": "''"},
                "created_at": {"type": "TEXT", "not_null": True},
                "updated_at": {"type": "TEXT", "not_null": True},
                "started_at": {"type": "TEXT", "not_null": False},
                "finished_at": {"type": "TEXT", "not_null": False},
                "discovered_items": {"type": "INTEGER", "not_null": True, "default": "0"},
                "total_files": {"type": "INTEGER", "not_null": True, "default": "0"},
                "completed_files": {"type": "INTEGER", "not_null": True, "default": "0"},
                "failed_files": {"type": "INTEGER", "not_null": True, "default": "0"},
                "planning_errors": {"type": "INTEGER", "not_null": True, "default": "0"},
                "total_bytes": {"type": "INTEGER", "not_null": True, "default": "0"},
                "downloaded_bytes": {"type": "INTEGER", "not_null": True, "default": "0"},
                "current_file": {"type": "TEXT", "not_null": False},
                "error": {"type": "TEXT", "not_null": False},
            },
            "checks": (),
            "unique_constraints": (("idempotency_key",),),
        },
        "search_pages": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": True, "primary_key": 1},
                "page_number": {"type": "INTEGER", "not_null": True, "primary_key": 2},
                "incoming_cursor": {"type": "TEXT", "not_null": False},
                "outgoing_cursor": {"type": "TEXT", "not_null": False},
                "item_count": {"type": "INTEGER", "not_null": True},
                "manifest_path": {"type": "TEXT", "not_null": True},
                "checksum_sha256": {"type": "TEXT", "not_null": True},
                "created_at": {"type": "TEXT", "not_null": True},
                "planned_at": {"type": "TEXT", "not_null": False},
            },
            "checks": (),
            "foreign_keys": (("acquisition_runs", (("run_id", "run_id"),)),),
        },
        "discovered_items": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": True, "primary_key": 1},
                "catalog": {"type": "TEXT", "not_null": True, "primary_key": 2},
                "collection_id": {"type": "TEXT", "not_null": True, "primary_key": 3},
                "source_item_id": {"type": "TEXT", "not_null": True, "primary_key": 4},
                "page_number": {"type": "INTEGER", "not_null": True},
                "item_json": {"type": "TEXT", "not_null": True},
            },
            "checks": (),
            "foreign_keys": (("acquisition_runs", (("run_id", "run_id"),)),),
        },
        "download_batches": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": True, "primary_key": 1},
                "batch_number": {"type": "INTEGER", "not_null": True, "primary_key": 2},
                "status": {"type": "TEXT", "not_null": True},
                "asset_count": {"type": "INTEGER", "not_null": True},
                "completed_count": {"type": "INTEGER", "not_null": True, "default": "0"},
                "failed_count": {"type": "INTEGER", "not_null": True, "default": "0"},
                "created_at": {"type": "TEXT", "not_null": True},
                "updated_at": {"type": "TEXT", "not_null": True},
            },
            "checks": (),
            "foreign_keys": (("acquisition_runs", (("run_id", "run_id"),)),),
        },
        "download_attempts": {
            "columns": {
                "attempt_id": {"type": "TEXT", "not_null": False, "primary_key": 1},
                "run_id": {"type": "TEXT", "not_null": True},
                "batch_number": {"type": "INTEGER", "not_null": True},
                "catalog": {"type": "TEXT", "not_null": True},
                "collection_id": {"type": "TEXT", "not_null": True},
                "source_item_id": {"type": "TEXT", "not_null": True},
                "asset_key": {"type": "TEXT", "not_null": True},
                "source_url": {"type": "TEXT", "not_null": True},
                "destination": {"type": "TEXT", "not_null": True},
                "status": {"type": "TEXT", "not_null": True},
                "attempts": {"type": "INTEGER", "not_null": True, "default": "0"},
                "expected_bytes": {"type": "INTEGER", "not_null": True, "default": "0"},
                "downloaded_bytes": {"type": "INTEGER", "not_null": True, "default": "0"},
                "etag": {"type": "TEXT", "not_null": False},
                "error": {"type": "TEXT", "not_null": False},
                "updated_at": {"type": "TEXT", "not_null": True},
            },
            "checks": (),
            "unique_constraints": (
                ("run_id", "catalog", "collection_id", "source_item_id", "asset_key"),
            ),
            "foreign_keys": (
                ("download_batches", (("run_id", "run_id"), ("batch_number", "batch_number"))),
            ),
        },
        "planning_errors": {
            "columns": {
                "run_id": {"type": "TEXT", "not_null": True, "primary_key": 1},
                "catalog": {"type": "TEXT", "not_null": True, "primary_key": 2},
                "collection_id": {"type": "TEXT", "not_null": True, "primary_key": 3},
                "source_item_id": {"type": "TEXT", "not_null": True, "primary_key": 4},
                "selector": {"type": "TEXT", "not_null": True, "primary_key": 5},
                "message": {"type": "TEXT", "not_null": True},
                "created_at": {"type": "TEXT", "not_null": True},
            },
            "checks": (),
            "foreign_keys": (("acquisition_runs", (("run_id", "run_id"),)),),
        },
    }
    LEGACY_CATALOG = "__legacy__"
    LEGACY_COLLECTION = "__ambiguous__"

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.registry = self.root / "registry"
        self.registry.mkdir(parents=True, exist_ok=True)
        self.path = self.registry / "acquisition_state.sqlite"
        self._lock = threading.RLock()
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        last_error = None
        for attempt in range(6):
            try:
                connection = sqlite3.connect(self.path, timeout=30)
                break
            except sqlite3.OperationalError as exc:
                last_error = exc
                if attempt == 5:
                    raise
                time.sleep(0.25 * (attempt + 1))
        else:
            raise last_error or sqlite3.OperationalError("unable to open acquisition database")
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 120000")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @contextmanager
    def transaction(self):
        connection = self.connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.transaction() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS acquisition_runs (
                    run_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    request_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    discovered_items INTEGER NOT NULL DEFAULT 0,
                    total_files INTEGER NOT NULL DEFAULT 0,
                    completed_files INTEGER NOT NULL DEFAULT 0,
                    failed_files INTEGER NOT NULL DEFAULT 0,
                    planning_errors INTEGER NOT NULL DEFAULT 0,
                    total_bytes INTEGER NOT NULL DEFAULT 0,
                    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                    current_file TEXT,
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS acquisition_runs_created_idx
                    ON acquisition_runs(created_at DESC, run_id DESC);
                CREATE TABLE IF NOT EXISTS search_pages (
                    run_id TEXT NOT NULL,
                    page_number INTEGER NOT NULL,
                    incoming_cursor TEXT,
                    outgoing_cursor TEXT,
                    item_count INTEGER NOT NULL,
                    manifest_path TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    planned_at TEXT,
                    PRIMARY KEY (run_id, page_number),
                    FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS discovered_items (
                    run_id TEXT NOT NULL,
                    catalog TEXT NOT NULL,
                    collection_id TEXT NOT NULL,
                    source_item_id TEXT NOT NULL,
                    page_number INTEGER NOT NULL,
                    item_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, catalog, collection_id, source_item_id),
                    FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS download_batches (
                    run_id TEXT NOT NULL,
                    batch_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    asset_count INTEGER NOT NULL,
                    completed_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, batch_number),
                    FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS download_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    batch_number INTEGER NOT NULL,
                    catalog TEXT NOT NULL,
                    collection_id TEXT NOT NULL,
                    source_item_id TEXT NOT NULL,
                    asset_key TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    expected_bytes INTEGER NOT NULL DEFAULT 0,
                    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                    etag TEXT,
                    error TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),
                    FOREIGN KEY (run_id, batch_number) REFERENCES download_batches(run_id, batch_number)
                );
                CREATE TABLE IF NOT EXISTS planning_errors (
                    run_id TEXT NOT NULL,
                    catalog TEXT NOT NULL,
                    collection_id TEXT NOT NULL,
                    source_item_id TEXT NOT NULL,
                    selector TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, catalog, collection_id, source_item_id, selector),
                    FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)
                );
                """
            )
            run_columns = self._table_columns(db, "acquisition_runs")
            if "planning_errors" not in run_columns:
                db.execute("ALTER TABLE acquisition_runs ADD COLUMN planning_errors INTEGER NOT NULL DEFAULT 0")
            search_page_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(search_pages)").fetchall()
            }
            if "planned_at" not in search_page_columns:
                db.execute("ALTER TABLE search_pages ADD COLUMN planned_at TEXT")
                db.execute(
                    """
                    UPDATE search_pages
                    SET planned_at = created_at
                    WHERE run_id IN (
                        SELECT run_id FROM acquisition_runs WHERE total_files > 0
                    )
                    """
                )
            self._migrate_download_attempts(db)

    @staticmethod
    def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
        return {str(row["name"]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()}

    @staticmethod
    def _has_download_identity_unique(db: sqlite3.Connection) -> bool:
        expected = ("run_id", "catalog", "collection_id", "source_item_id", "asset_key")
        for index in db.execute("PRAGMA index_list(download_attempts)").fetchall():
            if not index["unique"]:
                continue
            if "partial" in index.keys() and index["partial"]:
                continue
            columns = tuple(
                str(row["name"])
                for row in sorted(
                    db.execute(f"PRAGMA index_info({index['name']})").fetchall(),
                    key=lambda row: int(row["seqno"]),
                )
            )
            if columns == expected:
                return True
        return False

    def _migrate_download_attempts(self, db: sqlite3.Connection) -> None:
        """Upgrade the pre-identity attempt table without guessing legacy rows."""

        columns = self._table_columns(db, "download_attempts")
        if not columns:
            db.execute(f"PRAGMA user_version = {self.IDENTITY_SCHEMA_VERSION}")
            return
        if {"catalog", "collection_id"}.issubset(columns) and self._has_download_identity_unique(db):
            db.execute(f"PRAGMA user_version = {self.IDENTITY_SCHEMA_VERSION}")
            return

        legacy_table = "download_attempts_legacy_v1"
        if db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (legacy_table,)
        ).fetchone():
            suffix = 2
            while db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"{legacy_table}_{suffix}",),
            ).fetchone():
                suffix += 1
            legacy_table = f"{legacy_table}_{suffix}"
        db.execute(f"ALTER TABLE download_attempts RENAME TO {legacy_table}")
        db.execute(
            """
            CREATE TABLE download_attempts (
                attempt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                batch_number INTEGER NOT NULL,
                catalog TEXT NOT NULL,
                collection_id TEXT NOT NULL,
                source_item_id TEXT NOT NULL,
                asset_key TEXT NOT NULL,
                source_url TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                expected_bytes INTEGER NOT NULL DEFAULT 0,
                downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                error TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),
                FOREIGN KEY (run_id, batch_number) REFERENCES download_batches(run_id, batch_number)
            )
            """
        )
        legacy_rows = db.execute(f"SELECT rowid, * FROM {legacy_table}").fetchall()
        for row in legacy_rows:
            row_keys = set(row.keys())

            def value(name: str, default: Any = None) -> Any:
                return row[name] if name in row_keys else default

            candidates = self._legacy_identity_candidates(db, row["run_id"], row["source_item_id"])
            if len(candidates) == 1:
                catalog, collection = candidates[0]
                identity = AssetIdentity(catalog, collection, row["source_item_id"], row["asset_key"])
                attempt_id = identity.attempt_digest(row["run_id"])
                status = value("status", "queued")
                error = value("error")
            else:
                # Keep the row for inspection, but make it impossible for the
                # downloader to associate it with an arbitrary Collection.
                catalog = self.LEGACY_CATALOG
                collection = self.LEGACY_COLLECTION
                attempt_id = hashlib.sha256(
                    f"legacy\0{row['run_id']}\0{row['rowid']}".encode("utf-8")
                ).hexdigest()
                status = "failed"
                error = (
                    "Legacy download attempt has ambiguous Collection identity; "
                    "re-plan this run instead of guessing."
                )
            destination = value("destination", "")
            if not isinstance(destination, str) or not destination.strip() or destination.strip() in {".", "./"}:
                reconstructed = self._reconstruct_legacy_destination(
                    db,
                    row["run_id"],
                    catalog,
                    collection,
                    row["source_item_id"],
                    row["asset_key"],
                )
                if reconstructed:
                    destination = reconstructed
                else:
                    destination = f"__legacy_ambiguous__/{attempt_id}"
                    status = "failed"
                    error = (
                        error
                        or "Legacy download attempt has no safe destination and its Asset metadata "
                        "cannot be reconstructed; re-plan this run."
                    )
            db.execute(
                """
                INSERT INTO download_attempts(
                    attempt_id,run_id,batch_number,catalog,collection_id,source_item_id,
                    asset_key,source_url,destination,status,attempts,expected_bytes,
                    downloaded_bytes,etag,error,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    row["run_id"],
                    row["batch_number"],
                    catalog,
                    collection,
                    row["source_item_id"],
                    row["asset_key"],
                    value("source_url", ""),
                    destination,
                    status,
                    value("attempts", 0),
                    value("expected_bytes", 0),
                    value("downloaded_bytes", 0),
                    value("etag"),
                    error,
                    value("updated_at", now()),
                ),
            )
        db.execute(f"PRAGMA user_version = {self.IDENTITY_SCHEMA_VERSION}")

    def _reconstruct_legacy_destination(
        self,
        db: sqlite3.Connection,
        run_id: str,
        catalog: str,
        collection: str,
        item_id: str,
        asset_key: str,
    ) -> str | None:
        row = db.execute(
            """
            SELECT item_json FROM discovered_items
            WHERE run_id=? AND catalog=? AND collection_id=? AND source_item_id=?
            """,
            (run_id, catalog, collection, item_id),
        ).fetchone()
        if not row:
            return None
        try:
            item = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return None
        asset = (item.get("assets") or {}).get(asset_key) if isinstance(item, dict) else None
        if not isinstance(asset, dict) or not isinstance(item, dict):
            return None
        try:
            identity = AssetIdentity.from_item(catalog, item, asset_key)
            return str(canonical_asset_path(self.root, identity, asset))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _legacy_identity_candidates(
        db: sqlite3.Connection, run_id: str, source_item_id: str
    ) -> list[tuple[str, str]]:
        rows = db.execute(
            """
            SELECT DISTINCT catalog, collection_id
            FROM discovered_items
            WHERE run_id=? AND source_item_id=?
            """,
            (run_id, source_item_id),
        ).fetchall()
        if rows:
            return [(str(row["catalog"]), str(row["collection_id"])) for row in rows]
        run = db.execute(
            "SELECT request_json FROM acquisition_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if not run:
            return []
        try:
            request = json.loads(run["request_json"])
        except (TypeError, json.JSONDecodeError):
            return []
        collections = request.get("collections") or []
        catalog = request.get("catalog")
        if catalog and len(collections) == 1 and collections[0]:
            return [(str(catalog), str(collections[0]))]
        return []

    def create_run(self, run_id: str, key: str, request: dict[str, Any]) -> tuple[str, bool]:
        timestamp = now()
        with self._lock, self.transaction() as db:
            existing = db.execute(
                "SELECT run_id, request_json FROM acquisition_runs WHERE idempotency_key = ?", (key,)
            ).fetchone()
            payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
            if existing:
                if existing["request_json"] != payload:
                    raise ValueError("idempotency key was already used for a different request")
                return str(existing["run_id"]), False
            db.execute(
                "INSERT INTO acquisition_runs(run_id,idempotency_key,request_json,status,message,created_at,updated_at) VALUES(?,?,?,'queued','Run queued.',?,?)",
                (run_id, key, payload, timestamp, timestamp),
            )
        return run_id, True

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.transaction() as db:
            row = db.execute("SELECT * FROM acquisition_runs WHERE run_id = ?", (run_id,)).fetchone()
        return self._run_dict(row) if row else None

    def run_status(self, run_id: str) -> str:
        with self.transaction() as db:
            row = db.execute(
                "SELECT status FROM acquisition_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return str(row[0])

    def list_runs(self, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        limit = max(1, min(limit, 200))
        params: list[Any] = []
        where = ""
        if cursor:
            created_at, run_id = cursor.split("|", 1)
            where = "WHERE (created_at < ? OR (created_at = ? AND run_id < ?))"
            params.extend((created_at, created_at, run_id))
        with self.transaction() as db:
            rows = db.execute(
                f"SELECT * FROM acquisition_runs {where} ORDER BY created_at DESC, run_id DESC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = f"{rows[-1]['created_at']}|{rows[-1]['run_id']}" if more and rows else None
        return {"items": [self._run_dict(row) for row in rows], "next_cursor": next_cursor}

    def update_run(self, run_id: str, **values: Any) -> None:
        if not values:
            return
        values["updated_at"] = now()
        assignments = ", ".join(f"{key} = ?" for key in values)
        with self._lock, self.transaction() as db:
            cursor = db.execute(
                f"UPDATE acquisition_runs SET {assignments} WHERE run_id = ?", (*values.values(), run_id)
            )
            if cursor.rowcount != 1:
                raise KeyError(run_id)

    def transition_run(self, run_id: str, expected: set[str], **values: Any) -> bool:
        if not expected:
            return False
        values["updated_at"] = now()
        assignments = ", ".join(f"{key} = ?" for key in values)
        placeholders = ",".join("?" for _ in expected)
        with self._lock, self.transaction() as db:
            cursor = db.execute(
                f"UPDATE acquisition_runs SET {assignments} "
                f"WHERE run_id = ? AND status IN ({placeholders})",
                (*values.values(), run_id, *sorted(expected)),
            )
        return cursor.rowcount == 1

    def increment_run(self, run_id: str, field: str, amount: int) -> None:
        if field not in {"completed_files", "failed_files", "downloaded_bytes", "total_bytes"}:
            raise ValueError(f"cannot increment {field}")
        with self._lock, self.transaction() as db:
            db.execute(
                f"UPDATE acquisition_runs SET {field}={field}+?, updated_at=? WHERE run_id=?",
                (amount, now(), run_id),
            )

    def recompute_run_counters(self, run_id: str) -> None:
        """Rebuild counters from durable attempt state after recovery."""

        with self._lock, self.transaction() as db:
            row = db.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status IN ('completed','skipped') THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(downloaded_bytes), 0)
                FROM download_attempts WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            db.execute(
                """
                UPDATE acquisition_runs
                SET completed_files=?, failed_files=?, updated_at=?
                WHERE run_id=?
                """,
                (int(row[0]), int(row[1]), now(), run_id),
            )

    def attempts_for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute(
                "SELECT * FROM download_attempts WHERE run_id=? ORDER BY batch_number, attempt_id",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def is_planning_complete(self, run_id: str, *, require_no_errors: bool = False) -> bool:
        """Return whether durable discovery and page planning reached a terminal state."""

        with self._lock, self.transaction() as db:
            run = db.execute(
                "SELECT planning_errors FROM acquisition_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            latest_page = db.execute(
                """
                SELECT planned_at, outgoing_cursor
                FROM search_pages
                WHERE run_id=?
                ORDER BY page_number DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            unplanned_pages = db.execute(
                """
                SELECT COUNT(*) FROM search_pages
                WHERE run_id=? AND planned_at IS NULL
                """,
                (run_id,),
            ).fetchone()[0]

        complete = bool(
            latest_page
            and latest_page["planned_at"] is not None
            and latest_page["outgoing_cursor"] is None
            and int(unplanned_pages) == 0
        )
        if require_no_errors:
            return complete and int(run["planning_errors"] or 0) == 0
        return complete

    def can_finalize_without_transfer(self, run_id: str) -> bool:
        """Return whether a run can retry finalization without HTTP transfer."""

        with self.transaction() as db:
            run = db.execute(
                """
                SELECT total_files, planning_errors
                FROM acquisition_runs WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            attempts = db.execute(
                """
                SELECT COUNT(*) AS total,
                       COALESCE(SUM(CASE WHEN status IN ('completed','skipped') THEN 1 ELSE 0 END), 0) AS done
                FROM download_attempts WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
        total_attempts, completed_attempts = int(attempts[0]), int(attempts[1])
        if not self.is_planning_complete(run_id, require_no_errors=True):
            return False
        if total_attempts:
            return total_attempts == completed_attempts
        return (
            int(run[0]) == 0
            and int(run[1]) == 0
        )

    def record_planning_error(
        self,
        run_id: str,
        catalog: str,
        collection_id: str,
        source_item_id: str,
        selector: str,
        message: str,
    ) -> None:
        """Persist unresolved Asset planning without manufacturing a download attempt."""

        with self._lock, self.transaction() as db:
            db.execute(
                """
                INSERT OR IGNORE INTO planning_errors(
                    run_id,catalog,collection_id,source_item_id,selector,message,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (run_id, catalog, collection_id, source_item_id, selector, message, now()),
            )
            count = db.execute(
                "SELECT COUNT(*) FROM planning_errors WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            db.execute(
                "UPDATE acquisition_runs SET planning_errors=?, error=?, updated_at=? WHERE run_id=?",
                (count, message, now(), run_id),
            )

    def planning_errors_for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute(
                "SELECT * FROM planning_errors WHERE run_id=? ORDER BY created_at, source_item_id, selector",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def commit_page(
        self, run_id: str, page_number: int, incoming: str | None, outgoing: str | None,
        manifest_path: str, checksum: str, items: list[dict[str, Any]], catalog: str,
    ) -> None:
        with self._lock, self.transaction() as db:
            db.execute(
                """
                INSERT OR IGNORE INTO search_pages(
                    run_id,page_number,incoming_cursor,outgoing_cursor,item_count,
                    manifest_path,checksum_sha256,created_at,planned_at
                ) VALUES(?,?,?,?,?,?,?,?,NULL)
                """,
                (run_id, page_number, incoming, outgoing, len(items), manifest_path, checksum, now()),
            )
            for item in items:
                if not catalog or not item.get("collection") or not item.get("id"):
                    raise ValueError("STAC Item requires catalog, collection, and id before persistence")
                db.execute(
                    "INSERT OR IGNORE INTO discovered_items VALUES(?,?,?,?,?,?)",
                    (run_id, catalog, str(item.get("collection") or "unknown"), str(item.get("id") or "unknown"), page_number, json.dumps(item, separators=(",", ":"))),
                )
            count = db.execute("SELECT COUNT(*) FROM discovered_items WHERE run_id=?", (run_id,)).fetchone()[0]
            db.execute("UPDATE acquisition_runs SET discovered_items=?, updated_at=? WHERE run_id=?", (count, now(), run_id))

    def discovered(self, run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute("SELECT item_json FROM discovered_items WHERE run_id=? ORDER BY page_number, source_item_id", (run_id,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def page_items(self, run_id: str, page_number: int) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute(
                """
                SELECT item_json FROM discovered_items
                WHERE run_id=? AND page_number=?
                ORDER BY source_item_id
                """,
                (run_id, page_number),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def unplanned_pages(self, run_id: str) -> list[int]:
        with self.transaction() as db:
            rows = db.execute(
                """
                SELECT page_number FROM search_pages
                WHERE run_id=? AND planned_at IS NULL
                ORDER BY page_number
                """,
                (run_id,),
            ).fetchall()
        return [int(row[0]) for row in rows]

    def search_checkpoint(self, run_id: str) -> tuple[int, str | None, bool]:
        with self.transaction() as db:
            row = db.execute(
                "SELECT page_number, outgoing_cursor FROM search_pages WHERE run_id=? ORDER BY page_number DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        if not row:
            return 1, None, False
        return int(row["page_number"]) + 1, row["outgoing_cursor"], row["outgoing_cursor"] is None

    @staticmethod
    def _job_identity(
        db: sqlite3.Connection, run_id: str, job: dict[str, Any]
    ) -> AssetIdentity:
        catalog = job.get("catalog")
        collection = job.get("collection") or job.get("collection_id")
        if not catalog or not collection:
            row = db.execute(
                "SELECT request_json FROM acquisition_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row:
                request = json.loads(row["request_json"])
                catalog = catalog or request.get("catalog")
                collections = request.get("collections") or []
                if not collection and len(collections) == 1:
                    collection = collections[0]
        return AssetIdentity(
            str(catalog or ""),
            str(collection or ""),
            str(job.get("source_item_id") or ""),
            str(job.get("asset_key") or ""),
        )

    def replace_plan(self, run_id: str, batches: list[list[dict[str, Any]]]) -> None:
        timestamp = now()
        with self._lock, self.transaction() as db:
            existing = db.execute("SELECT COUNT(*) FROM download_attempts WHERE run_id=?", (run_id,)).fetchone()[0]
            if existing:
                return
            for number, jobs in enumerate(batches, 1):
                db.execute("INSERT INTO download_batches VALUES(?,?,'queued',?,0,0,?,?)", (run_id, number, len(jobs), timestamp, timestamp))
                for job in jobs:
                    asset_identity = self._job_identity(db, run_id, job)
                    identity = asset_identity.attempt_digest(run_id)
                    db.execute(
                        """
                        INSERT INTO download_attempts(
                            attempt_id,run_id,batch_number,catalog,collection_id,source_item_id,
                            asset_key,source_url,destination,status,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,'queued',?)
                        """,
                        (
                            identity,
                            run_id,
                            number,
                            asset_identity.catalog,
                            asset_identity.collection,
                            asset_identity.item_id,
                            asset_identity.asset_key,
                            job["source_url"],
                            job["destination"],
                            timestamp,
                        ),
                    )
            total = sum(len(batch) for batch in batches)
            db.execute("UPDATE acquisition_runs SET total_files=?, updated_at=? WHERE run_id=?", (total, timestamp, run_id))

    def append_page_plan(
        self,
        run_id: str,
        page_number: int,
        batches: list[list[dict[str, Any]]],
    ) -> None:
        """Atomically append one committed Search Page plan and mark it planned."""
        timestamp = now()
        with self._lock, self.transaction() as db:
            page = db.execute(
                "SELECT planned_at FROM search_pages WHERE run_id=? AND page_number=?",
                (run_id, page_number),
            ).fetchone()
            if page is None:
                raise KeyError((run_id, page_number))
            if page["planned_at"] is not None:
                return
            next_batch = db.execute(
                "SELECT COALESCE(MAX(batch_number), 0) + 1 FROM download_batches WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
            for offset, jobs in enumerate(batches):
                batch_number = int(next_batch) + offset
                db.execute(
                    """
                    INSERT INTO download_batches(
                        run_id,batch_number,status,asset_count,completed_count,
                        failed_count,created_at,updated_at
                    ) VALUES(?,?,'queued',?,0,0,?,?)
                    """,
                    (run_id, batch_number, len(jobs), timestamp, timestamp),
                )
                for job in jobs:
                    asset_identity = self._job_identity(db, run_id, job)
                    identity = asset_identity.attempt_digest(run_id)
                    db.execute(
                        """
                        INSERT OR IGNORE INTO download_attempts(
                            attempt_id,run_id,batch_number,catalog,collection_id,source_item_id,asset_key,
                            source_url,destination,status,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,'queued',?)
                        """,
                        (
                            identity,
                            run_id,
                            batch_number,
                            asset_identity.catalog,
                            asset_identity.collection,
                            asset_identity.item_id,
                            asset_identity.asset_key,
                            job["source_url"],
                            job["destination"],
                            timestamp,
                        ),
                    )
            db.execute(
                "UPDATE search_pages SET planned_at=? WHERE run_id=? AND page_number=?",
                (timestamp, run_id, page_number),
            )
            total = db.execute(
                "SELECT COUNT(*) FROM download_attempts WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            db.execute(
                "UPDATE acquisition_runs SET total_files=?, updated_at=? WHERE run_id=?",
                (total, timestamp, run_id),
            )

    def list_batches(self, run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute("SELECT * FROM download_batches WHERE run_id=? ORDER BY batch_number", (run_id,)).fetchall()
        return [dict(row) for row in rows]

    def pending_batch_count(self, run_id: str) -> int:
        with self.transaction() as db:
            return int(
                db.execute(
                    """
                    SELECT COUNT(*) FROM download_batches
                    WHERE run_id=? AND status IN ('queued','downloading')
                    """,
                    (run_id,),
                ).fetchone()[0]
            )

    def next_pending_batch(self, run_id: str) -> dict[str, Any] | None:
        with self.transaction() as db:
            row = db.execute(
                """
                SELECT * FROM download_batches
                WHERE run_id=? AND status IN ('queued','downloading')
                ORDER BY batch_number LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return dict(row) if row else None

    def queued_run_ids(self, *, limit: int = 100) -> list[str]:
        with self.transaction() as db:
            rows = db.execute(
                """
                SELECT run_id FROM acquisition_runs
                WHERE status='queued'
                ORDER BY created_at, run_id
                LIMIT ?
                """,
                (max(1, limit),),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def attempts_for_batch(self, run_id: str, batch_number: int) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute(
                "SELECT * FROM download_attempts WHERE run_id=? AND batch_number=? ORDER BY attempt_id",
                (run_id, batch_number),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_attempt(self, attempt_id: str, **values: Any) -> None:
        values["updated_at"] = now()
        assignments = ", ".join(f"{key}=?" for key in values)
        with self._lock, self.transaction() as db:
            db.execute(f"UPDATE download_attempts SET {assignments} WHERE attempt_id=?", (*values.values(), attempt_id))

    def update_batch(self, run_id: str, batch_number: int, **values: Any) -> None:
        values["updated_at"] = now()
        assignments = ", ".join(f"{key}=?" for key in values)
        with self._lock, self.transaction() as db:
            db.execute(f"UPDATE download_batches SET {assignments} WHERE run_id=? AND batch_number=?", (*values.values(), run_id, batch_number))

    def item(
        self,
        run_id: str,
        catalog: str,
        collection_id: str | None = None,
        source_item_id: str | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as db:
            if source_item_id is None:
                # Compatibility for callers of the old two-argument helper.
                # It remains fail-closed when the old key is ambiguous.
                legacy_rows = db.execute(
                    """
                    SELECT item_json FROM discovered_items
                    WHERE run_id=? AND source_item_id=?
                    """,
                    (run_id, catalog),
                ).fetchall()
                if len(legacy_rows) != 1:
                    raise KeyError((run_id, catalog, "ambiguous legacy item identity"))
                return json.loads(legacy_rows[0][0])
            row = db.execute(
                """
                SELECT item_json FROM discovered_items
                WHERE run_id=? AND catalog=? AND collection_id=? AND source_item_id=?
                """,
                (run_id, catalog, collection_id, source_item_id),
            ).fetchone()
        if not row:
            raise KeyError((run_id, catalog, collection_id, source_item_id))
        return json.loads(row[0])

    def recover_interrupted(self) -> list[str]:
        with self._lock, self.transaction() as db:
            rows = db.execute(
                "SELECT run_id FROM acquisition_runs WHERE status IN ('queued','recovering','discovering','planning','downloading','finalizing','cancelling')"
            ).fetchall()
            ids = [str(row[0]) for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                db.execute(
                    f"UPDATE acquisition_runs SET status='queued', message='Recovered after process restart.', updated_at=? WHERE run_id IN ({placeholders})",
                    (now(), *ids),
                )
                db.execute(
                    f"UPDATE download_attempts SET status='queued', updated_at=? WHERE run_id IN ({placeholders}) AND status='downloading'",
                    (now(), *ids),
                )
        return ids

    def retry_failed(self, run_id: str) -> int:
        with self._lock, self.transaction() as db:
            count = db.execute(
                """
                SELECT COUNT(*) FROM download_attempts
                WHERE run_id=? AND status IN ('failed', 'downloading', 'queued')
                """,
                (run_id,),
            ).fetchone()[0]
            if not count:
                return 0
            db.execute(
                """
                UPDATE download_attempts
                SET status='queued', error=NULL, updated_at=?
                WHERE run_id=? AND status IN ('failed', 'downloading', 'queued')
                """,
                (now(), run_id),
            )
            db.execute(
                """
                UPDATE download_batches
                SET status='queued', failed_count=0, updated_at=?
                WHERE run_id=? AND status IN ('failed', 'partial', 'downloading')
                """,
                (now(), run_id),
            )
            db.execute(
                "UPDATE acquisition_runs SET status='queued', failed_files=0, error=NULL, message='Failed transfers queued for retry.', finished_at=NULL, updated_at=? WHERE run_id=?",
                (now(), run_id),
            )
        return int(count)

    @staticmethod
    def _run_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["request"] = json.loads(result.pop("request_json"))
        total = result["total_files"]
        done = result["completed_files"] + result["failed_files"]
        result["progress"] = round(done / total * 100, 2) if total else (100.0 if result["status"] == "completed" else 0.0)
        return result
