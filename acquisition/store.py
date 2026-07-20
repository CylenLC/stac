import json
import sqlite3
import threading
import hashlib
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def now() -> str:
    return datetime.now(UTC).isoformat()


class AcquisitionStore:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.registry = self.root / "registry"
        self.registry.mkdir(parents=True, exist_ok=True)
        self.path = self.registry / "acquisition_state.sqlite"
        self._lock = threading.RLock()
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
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
                    UNIQUE (run_id, source_item_id, asset_key),
                    FOREIGN KEY (run_id, batch_number) REFERENCES download_batches(run_id, batch_number)
                );
                """
            )

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

    def increment_run(self, run_id: str, field: str, amount: int) -> None:
        if field not in {"completed_files", "failed_files", "downloaded_bytes", "total_bytes"}:
            raise ValueError(f"cannot increment {field}")
        with self._lock, self.transaction() as db:
            db.execute(
                f"UPDATE acquisition_runs SET {field}={field}+?, updated_at=? WHERE run_id=?",
                (amount, now(), run_id),
            )

    def commit_page(
        self, run_id: str, page_number: int, incoming: str | None, outgoing: str | None,
        manifest_path: str, checksum: str, items: list[dict[str, Any]], catalog: str,
    ) -> None:
        with self._lock, self.transaction() as db:
            db.execute(
                "INSERT OR IGNORE INTO search_pages VALUES(?,?,?,?,?,?,?,?)",
                (run_id, page_number, incoming, outgoing, len(items), manifest_path, checksum, now()),
            )
            for item in items:
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

    def search_checkpoint(self, run_id: str) -> tuple[int, str | None, bool]:
        with self.transaction() as db:
            row = db.execute(
                "SELECT page_number, outgoing_cursor FROM search_pages WHERE run_id=? ORDER BY page_number DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        if not row:
            return 1, None, False
        return int(row["page_number"]) + 1, row["outgoing_cursor"], row["outgoing_cursor"] is None

    def replace_plan(self, run_id: str, batches: list[list[dict[str, Any]]]) -> None:
        timestamp = now()
        with self._lock, self.transaction() as db:
            existing = db.execute("SELECT COUNT(*) FROM download_attempts WHERE run_id=?", (run_id,)).fetchone()[0]
            if existing:
                return
            for number, jobs in enumerate(batches, 1):
                db.execute("INSERT INTO download_batches VALUES(?,?,'queued',?,0,0,?,?)", (run_id, number, len(jobs), timestamp, timestamp))
                for job in jobs:
                    identity = hashlib.sha256(f"{run_id}\0{job['source_item_id']}\0{job['asset_key']}".encode()).hexdigest()
                    db.execute(
                        "INSERT INTO download_attempts(attempt_id,run_id,batch_number,source_item_id,asset_key,source_url,destination,status,updated_at) VALUES(?,?,?,?,?,?,?,'queued',?)",
                        (identity, run_id, number, job["source_item_id"], job["asset_key"], job["source_url"], job["destination"], timestamp),
                    )
            total = sum(len(batch) for batch in batches)
            db.execute("UPDATE acquisition_runs SET total_files=?, updated_at=? WHERE run_id=?", (total, timestamp, run_id))

    def list_batches(self, run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as db:
            rows = db.execute("SELECT * FROM download_batches WHERE run_id=? ORDER BY batch_number", (run_id,)).fetchall()
        return [dict(row) for row in rows]

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

    def item(self, run_id: str, source_item_id: str) -> dict[str, Any]:
        with self.transaction() as db:
            row = db.execute(
                "SELECT item_json FROM discovered_items WHERE run_id=? AND source_item_id=? LIMIT 1",
                (run_id, source_item_id),
            ).fetchone()
        if not row:
            raise KeyError(source_item_id)
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
                "SELECT COUNT(*) FROM download_attempts WHERE run_id=? AND status='failed'", (run_id,)
            ).fetchone()[0]
            db.execute(
                "UPDATE download_attempts SET status='queued', error=NULL, updated_at=? WHERE run_id=? AND status='failed'",
                (now(), run_id),
            )
            db.execute(
                "UPDATE download_batches SET status='queued', failed_count=0, updated_at=? WHERE run_id=? AND status IN ('failed','partial')",
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
