"""Crash-recoverable publication of Earth Lake protocol facts."""

from __future__ import annotations

import json
import os
import shutil
import uuid
import fcntl
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


class ProtocolCommit:
    """Stage related protocol files and roll forward an interrupted publication."""

    _process_lock = threading.RLock()
    _held_roots: set[Path] = set()

    def __init__(self, root: str | Path, *, kind: str, metadata: dict[str, Any] | None = None):
        self.root = Path(root).resolve()
        self.commit_id = str(uuid.uuid4())
        self.kind = kind
        self.metadata = metadata or {}
        self.commit_root = self.root / "manifests" / "protocol_commits"
        self.lock_path = self.commit_root / ".protocol.lock"
        self._lock_handle = None
        self.stage = self.commit_root / ".staging" / self.commit_id
        self.journal = self.commit_root / f"{self.commit_id}.json"
        self.payload: dict[str, Any] = {
            "commit_id": self.commit_id,
            "kind": kind,
            "status": "preparing",
            "created_at": _now(),
            "updated_at": _now(),
            "metadata": self.metadata,
            "outputs": [],
        }
        self._process_lock_acquired = False

    def __enter__(self) -> "ProtocolCommit":
        self._process_lock.acquire()
        self._process_lock_acquired = True
        try:
            self.commit_root.mkdir(parents=True, exist_ok=True)
            self._lock_handle = self.lock_path.open("a+b")
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX)
            self._held_roots.add(self.root)
            self.stage.mkdir(parents=True, exist_ok=False)
            _atomic_json(self.journal, self.payload)
            return self
        except Exception:
            self._release_lock()
            raise

    def __exit__(self, exc_type, exc, traceback) -> bool:
        try:
            if exc_type is None:
                self.publish()
            else:
                self.abort(str(exc))
        finally:
            self._release_lock()
        return False

    def _release_lock(self) -> None:
        if self._lock_handle is not None:
            self._held_roots.discard(self.root)
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            self._lock_handle.close()
            self._lock_handle = None
        if self._process_lock_acquired:
            self._process_lock_acquired = False
            self._process_lock.release()

    def __del__(self):
        # Tests and crash simulation may call __enter__ without __exit__.
        self._release_lock()

    def staged_path(self, live_path: str | Path) -> Path:
        live = Path(live_path).resolve()
        try:
            relative = live.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("Protocol commit outputs must be inside the Earth Lake root") from exc
        staged = self.stage / relative
        staged.parent.mkdir(parents=True, exist_ok=True)
        return staged

    def prepare_tree(self, live_path: str | Path) -> Path:
        live = Path(live_path).resolve()
        staged = self.staged_path(live)
        if staged.exists():
            return staged
        if live.is_dir():
            shutil.copytree(
                live,
                staged,
                ignore=shutil.ignore_patterns("._*", ".DS_Store"),
            )
        else:
            staged.mkdir(parents=True, exist_ok=True)
        return staged

    def publish(self) -> None:
        outputs = sorted(
            path.relative_to(self.stage).as_posix()
            for path in self.stage.rglob("*")
            if path.is_file()
        )
        self.payload.update(status="publishing", outputs=outputs, updated_at=_now())
        _atomic_json(self.journal, self.payload)
        self._roll_forward(self.root, self.stage, outputs)
        self.payload.update(status="committed", committed_at=_now(), updated_at=_now())
        _atomic_json(self.journal, self.payload)
        shutil.rmtree(self.stage, ignore_errors=True)

    def abort(self, error: str) -> None:
        self.payload.update(status="aborted", error=error, updated_at=_now())
        _atomic_json(self.journal, self.payload)
        shutil.rmtree(self.stage, ignore_errors=True)

    @classmethod
    def recover(cls, root: str | Path) -> list[str]:
        root = Path(root).resolve()
        journal_root = root / "manifests" / "protocol_commits"
        recovered: list[str] = []
        if not journal_root.exists():
            return recovered
        lock_path = journal_root / ".protocol.lock"
        with cls._process_lock, lock_path.open("a+b") as lock_handle:
            if root not in cls._held_roots:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            journals = sorted(journal_root.glob("*.json"))
            for journal in journals:
            # macOS may create binary AppleDouble sidecars on external volumes.
                if journal.name.startswith("._"):
                    continue
                try:
                    payload = json.loads(journal.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError):
                    continue
                status = payload.get("status")
                commit_id = payload.get("commit_id")
                if not isinstance(commit_id, str) or status not in {"preparing", "publishing"}:
                    continue
                stage = journal_root / ".staging" / commit_id
                legacy_stage = root / "cache" / "protocol-commits" / commit_id
                if not stage.exists() and legacy_stage.exists():
                    stage = legacy_stage
                if status == "publishing":
                    outputs = [value for value in payload.get("outputs", []) if isinstance(value, str)]
                    cls._roll_forward(root, stage, outputs)
                    payload.update(status="committed", recovered_at=_now(), updated_at=_now())
                    recovered.append(commit_id)
                else:
                    payload.update(status="aborted", error="Recovered incomplete preparation", updated_at=_now())
                _atomic_json(journal, payload)
                shutil.rmtree(stage, ignore_errors=True)
        return recovered

    @staticmethod
    def _roll_forward(root: Path, stage: Path, outputs: list[str]) -> None:
        for relative in outputs:
            source = stage / relative
            target = root / relative
            if not source.is_file() and target.is_file():
                continue
            if not source.is_file():
                raise FileNotFoundError(f"Missing staged protocol output: {relative}")
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, target)
