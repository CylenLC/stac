import json
import os
import uuid
import gzip
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import AcquisitionRequest, TERMINAL_STATUSES
from .store import AcquisitionStore
from earth_lake import EarthLake
import requests
from stac_core import asset_filename, download_asset, http_session, resolve_asset_url, search_items_page, selected_assets


class RunInterrupted(Exception):
    pass


class AuthenticationRequired(Exception):
    pass


class AcquisitionManager:
    """Public interface for durable Acquisition Runs."""

    def __init__(self, root: str | Path = "downloads", page_fetcher=None, transfer=None):
        self.root = Path(root).resolve()
        self.store = AcquisitionStore(self.root)
        self.manifests = self.root / "manifests" / "acquisitions"
        self.manifests.mkdir(parents=True, exist_ok=True)
        self.page_fetcher = page_fetcher or self._fetch_page
        self.transfer = transfer or download_asset
        self._scheduler_stop = threading.Event()
        self._scheduler_thread: threading.Thread | None = None
        self._registry_lock = threading.RLock()

    def create_run(self, request: AcquisitionRequest, idempotency_key: str) -> str:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        run_id = str(uuid.uuid4())
        run_id, created = self.store.create_run(run_id, idempotency_key, request.to_dict())
        if created:
            directory = self.manifests / run_id
            directory.mkdir(parents=True, exist_ok=False)
            temporary = directory / "request.json.tmp"
            temporary.write_text(json.dumps(request.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temporary, directory / "request.json")
        return run_id

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def list_runs(self, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        return self.store.list_runs(cursor, limit)

    def pause_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            raise ValueError(f"cannot pause {run['status']} run")
        self.store.update_run(run_id, status="paused", message="Run paused by user.")
        return self.get_run(run_id)

    def resume_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"paused", "auth_required", "failed"}:
            raise ValueError(f"cannot resume {run['status']} run")
        self.store.update_run(run_id, status="queued", message="Run queued for resume.", error=None)
        return self.get_run(run_id)

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            if run["status"] == "cancelled":
                return run
            raise ValueError(f"cannot cancel {run['status']} run")
        self.store.update_run(run_id, status="cancelled", message="Run cancelled by user.")
        return self.get_run(run_id)

    def retry_failed(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"failed", "partial", "auth_required"}:
            raise ValueError(f"cannot retry {run['status']} run")
        if not self.store.retry_failed(run_id):
            raise ValueError("run has no failed transfers to retry")
        return self.get_run(run_id)

    @staticmethod
    def _fetch_page(request: AcquisitionRequest, cursor: str | None, page_size: int):
        return search_items_page(
            request.catalog, request.wkt, request.collections, request.start_date,
            request.end_date, cursor, page_size,
        )

    def discover_and_plan(self, run_id: str, page_size: int = 100) -> None:
        run = self.get_run(run_id)
        request = AcquisitionRequest(**run["request"])
        self.store.update_run(run_id, status="discovering", message="Discovering catalog pages.")
        page_number, cursor, discovery_complete = self.store.search_checkpoint(run_id)
        already_discovered = self.get_run(run_id)["discovered_items"]
        remaining = request.max_items - already_discovered if request.max_items is not None else None
        while not discovery_complete:
            self._check_control(run_id)
            limit = min(page_size, remaining) if remaining is not None else page_size
            if limit <= 0:
                break
            items, outgoing = self.page_fetcher(request, cursor, limit)
            if remaining is not None:
                items = items[:remaining]
                remaining -= len(items)
                if remaining <= 0:
                    outgoing = None
            self._commit_page(run_id, page_number, cursor, outgoing, items, request.catalog)
            self._check_control(run_id)
            if not outgoing:
                break
            cursor, page_number = outgoing, page_number + 1

        self.store.update_run(run_id, status="planning", message="Planning download batches.")
        jobs: list[dict[str, str]] = []
        lake = EarthLake(self.root)
        for item in self.store.discovered(run_id):
            for key, asset in selected_assets(item, request.only_main):
                url = resolve_asset_url(asset, request.catalog)
                if not url:
                    continue
                directory = lake.source_item_directory(request.catalog, item.get("collection"), item.get("id"))
                jobs.append({
                    "source_item_id": str(item.get("id")), "asset_key": key, "source_url": url,
                    "destination": str(directory / asset_filename(key, asset)),
                })
        batches = [jobs[index:index + request.batch_size] for index in range(0, len(jobs), request.batch_size)]
        self.store.replace_plan(run_id, batches)
        self.store.update_run(run_id, status="queued", message=f"Planned {len(jobs)} asset files.")

    def _commit_page(self, run_id: str, number: int, incoming: str | None, outgoing: str | None, items: list[dict[str, Any]], catalog: str) -> None:
        directory = self.manifests / run_id / "pages"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{number:06d}.jsonl.gz"
        temporary = path.with_suffix(path.suffix + ".tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as output:
            for item in items:
                output.write(json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n")
        checksum = hashlib.sha256(temporary.read_bytes()).hexdigest()
        os.replace(temporary, path)
        self.store.commit_page(run_id, number, incoming, outgoing, str(path.relative_to(self.root)), checksum, items, catalog)

    def list_batches(self, run_id: str) -> list[dict[str, Any]]:
        self.get_run(run_id)
        return self.store.list_batches(run_id)

    def run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES or run["status"] == "paused":
            return run
        try:
            discovery_complete = self.store.search_checkpoint(run_id)[2]
            if not run["total_files"] and not discovery_complete:
                self.discover_and_plan(run_id)
            run = self.get_run(run_id)
            if run["status"] in {"paused", "cancelled"}:
                return run
            self.store.update_run(run_id, status="downloading", message="Downloading planned batches.", started_at=run["started_at"] or datetime.now(UTC).isoformat())
            protocol_run_id = f"acq-{run_id}"
            with self._registry_lock:
                EarthLake(self.root).start_run({"interface": "acquisition", "acquisition_run_id": run_id}, run_id=protocol_run_id)
            for batch in self.list_batches(run_id):
                self._check_control(run_id)
                self._run_batch(run_id, batch)
            run = self.get_run(run_id)
            status = "partial" if run["failed_files"] and run["completed_files"] else "failed" if run["failed_files"] else "completed"
            self.store.update_run(
                run_id, status="finalizing", message="Finalizing protocol registries."
            )
            with self._registry_lock:
                lake = EarthLake(self.root)
                lake.finish_run(protocol_run_id, status, lake.output_asset_ids(protocol_run_id))
            self.store.update_run(
                run_id, status=status,
                message=f"Finished with {run['completed_files']} completed and {run['failed_files']} failed files.",
                finished_at=datetime.now(UTC).isoformat(), current_file=None,
            )
        except AuthenticationRequired as exc:
            self.store.update_run(run_id, status="auth_required", message="NASA Earthdata authentication required.", error=str(exc))
        except RunInterrupted:
            pass
        except Exception as exc:
            self.store.update_run(run_id, status="failed", message=f"Acquisition failed: {exc}", error=str(exc), finished_at=datetime.now(UTC).isoformat())
        return self.get_run(run_id)

    def _run_batch(self, run_id: str, batch: dict[str, Any]) -> None:
        attempts = [item for item in self.store.attempts_for_batch(run_id, batch["batch_number"]) if item["status"] not in {"completed", "skipped"}]
        if not attempts:
            return
        self.store.update_batch(run_id, batch["batch_number"], status="downloading")
        concurrency = self.get_run(run_id)["request"]["download_concurrency"]
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(self._transfer_attempt, run_id, attempt): attempt for attempt in attempts}
            for future in as_completed(futures):
                future.result()
        current = self.store.attempts_for_batch(run_id, batch["batch_number"])
        completed = sum(item["status"] in {"completed", "skipped"} for item in current)
        failed = sum(item["status"] == "failed" for item in current)
        self.store.update_batch(run_id, batch["batch_number"], status="partial" if failed and completed else "failed" if failed else "completed", completed_count=completed, failed_count=failed)

    def _transfer_attempt(self, run_id: str, attempt: dict[str, Any]) -> None:
        self._check_control(run_id)
        destination = Path(attempt["destination"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        item = self.store.item(run_id, attempt["source_item_id"])
        metadata = destination.parent / "metadata.json"
        if not metadata.exists():
            metadata.write_text(json.dumps(item, indent=2), encoding="utf-8")
        if destination.exists():
            self._complete_attempt(run_id, attempt, item, destination, "skipped")
            return
        session = http_session()
        for retry in range(1, 4):
            self.store.update_attempt(attempt["attempt_id"], status="downloading", attempts=retry, error=None)
            self.store.update_run(run_id, current_file=destination.name)

            def on_chunk(size: int) -> None:
                self._check_control(run_id)
                self.store.increment_run(run_id, "downloaded_bytes", size)

            try:
                self.transfer(session, attempt["source_url"], destination, on_chunk)
                self._complete_attempt(run_id, attempt, item, destination, "completed")
                return
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else None
                if code in {401, 403}:
                    raise AuthenticationRequired(str(exc)) from exc
                if code not in {429, 500, 502, 503, 504}:
                    break
            except RunInterrupted:
                self.store.update_attempt(attempt["attempt_id"], status="queued")
                raise
            except (requests.RequestException, OSError) as exc:
                error = exc
            if retry < 3:
                time.sleep(0.1 * 2 ** (retry - 1))
        message = str(locals().get("error", "download failed"))
        self.store.update_attempt(attempt["attempt_id"], status="failed", error=message)
        self.store.increment_run(run_id, "failed_files", 1)

    def _complete_attempt(self, run_id: str, attempt: dict[str, Any], item: dict[str, Any], destination: Path, status: str) -> None:
        request = self.get_run(run_id)["request"]
        with self._registry_lock:
            lake = EarthLake(self.root)
            lake.record_asset(
                run_id=f"acq-{run_id}", catalog=request["catalog"], item=item, asset_key=attempt["asset_key"],
                source_url=attempt["source_url"], local_path=destination, status=status,
            )
        self.store.update_attempt(attempt["attempt_id"], status=status, downloaded_bytes=destination.stat().st_size)
        self.store.increment_run(run_id, "completed_files", 1)

    def _check_control(self, run_id: str) -> None:
        status = self.get_run(run_id)["status"]
        if status == "paused":
            raise RunInterrupted()
        if status in {"cancelled", "cancelling"}:
            if status == "cancelling":
                self.store.update_run(run_id, status="cancelled", message="Run cancelled by user.")
            raise RunInterrupted()

    def recover_interrupted_runs(self) -> list[str]:
        return self.store.recover_interrupted()

    def start_scheduler(self) -> None:
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._scheduler_stop.clear()
        self.recover_interrupted_runs()

        def loop() -> None:
            while not self._scheduler_stop.is_set():
                queued = [run for run in self.list_runs(limit=200)["items"] if run["status"] == "queued"]
                for run in queued:
                    self.run(run["run_id"])
                self._scheduler_stop.wait(0.5)

        self._scheduler_thread = threading.Thread(target=loop, name="acquisition-scheduler", daemon=True)
        self._scheduler_thread.start()

    def stop_scheduler(self) -> None:
        self._scheduler_stop.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
