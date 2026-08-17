import json
import os
import sqlite3
import uuid
import gzip
import hashlib
import logging
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import ACTIVE_STATUSES, AcquisitionRequest, TERMINAL_STATUSES
from .store import AcquisitionStore
from earth_lake import EarthLake
import requests
from stac_core import (
    AssetResolutionError,
    canonical_asset_path,
    classify_asset,
    download_asset,
    download_asset_checked,
    http_session,
    resolve_asset_url,
    resolve_assets,
    search_items_page,
)
from stac_identity import AssetIdentity
from stac_integrity import IntegrityError, IntegrityGate, IntegrityResult


logger = logging.getLogger(__name__)


class RunInterrupted(Exception):
    pass


class AuthenticationRequired(Exception):
    pass


class AcquisitionManager:
    """Public interface for durable Acquisition Runs."""

    PROGRESS_FLUSH_BYTES = 4 * 1024 * 1024
    PROGRESS_FLUSH_SECONDS = 0.5

    def __init__(self, root: str | Path = "downloads", page_fetcher=None, transfer=None):
        self.root = Path(root).resolve()
        self.store = AcquisitionStore(self.root)
        self.manifests = self.root / "manifests" / "acquisitions"
        self.manifests.mkdir(parents=True, exist_ok=True)
        self.page_fetcher = page_fetcher or self._fetch_page
        self._uses_default_transfer = transfer is None
        self.transfer = transfer or download_asset
        self._scheduler_stop = threading.Event()
        self._scheduler_wake = threading.Event()
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
            self._scheduler_wake.set()
        return run_id

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def list_runs(self, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        result = self.store.list_runs(cursor, limit)
        for run in result["items"]:
            if run["status"] == "completed":
                self.reconcile_external_status(run["run_id"])
        return self.store.list_runs(cursor, limit)

    def reconcile_external_status(self, run_id: str) -> dict[str, Any]:
        """Resolve a terminal status without starting transfers.

        This is the single status path for API-facing completed Runs. Invalid
        terminal Assets are reported as failed instead of being queued, so a
        read request cannot unexpectedly initiate a download.
        """

        run = self.get_run(run_id)
        if run["status"] != "completed":
            return run
        try:
            with self._registry_lock:
                result = self._reconcile_completed_state(run_id, allow_asset_repair=False)
            return result or self.get_run(run_id)
        except Exception as exc:
            logger.exception("External status reconciliation failed for %s", run_id)
            self.store.transition_run(
                run_id,
                {"completed"},
                status="failed",
                message=f"External completion reconciliation failed: {exc}",
                error=str(exc),
                finished_at=datetime.now(UTC).isoformat(),
            )
            return self.get_run(run_id)

    def pause_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            raise ValueError(f"cannot pause {run['status']} run")
        if not self.store.transition_run(
            run_id,
            ACTIVE_STATUSES | {"auth_required"},
            status="paused",
            message="Run paused by user.",
        ):
            raise ValueError(f"cannot pause {self.get_run(run_id)['status']} run")
        return self.get_run(run_id)

    def resume_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"paused", "auth_required", "failed"}:
            raise ValueError(f"cannot resume {run['status']} run")
        if not self.store.transition_run(
            run_id,
            {"paused", "auth_required", "failed"},
            status="queued",
            message="Run queued for resume.",
            error=None,
            finished_at=None,
        ):
            raise ValueError(f"cannot resume {self.get_run(run_id)['status']} run")
        self._scheduler_wake.set()
        return self.get_run(run_id)

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] in TERMINAL_STATUSES:
            if run["status"] == "cancelled":
                return run
            raise ValueError(f"cannot cancel {run['status']} run")
        if not self.store.transition_run(
            run_id,
            ACTIVE_STATUSES | {"paused", "auth_required"},
            status="cancelled",
            message="Run cancelled by user.",
            finished_at=datetime.now(UTC).isoformat(),
        ):
            raise ValueError(f"cannot cancel {self.get_run(run_id)['status']} run")
        return self.get_run(run_id)

    def retry_failed(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in {"failed", "partial", "auth_required"}:
            raise ValueError(f"cannot retry {run['status']} run")
        if not self.store.retry_failed(run_id):
            if not self.store.can_finalize_without_transfer(run_id):
                raise ValueError("run has no unfinished transfers to retry")
            if not self.store.transition_run(
                run_id,
                {"failed", "partial", "auth_required"},
                status="queued",
                message="Run queued for finalization recovery.",
                error=None,
                finished_at=None,
            ):
                raise ValueError(f"cannot retry {self.get_run(run_id)['status']} run")
        self._scheduler_wake.set()
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
        self._transition_active(run_id, status="discovering", message="Discovering catalog pages.")
        self._plan_committed_pages(run_id, request)
        already_discovered = self.get_run(run_id)["discovered_items"]
        remaining = request.max_items - already_discovered if request.max_items is not None else None
        page_number, cursor, discovery_complete = self.store.search_checkpoint(run_id)
        while not discovery_complete:
            self._check_control(run_id)
            page_number, cursor, discovery_complete, item_count = self._discover_page(
                run_id,
                request,
                page_number,
                cursor,
                page_size,
                remaining,
            )
            if remaining is not None:
                remaining -= item_count
            self._check_control(run_id)
        self._transition_active(
            run_id,
            status="queued",
            message=self._planning_message(run_id),
        )

    def _planning_message(self, run_id: str) -> str:
        run = self.get_run(run_id)
        suffix = f"; {run['planning_errors']} Asset planning errors" if run.get("planning_errors") else ""
        return f"Planned {run['total_files']} asset files{suffix}."

    def _discover_page(
        self,
        run_id: str,
        request: AcquisitionRequest,
        page_number: int,
        cursor: str | None,
        page_size: int,
        remaining: int | None,
    ) -> tuple[int, str | None, bool, int]:
        limit = min(page_size, remaining) if remaining is not None else page_size
        if limit <= 0:
            return page_number, cursor, True, 0
        items, outgoing = self.page_fetcher(request, cursor, limit)
        if remaining is not None:
            items = items[:remaining]
            if len(items) >= remaining:
                outgoing = None
        self._commit_page(run_id, page_number, cursor, outgoing, items, request.catalog)
        self._plan_page(run_id, request, page_number, items)
        return page_number + 1, outgoing, outgoing is None, len(items)

    def _plan_committed_pages(self, run_id: str, request: AcquisitionRequest) -> None:
        for page_number in self.store.unplanned_pages(run_id):
            self._plan_page(run_id, request, page_number, self.store.page_items(run_id, page_number))

    def _plan_page(
        self,
        run_id: str,
        request: AcquisitionRequest,
        page_number: int,
        items: list[dict[str, Any]],
    ) -> None:
        self._transition_active(
            run_id,
            status="planning",
            message=f"Planning Search Page {page_number}.",
        )
        jobs: list[dict[str, str]] = []
        for item in items:
            selector = "explicit" if request.asset_keys is not None else "main" if request.only_main else "all"
            try:
                resolution = resolve_assets(
                    item,
                    request.catalog,
                    mode=selector,
                    asset_keys=request.asset_keys,
                )
                for key, asset in resolution.selected:
                    url = resolve_asset_url(asset, request.catalog)
                    if not isinstance(url, str) or not url.strip():
                        raise AssetResolutionError(
                            f"selected Asset {key!r} resolved to an empty URL",
                            decisions=resolution.decisions,
                        )
                    identity = AssetIdentity.from_item(request.catalog, item, key)
                    jobs.append({
                        "catalog": identity.catalog,
                        "collection": identity.collection,
                        "source_item_id": identity.item_id,
                        "asset_key": identity.asset_key,
                        "source_url": url,
                        "destination": str(canonical_asset_path(self.root, identity, asset)),
                    })
            except (AssetResolutionError, ValueError) as exc:
                catalog = str(request.catalog)
                collection = str(item.get("collection") or "__unknown__")
                item_id = str(item.get("id") or "__unknown__")
                self.store.record_planning_error(
                    run_id,
                    catalog,
                    collection,
                    item_id,
                    selector,
                    str(exc),
                )
                logger.warning("Asset planning failed: %s", exc)
        batches = [jobs[index:index + request.batch_size] for index in range(0, len(jobs), request.batch_size)]
        self.store.append_page_plan(run_id, page_number, batches)

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
        if run["status"] == "paused":
            return run
        if run["status"] == "completed":
            try:
                reconciled = self._reconcile_completed_state(run_id, allow_asset_repair=True)
                if reconciled is not None:
                    return reconciled
            except Exception as exc:
                logger.exception("Completed acquisition run %s failed reconciliation", run_id)
                self.store.transition_run(
                    run_id,
                    {"completed"},
                    status="failed",
                    message=f"Completed Run reconciliation failed: {exc}",
                    error=str(exc),
                    finished_at=datetime.now(UTC).isoformat(),
                )
                return self.get_run(run_id)
        elif run["status"] in TERMINAL_STATUSES:
            return run
        try:
            request = AcquisitionRequest(**run["request"])
            self._plan_committed_pages(run_id, request)
            run = self.get_run(run_id)
            if run["status"] in {"paused", "cancelled"}:
                return run
            protocol_run_id = f"acq-{run_id}"
            with self._registry_lock:
                EarthLake(self.root).start_run({"interface": "acquisition", "acquisition_run_id": run_id}, run_id=protocol_run_id)
            self._reconcile_terminal_attempts(run_id)
            self.store.recompute_run_counters(run_id)
            self.store.update_run(
                run_id,
                started_at=run["started_at"] or datetime.now(UTC).isoformat(),
            )
            page_number, cursor, discovery_complete = self.store.search_checkpoint(run_id)
            remaining = (
                request.max_items - run["discovered_items"]
                if request.max_items is not None
                else None
            )
            while True:
                self._check_control(run_id)
                pending = self.store.pending_batch_count(run_id)
                if pending and (discovery_complete or pending >= request.max_buffered_batches):
                    batch = self.store.next_pending_batch(run_id)
                    if batch:
                        self._transition_active(
                            run_id,
                            status="downloading",
                            message=f"Downloading batch {batch['batch_number']}.",
                        )
                        self._run_batch(run_id, batch)
                        continue
                if not discovery_complete:
                    self._transition_active(
                        run_id,
                        status="discovering",
                        message=f"Discovering Search Page {page_number}.",
                    )
                    page_number, cursor, discovery_complete, item_count = self._discover_page(
                        run_id,
                        request,
                        page_number,
                        cursor,
                        100,
                        remaining,
                    )
                    if remaining is not None:
                        remaining -= item_count
                    continue
                batch = self.store.next_pending_batch(run_id)
                if batch:
                    self._transition_active(
                        run_id,
                        status="downloading",
                        message=f"Downloading batch {batch['batch_number']}.",
                    )
                    self._run_batch(run_id, batch)
                    continue
                break
            return self.finalize_run(run_id, protocol_run_id)
        except AuthenticationRequired as exc:
            self.store.transition_run(
                run_id,
                ACTIVE_STATUSES,
                status="auth_required",
                message="NASA Earthdata authentication required.",
                error=str(exc),
            )
        except RunInterrupted:
            pass
        except Exception as exc:
            logger.exception("Acquisition run %s failed", run_id)
            self.store.transition_run(
                run_id,
                ACTIVE_STATUSES,
                status="failed",
                message=f"Acquisition failed: {exc}",
                error=str(exc),
                finished_at=datetime.now(UTC).isoformat(),
            )
        return self.get_run(run_id)

    def _reconcile_completed_state(
        self,
        run_id: str,
        *,
        allow_asset_repair: bool,
    ) -> dict[str, Any] | None:
        """Reconcile a completed Run before either exposing or resuming it."""

        self._reconcile_terminal_attempts(run_id)
        self.store.recompute_run_counters(run_id)
        unfinished = [
            attempt
            for attempt in self.store.attempts_for_run(run_id)
            if attempt["status"] not in {"completed", "skipped"}
        ]
        if unfinished:
            if allow_asset_repair:
                self.store.transition_run(
                    run_id,
                    {"completed"},
                    status="queued",
                    message="Completed Run requires Asset repair; queued for recovery.",
                    finished_at=None,
                    error=None,
                )
                return None
            self.store.transition_run(
                run_id,
                {"completed"},
                status="failed",
                message="Completed Run has invalid or missing terminal Assets; transfer recovery is required.",
                error="external completion reconciliation found unfinished Asset attempts",
                finished_at=datetime.now(UTC).isoformat(),
            )
            return self.get_run(run_id)
        return self.finalize_run(run_id)

    def finalize_run(self, run_id: str, protocol_run_id: str | None = None) -> dict[str, Any]:
        """Idempotently publish a run whose transfers are already complete."""

        if protocol_run_id is None:
            protocol_run_id = f"acq-{run_id}"
        if not self.store.is_planning_complete(run_id):
            raise RuntimeError("cannot finalize a run before planning completes")
        self._reconcile_terminal_attempts(run_id)
        self.store.recompute_run_counters(run_id)
        run = self.get_run(run_id)
        attempts = self.store.attempts_for_run(run_id)
        unfinished = [
            attempt for attempt in attempts
            if attempt["status"] not in {"completed", "skipped"}
        ]
        if unfinished:
            raise RuntimeError("cannot finalize a run with unfinished Asset attempts")
        planning_errors = int(run.get("planning_errors") or 0)
        has_failures = bool(run["failed_files"] or planning_errors)
        status = "partial" if has_failures and run["completed_files"] else "failed" if has_failures else "completed"
        if run["status"] in TERMINAL_STATUSES and run["status"] == status:
            with self._registry_lock:
                lake = EarthLake(self.root)
                lake.reconcile_processing_run(
                    protocol_run_id,
                    status,
                    self._processing_output_asset_ids(run_id, lake),
                    parameters={"interface": "acquisition", "acquisition_run_id": run_id},
                )
            return self.get_run(run_id)
        failure_message = (
            f"Finished with {run['completed_files']} completed, {run['failed_files']} failed files, "
            f"and {planning_errors} planning errors."
        )
        if not self.store.transition_run(
            run_id,
            {"queued", "discovering", "planning", "downloading", "finalizing", "failed", "partial"},
            status="finalizing",
            message="Finalizing protocol registries.",
            finished_at=None,
        ):
            current = self.get_run(run_id)
            if current["status"] != "finalizing":
                raise RuntimeError(f"cannot finalize run in status {current['status']}")
        with self._registry_lock:
            lake = EarthLake(self.root)
            lake.reconcile_processing_run(
                protocol_run_id,
                status,
                self._processing_output_asset_ids(run_id, lake),
                parameters={"interface": "acquisition", "acquisition_run_id": run_id},
            )
        self.store.transition_run(
            run_id,
            {"finalizing"},
            status=status,
            message=failure_message,
            error=run.get("error") if planning_errors else None,
            finished_at=datetime.now(UTC).isoformat(),
            current_file=None,
        )
        return self.get_run(run_id)

    def _processing_output_asset_ids(self, run_id: str, lake: EarthLake) -> list[str]:
        """Resolve this acquisition's outputs from durable attempts and Registry."""

        output_ids: set[str] = set()
        for attempt in self.store.attempts_for_run(run_id):
            if attempt["status"] not in {"completed", "skipped"}:
                continue
            item = self.store.item(
                run_id,
                attempt["catalog"],
                attempt["collection_id"],
                attempt["source_item_id"],
            )
            identity = AssetIdentity.from_item(
                attempt["catalog"],
                item,
                attempt["asset_key"],
            )
            asset_id = identity.digest()
            if lake.registered_asset(asset_id) is None:
                raise RuntimeError(
                    f"completed attempt has no registered canonical Asset: {asset_id}"
                )
            output_ids.add(asset_id)
        return sorted(output_ids)

    def _reconcile_terminal_attempts(self, run_id: str) -> None:
        """Repair stale terminal attempts before scheduling or finalization."""

        terminal = {"completed", "skipped"}
        lake = EarthLake(self.root)
        for attempt in self.store.attempts_for_run(run_id):
            if attempt["status"] not in terminal:
                continue
            item = self.store.item(
                run_id,
                attempt["catalog"],
                attempt["collection_id"],
                attempt["source_item_id"],
            )
            asset = (item.get("assets") or {}).get(attempt["asset_key"])
            if not isinstance(asset, dict):
                self.store.update_attempt(
                    attempt["attempt_id"],
                    status="queued",
                    error="terminal attempt cannot be revalidated: Asset metadata is missing",
                )
                continue
            raw_destination = Path(attempt["destination"])
            missing_destination = not str(attempt["destination"] or "").strip() or str(attempt["destination"]).strip() in {".", "./"}
            raw_absolute = raw_destination.expanduser().resolve() if raw_destination.is_absolute() else None
            if missing_destination:
                identity = AssetIdentity.from_item(attempt["catalog"], item, attempt["asset_key"])
                destination = canonical_asset_path(self.root, identity, asset)
                self.store.update_attempt(attempt["attempt_id"], destination=str(destination))
                raw_absolute = None
            else:
                destination = self._canonical_destination(raw_destination)
            if raw_absolute is not None and not raw_absolute.is_relative_to(self.root):
                identity = AssetIdentity.from_item(attempt["catalog"], item, attempt["asset_key"])
                lake = EarthLake(self.root)
                canonical = canonical_asset_path(self.root, identity, asset)
                self._relocate_legacy_download(raw_absolute, canonical)
                destination = canonical.resolve()
                self.store.update_attempt(attempt["attempt_id"], destination=str(destination))
            if not destination.is_file():
                self.store.update_attempt(
                    attempt["attempt_id"],
                    status="queued",
                    error="terminal attempt cache is missing; queued for redownload",
                )
                self._queue_batch(run_id, attempt["batch_number"])
                continue
            result = IntegrityGate.validate(destination, asset)
            if not result.ok:
                self.store.update_attempt(
                    attempt["attempt_id"],
                    status="queued",
                    error=f"terminal attempt cache failed revalidation: {result.reason_code}",
                )
                self._queue_batch(run_id, attempt["batch_number"])
                continue

            identity = AssetIdentity.from_item(attempt["catalog"], item, attempt["asset_key"])
            registered = lake.registered_asset(identity.digest())
            relative_path = destination.relative_to(self.root).as_posix()
            if not registered or registered.get("local_path") != relative_path:
                self._complete_attempt(
                    run_id,
                    attempt,
                    item,
                    destination,
                    attempt["status"],
                    result,
                    increment_counters=False,
                )
        self.store.recompute_run_counters(run_id)

    def _queue_batch(self, run_id: str, batch_number: int) -> None:
        self.store.update_batch(run_id, batch_number, status="queued")

    def _run_batch(self, run_id: str, batch: dict[str, Any]) -> None:
        attempts = [item for item in self.store.attempts_for_batch(run_id, batch["batch_number"]) if item["status"] not in {"completed", "skipped"}]
        if not attempts:
            # A process can stop after all attempts finish but before the batch
            # status is committed. Finalize that batch so resume can advance.
            current = self.store.attempts_for_batch(run_id, batch["batch_number"])
            completed = sum(item["status"] in {"completed", "skipped"} for item in current)
            failed = sum(item["status"] == "failed" for item in current)
            self.store.update_batch(
                run_id,
                batch["batch_number"],
                status="partial" if failed and completed else "failed" if failed else "completed",
                completed_count=completed,
                failed_count=failed,
            )
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
        request = self.get_run(run_id)["request"]
        item = self.store.item(
            run_id,
            attempt["catalog"],
            attempt["collection_id"],
            attempt["source_item_id"],
        )
        asset = (item.get("assets") or {}).get(attempt["asset_key"])
        if not isinstance(asset, dict):
            raise ValueError(f"Asset metadata missing for {attempt['asset_key']}")
        identity = AssetIdentity.from_item(request["catalog"], item, attempt["asset_key"])
        lake = EarthLake(self.root)
        canonical_from_asset = canonical_asset_path(self.root, identity, asset)
        raw_destination = Path(attempt["destination"])
        missing_destination = not str(attempt["destination"] or "").strip() or str(attempt["destination"]).strip() in {".", "./"}
        raw_absolute = raw_destination.expanduser().resolve() if raw_destination.is_absolute() else None
        stored_destination = (
            canonical_from_asset
            if missing_destination
            else self._canonical_destination(raw_destination)
        )
        if missing_destination:
            self.store.update_attempt(attempt["attempt_id"], destination=str(canonical_from_asset))
        # A planned destination is authoritative for retries and refreshed
        # signed URLs. Only pre-1A absolute paths outside the current lake are
        # relocated, and the relocation target is derived from Asset metadata,
        # never from the request-time URL basename.
        destination = (
            self._canonical_destination(canonical_from_asset)
            if raw_absolute is not None and not raw_absolute.is_relative_to(self.root)
            else stored_destination
        )
        if destination != stored_destination:
            relocation_source = raw_absolute if raw_absolute is not None else stored_destination
            self._relocate_legacy_download(relocation_source, destination)
            self.store.update_attempt(attempt["attempt_id"], destination=str(destination))
        destination.parent.mkdir(parents=True, exist_ok=True)
        metadata = destination.parent / "metadata.json"
        if not metadata.exists():
            metadata.write_text(json.dumps(item, indent=2), encoding="utf-8")
        self.store.update_run(run_id, current_file=destination.name)
        if destination.exists():
            cache_result = IntegrityGate.validate(destination, asset)
            if cache_result.ok:
                self._complete_attempt(run_id, attempt, item, destination, "skipped", cache_result)
                return
            logger.warning(
                "Invalid existing cache for catalog=%s collection=%s item=%s asset=%s path=%s: %s",
                identity.catalog, identity.collection, identity.item_id, identity.asset_key,
                destination, cache_result,
            )
        session = http_session()
        for retry in range(1, 4):
            self.store.update_attempt(attempt["attempt_id"], status="downloading", attempts=retry, error=None)
            self.store.update_run(run_id, current_file=destination.name)
            pending_bytes = 0
            last_flush = time.monotonic()

            def flush_progress(*, check_control: bool) -> None:
                nonlocal pending_bytes, last_flush
                if pending_bytes:
                    self.store.increment_run(run_id, "downloaded_bytes", pending_bytes)
                    pending_bytes = 0
                    last_flush = time.monotonic()
                if check_control:
                    self._check_control(run_id)

            def on_chunk(size: int) -> None:
                nonlocal pending_bytes
                pending_bytes += size
                if (
                    pending_bytes >= self.PROGRESS_FLUSH_BYTES
                    or time.monotonic() - last_flush >= self.PROGRESS_FLUSH_SECONDS
                ):
                    flush_progress(check_control=True)

            try:
                try:
                    if self._uses_default_transfer:
                        integrity = download_asset_checked(
                            session,
                            attempt["source_url"],
                            destination,
                            on_chunk,
                            asset_metadata=asset,
                            asset_identity=identity,
                        )
                    else:
                        temporary = destination.with_name(f"{destination.name}.part")
                        self.transfer(session, attempt["source_url"], temporary, on_chunk)
                        integrity = IntegrityGate.validate(temporary, asset)
                        if not integrity.ok:
                            temporary.unlink(missing_ok=True)
                            raise IntegrityError(integrity)
                        os.replace(temporary, destination)
                        integrity = integrity.bind_path(destination)
                finally:
                    flush_progress(check_control=False)
                self._check_control(run_id)
                self._complete_attempt(run_id, attempt, item, destination, "completed", integrity)
                return
            except IntegrityError as exc:
                error = exc
                if exc.result.reason_code not in {"content_length_mismatch", "zero_byte_file"}:
                    break
            except requests.HTTPError as exc:
                error = exc
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

    def _canonical_destination(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self.root / path
        path = path.resolve()
        try:
            path.relative_to(self.root)
            return path
        except ValueError:
            pass

        # Acquisition plans created before EARTH_LAKE_ROOT was changed contain
        # absolute paths such as <old-root>/downloads/source/....
        parts = path.parts
        for index, part in enumerate(parts):
            if part == "source" and index > 0 and parts[index - 1] in {"downloads", "stac"}:
                return self.root / Path(*parts[index:])
        raise ValueError(f"Download destination is outside the current Earth Lake root: {path}")

    @staticmethod
    def _copy_if_missing(source: Path, destination: Path) -> None:
        if not source.is_file() or destination.exists():
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.legacy.tmp")
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)

    def _relocate_legacy_download(self, legacy: Path, current: Path) -> None:
        self._copy_if_missing(legacy, current)
        legacy_part = legacy.with_name(f"{legacy.name}.part")
        current_part = current.with_name(f"{current.name}.part")
        self._copy_if_missing(legacy_part, current_part)
        self._copy_if_missing(
            legacy.with_name(f"{legacy_part.name}.meta"),
            current.with_name(f"{current_part.name}.meta"),
        )

    def _complete_attempt(
        self,
        run_id: str,
        attempt: dict[str, Any],
        item: dict[str, Any],
        destination: Path,
        status: str,
        integrity: IntegrityResult,
        increment_counters: bool = True,
    ) -> None:
        if not integrity.ok:
            raise IntegrityError(integrity)
        with self._registry_lock:
            lake = EarthLake(self.root)
            lake.record_asset(
                run_id=f"acq-{run_id}", catalog=attempt["catalog"], item=item, asset_key=attempt["asset_key"],
                source_url=attempt["source_url"], local_path=destination, status=status,
                integrity=integrity,
                asset_category=classify_asset(attempt["asset_key"], item["assets"][attempt["asset_key"]]),
            )
        self.store.update_attempt(attempt["attempt_id"], status=status, downloaded_bytes=destination.stat().st_size)
        if increment_counters:
            self.store.increment_run(run_id, "completed_files", 1)

    def _check_control(self, run_id: str) -> None:
        status = self.store.run_status(run_id)
        if status == "paused":
            raise RunInterrupted()
        if status in {"cancelled", "cancelling"}:
            if status == "cancelling":
                self.store.transition_run(
                    run_id,
                    {"cancelling"},
                    status="cancelled",
                    message="Run cancelled by user.",
                    finished_at=datetime.now(UTC).isoformat(),
                )
            raise RunInterrupted()

    def _transition_active(self, run_id: str, **values: Any) -> None:
        if not self.store.transition_run(run_id, ACTIVE_STATUSES, **values):
            self._check_control(run_id)
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
                self._scheduler_wake.clear()
                try:
                    for run_id in self.store.queued_run_ids(limit=100):
                        self.run(run_id)
                except (OSError, sqlite3.OperationalError, requests.RequestException) as exc:
                    # External volumes can briefly disappear or reject a lock.
                    # Keep the scheduler alive and retry on the next wake.
                    import logging
                    logging.getLogger(__name__).warning("Acquisition scheduler retrying: %s", exc)
                self._scheduler_wake.wait(5.0)

        self._scheduler_thread = threading.Thread(target=loop, name="acquisition-scheduler", daemon=True)
        self._scheduler_thread.start()

    def stop_scheduler(self) -> None:
        self._scheduler_stop.set()
        self._scheduler_wake.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
