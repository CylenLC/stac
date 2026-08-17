import tempfile
import unittest
from pathlib import Path
import gzip
import hashlib
import json
from unittest.mock import patch

import numpy as np
import rasterio
from rasterio.transform import from_origin

from acquisition import AcquisitionManager, AcquisitionRequest
from earth_lake import EarthLake, REGISTRY_SCHEMAS
from stac_identity import AssetIdentity


def write_test_raster(path: Path) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=1,
        height=1,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 1, 1, 1),
    ) as dataset:
        dataset.write(np.array([[1]], dtype="uint8"), 1)


class AcquisitionStoreTests(unittest.TestCase):
    def request(self) -> AcquisitionRequest:
        return AcquisitionRequest(
            catalog="nasa",
            collections=["HLSL30_V2.0"],
            wkt="POINT (-100 40)",
            start_date="2024-01-01",
            end_date="2024-01-02",
            max_items=None,
            only_main=True,
        )

    def test_idempotency_and_state_survive_manager_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            first = AcquisitionManager(directory)
            run_id = first.create_run(self.request(), "same-submission")
            self.assertEqual(first.create_run(self.request(), "same-submission"), run_id)

            second = AcquisitionManager(directory)
            run = second.get_run(run_id)
            self.assertEqual(run["status"], "queued")
            self.assertEqual(run["request"]["collections"], ["HLSL30_V2.0"])
            self.assertTrue((Path(directory) / "registry" / "acquisition_state.sqlite").exists())
            self.assertTrue((Path(directory) / "manifests" / "acquisitions" / run_id / "request.json").exists())

    def test_pause_resume_and_cancel_are_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            run_id = manager.create_run(self.request(), "controls")
            manager.pause_run(run_id)
            self.assertEqual(manager.get_run(run_id)["status"], "paused")
            manager.resume_run(run_id)
            self.assertEqual(manager.get_run(run_id)["status"], "queued")
            manager.cancel_run(run_id)
            self.assertEqual(manager.get_run(run_id)["status"], "cancelled")
            with self.assertRaises(ValueError):
                manager.resume_run(run_id)

    def test_empty_retry_does_not_change_failed_run(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            run_id = manager.create_run(self.request(), "empty-retry")
            manager.store.update_run(run_id, status="failed")

            with self.assertRaisesRegex(ValueError, "no unfinished transfers"):
                manager.retry_failed(run_id)

            self.assertEqual(manager.get_run(run_id)["status"], "failed")

    def test_retry_requeues_stuck_downloading_attempts_after_run_level_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            run_id = manager.create_run(self.request(), "stuck-retry")
            manager.store.replace_plan(run_id, [[{
                "source_item_id": "item-1",
                "asset_key": "B04",
                "source_url": "https://example.test/item.tif",
                "destination": str(Path(directory) / "source" / "item.tif"),
            }]])
            attempt = manager.store.attempts_for_batch(run_id, 1)[0]
            manager.store.update_attempt(attempt["attempt_id"], status="downloading")
            manager.store.update_run(run_id, status="failed", error="startup failure")

            result = manager.retry_failed(run_id)

            self.assertEqual(result["status"], "queued")
            self.assertEqual(manager.store.attempts_for_batch(run_id, 1)[0]["status"], "queued")

    def test_retry_accepts_queued_attempts_after_registry_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            run_id = manager.create_run(self.request(), "queued-retry")
            manager.store.replace_plan(run_id, [[{
                "source_item_id": "item-1",
                "asset_key": "B04",
                "source_url": "https://example.test/item.tif",
                "destination": str(Path(directory) / "source" / "item.tif"),
            }]])
            manager.store.update_run(run_id, status="failed", error="registry failure")

            result = manager.retry_failed(run_id)

            self.assertEqual(result["status"], "queued")

    def test_resume_finalizes_batch_with_only_completed_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            run_id = manager.create_run(self.request(), "stale-batch")
            manager.store.replace_plan(run_id, [[{
                "source_item_id": "item-1",
                "asset_key": "B04",
                "source_url": "https://example.test/item.tif",
                "destination": str(Path(directory) / "source" / "item.tif"),
            }]])
            attempt = manager.store.attempts_for_batch(run_id, 1)[0]
            manager.store.update_attempt(attempt["attempt_id"], status="completed")
            manager.store.update_batch(run_id, 1, status="downloading")

            manager._run_batch(run_id, {"batch_number": 1})

            self.assertEqual(manager.store.list_batches(run_id)[0]["status"], "completed")


class AcquisitionPipelineTests(unittest.TestCase):
    def test_empty_asset_selection_is_persisted_and_cannot_complete(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "no-data", "collection": "HLS", "assets": {}}], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page)
            run_id = manager.create_run(self._request(), "planning-error")

            result = manager.run(run_id)

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["planning_errors"], 1)
            self.assertEqual(len(manager.store.planning_errors_for_run(run_id)), 1)
            self.assertIn("no policy-approved", result["error"])

    def test_valid_preview_tiff_is_rejected_before_transfer_or_registry(self):
        transfer_calls = []

        def fetch_page(request, cursor, page_size):
            return ([{
                "id": "preview-only",
                "collection": "generic",
                "properties": {},
                "assets": {"preview": {"href": "https://example/preview.tif", "type": "image/tiff"}},
            }], None)

        def transfer(session, url, destination, on_chunk):
            transfer_calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "preview-integrity-boundary")

            result = manager.run(run_id)

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["planning_errors"], 1)
            self.assertEqual(transfer_calls, [])
            self.assertEqual(manager.store.attempts_for_run(run_id), [])

    def test_completed_attempt_with_missing_file_is_requeued_and_repaired(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "recover", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "recover-missing-final")
            first = manager.run(run_id)
            self.assertEqual(first["status"], "completed")
            attempt = manager.store.attempts_for_run(run_id)[0]
            Path(attempt["destination"]).unlink()
            manager.store.update_run(run_id, status="failed", error="simulated restart")
            manager.resume_run(run_id)

            second = manager.run(run_id)

            self.assertEqual(second["status"], "completed")
            self.assertEqual(len(calls), 2)
            self.assertTrue(Path(attempt["destination"]).is_file())

    def test_valid_file_with_missing_registry_is_reregistered_without_transfer(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "recover-registry", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "recover-missing-registry")
            self.assertEqual(manager.run(run_id)["status"], "completed")
            self.assertEqual(len(calls), 1)
            lake = EarthLake(directory)
            lake._write_table(lake.registry_dir / "assets.parquet", [], REGISTRY_SCHEMAS["assets"])
            manager.store.update_run(run_id, status="failed", error="simulated registry gap")
            manager.resume_run(run_id)

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(EarthLake(directory)._read_rows("assets")), 1)

    def test_finalization_failure_can_be_retried_without_transfer(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "finalize-retry", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        original_finish = EarthLake.finish_run
        finish_calls = []

        def finish_once(self, *args, **kwargs):
            finish_calls.append(1)
            if len(finish_calls) == 1:
                raise RuntimeError("simulated finalization failure")
            return original_finish(self, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory, patch("earth_lake.EarthLake.finish_run", new=finish_once):
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "finalization-only-retry")
            failed = manager.run(run_id)
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(len(calls), 1)

            queued = manager.retry_failed(run_id)
            self.assertEqual(queued["status"], "queued")
            completed = manager.run(run_id)

            self.assertEqual(completed["status"], "completed")
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(finish_calls), 2)

    def test_empty_run_finalization_failure_can_be_retried_without_transfer(self):
        fetch_calls = []
        finish_calls = []

        def fetch_page(request, cursor, page_size):
            fetch_calls.append(1)
            return ([], None)

        original_finish = EarthLake.finish_run

        def finish_once(self, *args, **kwargs):
            finish_calls.append(1)
            if len(finish_calls) == 1:
                raise RuntimeError("simulated empty finalization failure")
            return original_finish(self, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory, patch("earth_lake.EarthLake.finish_run", new=finish_once):
            manager = AcquisitionManager(
                directory,
                page_fetcher=fetch_page,
                transfer=lambda *args, **kwargs: self.fail("empty Run must not transfer"),
            )
            run_id = manager.create_run(self._request(), "empty-finalization-only-retry")

            failed = manager.run(run_id)
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(manager.store.attempts_for_run(run_id), [])

            queued = manager.retry_failed(run_id)
            self.assertEqual(queued["status"], "queued")
            completed = manager.run(run_id)

            self.assertEqual(completed["status"], "completed")
            self.assertEqual(fetch_calls, [1])
            self.assertEqual(len(finish_calls), 2)
            processing = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(processing["run_id"], f"acq-{run_id}")
            self.assertEqual(json.loads(processing["output_asset_ids"]), [])
            self.assertEqual(
                processing["checksum"],
                hashlib.sha256(b"").hexdigest(),
            )

    def test_empty_run_finalization_retry_survives_manager_restart(self):
        fetch_calls = []
        finish_calls = []
        original_finish = EarthLake.finish_run

        def fetch_page(request, cursor, page_size):
            fetch_calls.append(cursor)
            return ([], None)

        def finish_once(self, *args, **kwargs):
            finish_calls.append(1)
            if len(finish_calls) == 1:
                raise RuntimeError("simulated restart finalization failure")
            return original_finish(self, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            with patch("earth_lake.EarthLake.finish_run", new=finish_once):
                first = AcquisitionManager(
                    directory,
                    page_fetcher=fetch_page,
                    transfer=lambda *args, **kwargs: self.fail("empty Run must not transfer"),
                )
                run_id = first.create_run(self._request(), "empty-restart-finalization")
                self.assertEqual(first.run(run_id)["status"], "failed")
                second = AcquisitionManager(
                    directory,
                    page_fetcher=lambda *args: self.fail("empty finalization retry must not search"),
                    transfer=lambda *args, **kwargs: self.fail("empty Run must not transfer"),
                )
                queued = second.retry_failed(run_id)
                completed = second.run(run_id)

                self.assertEqual(queued["status"], "queued")
                self.assertEqual(completed["status"], "completed")
                self.assertEqual(fetch_calls, [None])
                self.assertEqual(len(finish_calls), 2)
                self.assertEqual(second.store.attempts_for_run(run_id), [])

    def test_incomplete_pagination_cannot_finalize_or_retry_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=lambda *args: ([], None))
            run_id = manager.create_run(self._request(), "incomplete-pagination-finalization")
            manager._commit_page(run_id, 1, None, "cursor-2", [], self._request().catalog)
            manager._plan_page(run_id, self._request(), 1, [])
            manager.store.update_run(run_id, status="failed", error="simulated interruption")

            self.assertFalse(manager.store.is_planning_complete(run_id))
            self.assertFalse(manager.store.can_finalize_without_transfer(run_id))
            with self.assertRaisesRegex(RuntimeError, "planning completes"):
                manager.finalize_run(run_id)
            with self.assertRaisesRegex(ValueError, "no unfinished transfers"):
                manager.retry_failed(run_id)

    def test_multi_page_empty_search_reaches_terminal_planning(self):
        cursors = []

        def fetch_page(request, cursor, page_size):
            cursors.append(cursor)
            return ([], "cursor-2") if cursor is None else ([], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(
                directory,
                page_fetcher=fetch_page,
                transfer=lambda *args, **kwargs: self.fail("empty Run must not transfer"),
            )
            run_id = manager.create_run(self._request(), "multi-page-empty")
            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(cursors, [None, "cursor-2"])
            self.assertTrue(manager.store.is_planning_complete(run_id, require_no_errors=True))
            self.assertEqual(manager.store.attempts_for_run(run_id), [])
            processing = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(json.loads(processing["output_asset_ids"]), [])

    def test_empty_first_page_does_not_hide_later_asset_page(self):
        cursors = []

        def fetch_page(request, cursor, page_size):
            cursors.append(cursor)
            if cursor is None:
                return ([], "asset-page")
            return ([{
                "id": "later-asset",
                "collection": "HLSL30_V2.0",
                "properties": {},
                "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}},
            }], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "empty-first-page-later-asset")
            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(cursors, [None, "asset-page"])
            self.assertEqual(len(manager.store.attempts_for_run(run_id)), 1)

    def test_completed_attempts_cannot_mask_incomplete_pagination(self):
        item = {
            "id": "first-page-asset",
            "collection": "HLSL30_V2.0",
            "properties": {},
            "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}},
        }
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=lambda *args: ([], None))
            request = self._request()
            run_id = manager.create_run(request, "completed-attempt-incomplete-pagination")
            manager._commit_page(run_id, 1, None, "cursor-2", [item], request.catalog)
            manager._plan_page(run_id, request, 1, [item])
            attempt = manager.store.attempts_for_run(run_id)[0]
            manager.store.update_attempt(attempt["attempt_id"], status="completed")

            with self.assertRaisesRegex(RuntimeError, "planning completes"):
                manager.finalize_run(run_id)

    def test_terminal_page_must_be_planned_before_finalization(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=lambda *args: ([], None))
            run_id = manager.create_run(self._request(), "terminal-page-unplanned")
            manager._commit_page(run_id, 1, None, None, [], self._request().catalog)

            self.assertFalse(manager.store.is_planning_complete(run_id))
            with self.assertRaisesRegex(RuntimeError, "planning completes"):
                manager.finalize_run(run_id)

    def test_external_status_rejects_completed_run_with_incomplete_pagination(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=lambda *args: ([], None))
            request = self._request()
            run_id = manager.create_run(request, "external-incomplete-pagination")
            manager._commit_page(run_id, 1, None, "cursor-2", [], request.catalog)
            manager._plan_page(run_id, request, 1, [])
            manager.store.update_run(run_id, status="completed")

            result = manager.reconcile_external_status(run_id)

            self.assertEqual(result["status"], "failed")

    def test_planning_failure_with_zero_attempts_is_not_empty_finalization_retry(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "planning-failure", "collection": "HLS", "assets": {}}], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page)
            run_id = manager.create_run(self._request(), "zero-attempt-planning-failure")
            result = manager.run(run_id)

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["planning_errors"], 1)
            with self.assertRaisesRegex(ValueError, "no unfinished transfers"):
                manager.retry_failed(run_id)

    def test_external_status_fails_closed_for_processing_association_mismatch(self):
        def fetch_page(request, cursor, page_size):
            return ([{
                "id": "association-mismatch",
                "collection": "HLS",
                "properties": {},
                "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}},
            }], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "association-mismatch")
            self.assertEqual(manager.run(run_id)["status"], "completed")
            lake = EarthLake(directory)
            row = lake.processing_run(f"acq-{run_id}")
            row["parameters_json"] = json.dumps(
                {"interface": "acquisition", "acquisition_run_id": "other-run"},
                sort_keys=True,
            )
            lake._write_table(lake.registry_dir / "processing_runs.parquet", [row], REGISTRY_SCHEMAS["processing_runs"])

            result = manager.reconcile_external_status(run_id)

            self.assertEqual(result["status"], "failed")
            unchanged = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(json.loads(unchanged["parameters_json"])["acquisition_run_id"], "other-run")

    def test_finalize_run_is_idempotent_after_success(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "idempotent", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "idempotent-finalization")
            first = manager.run(run_id)
            second = manager.finalize_run(run_id)
            third = manager.finalize_run(run_id)

            self.assertEqual(first["status"], "completed")
            self.assertEqual(second["status"], "completed")
            self.assertEqual(third["status"], "completed")
            self.assertEqual(second["completed_files"], third["completed_files"])
            self.assertEqual(len(EarthLake(directory)._read_rows("assets")), 1)

    def test_completed_run_recreates_missing_processing_registry_without_transfer(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "missing-processing", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "missing-processing")
            self.assertEqual(manager.run(run_id)["status"], "completed")

            lake = EarthLake(directory)
            processing_path = lake.registry_dir / "processing_runs.parquet"
            processing_path.unlink()

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(calls), 1)
            rows = EarthLake(directory)._read_rows("processing_runs")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], f"acq-{run_id}")
            self.assertEqual(rows[0]["status"], "completed")

    def test_completed_run_repairs_running_processing_row_without_transfer(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "running-processing", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "running-processing")
            self.assertEqual(manager.run(run_id)["status"], "completed")

            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["status"] = "running"
            rows[0]["end_time"] = None
            rows[0]["checksum"] = None
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(calls), 1)
            repaired = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(repaired["status"], "completed")

    def test_completed_run_repairs_stale_processing_outputs_without_transfer(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "stale-processing", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "stale-processing")
            self.assertEqual(manager.run(run_id)["status"], "completed")

            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["output_asset_ids"] = json.dumps(["stale-asset"])
            rows[0]["checksum"] = "stale-checksum"
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(calls), 1)
            expected = EarthLake(directory).output_asset_ids(f"acq-{run_id}")
            repaired = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(json.loads(repaired["output_asset_ids"]), expected)

    def test_reused_canonical_asset_is_output_of_each_acquisition_run(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "reused-output", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            first_id = manager.create_run(self._request(), "first-output")
            self.assertEqual(manager.run(first_id)["status"], "completed")

            second_id = manager.create_run(self._request(), "second-output")
            self.assertEqual(manager.run(second_id)["status"], "completed")

            self.assertEqual(len(calls), 1)
            processing = EarthLake(directory).processing_run(f"acq-{second_id}")
            self.assertEqual(
                json.loads(processing["output_asset_ids"]),
                [EarthLake(directory)._read_rows("assets")[0]["asset_id"]],
            )

    def test_duplicate_processing_identity_fails_closed(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "duplicate-processing", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "duplicate-processing")
            self.assertEqual(manager.run(run_id)["status"], "completed")

            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows + rows, REGISTRY_SCHEMAS["processing_runs"])

            result = manager.run(run_id)

            self.assertEqual(result["status"], "failed")
            self.assertIn("duplicate run identity", result["error"])

    def test_completed_run_reconciles_missing_file_when_reopened(self):
        calls = []

        def fetch_page(request, cursor, page_size):
            return ([{"id": "reopen-completed", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "reopen-completed")
            self.assertEqual(manager.run(run_id)["status"], "completed")
            attempt = manager.store.attempts_for_run(run_id)[0]
            Path(attempt["destination"]).unlink()

            reopened = manager.run(run_id)

            self.assertEqual(reopened["status"], "completed")
            self.assertEqual(len(calls), 2)
            self.assertTrue(Path(attempt["destination"]).is_file())

    def test_persisted_destination_survives_signed_url_refresh(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "signed", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://example/data.tiff?token=A", "type": "image/tiff"}}}], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "stable-signed-path")
            manager.discover_and_plan(run_id)
            attempt = manager.store.attempts_for_run(run_id)[0]
            original_destination = attempt["destination"]
            manager.store.update_attempt(
                attempt["attempt_id"], source_url="https://example/download?token=B"
            )

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            updated = manager.store.attempts_for_run(run_id)[0]
            self.assertEqual(updated["destination"], original_destination)
            self.assertTrue(Path(original_destination).is_file())

    def test_pause_wins_race_with_page_planning(self):
        manager = None
        run_id = None

        def fetch_page(request, cursor, page_size):
            manager.pause_run(run_id)
            return ([{"id": "item-1", "collection": "HLS", "assets": {}}], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page)
            run_id = manager.create_run(self._request(), "pause-during-page")

            result = manager.run(run_id)

            self.assertEqual(result["status"], "paused")
            self.assertEqual(manager.store.unplanned_pages(run_id), [1])

    def test_resume_relocates_legacy_download_destination(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "legacy-item", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://x/legacy.tif"}}}], None)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = AcquisitionManager(root / "lake", page_fetcher=fetch_page)
            run_id = manager.create_run(self._request(), "legacy-destination")
            manager.discover_and_plan(run_id)
            attempt = manager.store.next_pending_batch(run_id)
            self.assertIsNotNone(attempt)
            attempt_row = manager.store.attempts_for_batch(run_id, attempt["batch_number"])[0]
            legacy = root / "old" / "downloads" / "source" / "nasa" / "HLS" / "legacy-item" / "legacy.tif"
            legacy.parent.mkdir(parents=True)
            write_test_raster(legacy)
            manager.store.update_attempt(attempt_row["attempt_id"], destination=str(legacy))

            result = manager.run(run_id)

            identity = AssetIdentity("nasa", "HLS", "legacy-item", "B04")
            current = root / "lake" / "source" / "nasa" / "HLS" / "legacy-item" / f"B04__{identity.digest(16)}.tif"
            updated = manager.store.attempts_for_batch(run_id, attempt["batch_number"])[0]
            self.assertEqual(result["status"], "completed")
            self.assertEqual(updated["destination"], str(current.resolve()))
            self.assertEqual(current.read_bytes(), legacy.read_bytes())
            self.assertTrue(legacy.is_file())

    def test_commits_search_pages_before_planning_asset_batches(self):
        pages = [
            ([{"id": "item-1", "collection": "HLS", "assets": {"B04": {"href": "https://x/1.tif"}, "B05": {"href": "https://x/2.tif"}}}], "next"),
            ([{"id": "item-2", "collection": "HLS", "assets": {"B04": {"href": "https://x/3.tif"}}}], None),
        ]

        def fetch_page(request, cursor, page_size):
            self.assertEqual(cursor, None if not cursor else "next")
            return pages.pop(0)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page)
            request = AcquisitionRequest(
                catalog="nasa", collections=["HLS"], wkt="POINT (0 0)",
                start_date="2024-01-01", end_date="2024-01-02", only_main=False, batch_size=2,
            )
            run_id = manager.create_run(request, "paged")
            manager.discover_and_plan(run_id)
            run = manager.get_run(run_id)
            self.assertEqual(run["discovered_items"], 2)
            self.assertEqual(run["total_files"], 3)
            self.assertEqual([batch["asset_count"] for batch in manager.list_batches(run_id)], [2, 1])
            page_path = Path(directory) / "manifests" / "acquisitions" / run_id / "pages" / "000001.jsonl.gz"
            with gzip.open(page_path, "rt", encoding="utf-8") as source:
                self.assertEqual(json.loads(source.readline())["id"], "item-1")

    def test_run_downloads_planned_batches_and_registers_assets(self):
        def fetch_page(request, cursor, page_size):
            if cursor:
                return [], None
            return ([{"id": "item-1", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://x/1.tif"}}}], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)
            on_chunk(11)
            return 11

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            request = AcquisitionRequest(
                catalog="nasa", collections=["HLS"], wkt="POINT (0 0)",
                start_date="2024-01-01", end_date="2024-01-02", only_main=False,
            )
            run_id = manager.create_run(request, "execute")
            run = manager.run(run_id)
            self.assertEqual(run["status"], "completed")
            self.assertEqual(run["completed_files"], 1)
            self.assertEqual(run["downloaded_bytes"], 11)
            self.assertTrue((Path(directory) / "registry" / "assets.parquet").exists())

    def test_download_progress_batches_sqlite_updates(self):
        def fetch_page(request, cursor, page_size):
            return ([{"id": "item-1", "collection": "HLS", "properties": {}, "assets": {"B04": {"href": "https://x/1.tif"}}}], None)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)
            for _ in range(100):
                on_chunk(1)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            run_id = manager.create_run(self._request(), "batched-progress")

            with patch.object(manager.store, "increment_run", wraps=manager.store.increment_run) as increment:
                run = manager.run(run_id)

            byte_updates = [call for call in increment.call_args_list if call.args[1] == "downloaded_bytes"]
            self.assertEqual(run["downloaded_bytes"], 100)
            self.assertEqual(len(byte_updates), 1)

    def test_resume_continues_from_last_committed_search_cursor(self):
        cursors = []
        fail_once = {"value": True}

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)
            on_chunk(len(url))

        def fetch_page(request, cursor, page_size):
            cursors.append(cursor)
            if cursor is None:
                return ([{"id": "one", "collection": "HLS", "assets": {"B04": {"href": "https://x/one.tif"}}}], "page-2")
            if fail_once["value"]:
                fail_once["value"] = False
                raise OSError("catalog interrupted")
            return ([{"id": "two", "collection": "HLS", "assets": {"B04": {"href": "https://x/two.tif"}}}], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            request = AcquisitionRequest(
                catalog="nasa", collections=["HLS"], wkt="POINT (0 0)",
                start_date="2024-01-01", end_date="2024-01-02", only_main=False,
            )
            run_id = manager.create_run(request, "resume-search")
            self.assertEqual(manager.run(run_id)["status"], "failed")
            manager.resume_run(run_id)
            self.assertEqual(manager.run(run_id)["status"], "completed")
            self.assertEqual(cursors, [None, "page-2", "page-2"])
            self.assertEqual(manager.get_run(run_id)["discovered_items"], 2)

    def test_resume_plans_a_page_committed_before_planning(self):
        item = {
            "id": "committed",
            "collection": "HLS",
            "properties": {},
            "assets": {"B04": {"href": "https://x/committed.tif"}},
        }

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)
            on_chunk(14)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, transfer=transfer)
            run_id = manager.create_run(
                AcquisitionRequest(
                    catalog="nasa",
                    collections=["HLS"],
                    wkt="POINT (0 0)",
                    start_date="2024-01-01",
                    end_date="2024-01-02",
                    only_main=False,
                ),
                "commit-before-plan",
            )
            manager._commit_page(run_id, 1, None, None, [item], "nasa")

            self.assertEqual(manager.store.unplanned_pages(run_id), [1])
            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["total_files"], 1)
            self.assertEqual(manager.store.unplanned_pages(run_id), [])

    def test_pipeline_drains_buffer_before_fetching_next_page(self):
        transferred: list[str] = []
        fetch_count = 0

        def fetch_page(request, cursor, page_size):
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 2:
                self.assertEqual(transferred, ["https://x/one.tif"])
            item_id = "one" if cursor is None else "two"
            outgoing = "page-2" if cursor is None else None
            return ([{
                "id": item_id,
                "collection": "HLS",
                "properties": {},
                "assets": {"B04": {"href": f"https://x/{item_id}.tif"}},
            }], outgoing)

        def transfer(session, url, destination, on_chunk):
            write_test_raster(destination)
            transferred.append(url)
            on_chunk(len(url))

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(
                directory,
                page_fetcher=fetch_page,
                transfer=transfer,
            )
            run_id = manager.create_run(
                AcquisitionRequest(
                    catalog="nasa",
                    collections=["HLS"],
                    wkt="POINT (0 0)",
                    start_date="2024-01-01",
                    end_date="2024-01-02",
                    only_main=False,
                    batch_size=1,
                    max_buffered_batches=1,
                ),
                "bounded-pipeline",
            )

            result = manager.run(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(fetch_count, 2)
            self.assertEqual(len(transferred), 2)

    def test_scheduler_query_is_not_limited_to_latest_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory)
            first = None
            for index in range(205):
                run_id = manager.create_run(self._request(), f"queued-{index}")
                first = first or run_id

            queued = manager.store.queued_run_ids(limit=300)

            self.assertEqual(len(queued), 205)
            self.assertEqual(queued[0], first)

    @staticmethod
    def _request() -> AcquisitionRequest:
        return AcquisitionRequest(
            catalog="nasa",
            collections=["HLS"],
            wkt="POINT (0 0)",
            start_date="2024-01-01",
            end_date="2024-01-02",
            only_main=False,
        )


if __name__ == "__main__":
    unittest.main()
