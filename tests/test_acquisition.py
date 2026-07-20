import tempfile
import unittest
from pathlib import Path
import gzip
import json

from acquisition import AcquisitionManager, AcquisitionRequest


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


class AcquisitionPipelineTests(unittest.TestCase):
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
            destination.write_bytes(b"raster-data")
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

    def test_resume_continues_from_last_committed_search_cursor(self):
        cursors = []
        fail_once = {"value": True}

        def fetch_page(request, cursor, page_size):
            cursors.append(cursor)
            if cursor is None:
                return ([{"id": "one", "collection": "HLS", "assets": {}}], "page-2")
            if fail_once["value"]:
                fail_once["value"] = False
                raise OSError("catalog interrupted")
            return ([{"id": "two", "collection": "HLS", "assets": {}}], None)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page)
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


if __name__ == "__main__":
    unittest.main()
