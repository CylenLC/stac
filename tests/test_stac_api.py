import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from acquisition import AcquisitionManager, AcquisitionRequest
from earth_lake import EarthLake, REGISTRY_SCHEMAS
from stac_api import app, get_acquisition, get_task_status, invalidate_task_list_cache, list_tasks


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


class AcquisitionAPIStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.had_manager = hasattr(app.state, "acquisition_manager")
        self.previous_manager = getattr(app.state, "acquisition_manager", None)

    def tearDown(self) -> None:
        invalidate_task_list_cache()
        if self.had_manager:
            app.state.acquisition_manager = self.previous_manager
        else:
            delattr(app.state, "acquisition_manager")

    @staticmethod
    def request() -> AcquisitionRequest:
        return AcquisitionRequest(
            catalog="nasa",
            collections=["HLS"],
            wkt="POINT (0 0)",
            start_date="2024-01-01",
            end_date="2024-01-02",
            only_main=False,
        )

    def make_completed_manager(self, directory: str):
        calls: list[str] = []

        def fetch_page(request, cursor, page_size):
            return ([{
                "id": "api-item",
                "collection": "HLS",
                "properties": {},
                "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}},
            }], None)

        def transfer(session, url, destination, on_chunk):
            calls.append(url)
            write_test_raster(destination)

        manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
        run_id = manager.create_run(self.request(), f"api-{Path(directory).name}")
        self.assertEqual(manager.run(run_id)["status"], "completed")
        app.state.acquisition_manager = manager
        return manager, run_id, calls

    def make_empty_completed_manager(self, directory: str):
        fetch_calls: list[int] = []

        def fetch_page(request, cursor, page_size):
            fetch_calls.append(1)
            return ([], None)

        manager = AcquisitionManager(directory, page_fetcher=fetch_page)
        run_id = manager.create_run(self.request(), f"empty-api-{Path(directory).name}")
        self.assertEqual(manager.run(run_id)["status"], "completed")
        app.state.acquisition_manager = manager
        return manager, run_id, fetch_calls

    def test_task_status_repairs_missing_processing_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            manager, run_id, calls = self.make_completed_manager(directory)
            lake = EarthLake(directory)
            (lake.registry_dir / "processing_runs.parquet").unlink()

            result = get_task_status(run_id)

            self.assertEqual(result.status, "completed")
            self.assertEqual(len(calls), 1)
            processing = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(processing["status"], "completed")
            self.assertIsNotNone(manager.get_run(run_id))

    def test_acquisition_status_repairs_running_processing_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            _, run_id, calls = self.make_completed_manager(directory)
            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["status"] = "running"
            rows[0]["end_time"] = None
            rows[0]["checksum"] = None
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = get_acquisition(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(calls), 1)
            repaired = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(repaired["status"], "completed")

    def test_task_status_repairs_stale_processing_outputs_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            _, run_id, calls = self.make_completed_manager(directory)
            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["output_asset_ids"] = json.dumps(["stale-output"])
            rows[0]["checksum"] = "stale-checksum"
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = get_task_status(run_id)

            self.assertEqual(result.status, "completed")
            self.assertEqual(len(calls), 1)
            repaired = EarthLake(directory).processing_run(f"acq-{run_id}")
            expected = EarthLake(directory).output_asset_ids(f"acq-{run_id}")
            self.assertEqual(json.loads(repaired["output_asset_ids"]), expected)

    def test_acquisition_status_fails_closed_for_corrupt_processing_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            manager, run_id, calls = self.make_completed_manager(directory)
            path = EarthLake(directory).registry_dir / "processing_runs.parquet"
            path.write_bytes(b"not a parquet file")

            result = get_acquisition(run_id)

            self.assertNotEqual(result["status"], "completed")
            self.assertEqual(manager.get_run(run_id)["status"], "failed")
            self.assertEqual(len(calls), 1)

    def test_empty_task_status_recreates_missing_processing_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            _, run_id, fetch_calls = self.make_empty_completed_manager(directory)
            processing_path = EarthLake(directory).registry_dir / "processing_runs.parquet"
            processing_path.unlink()

            result = get_task_status(run_id)

            self.assertEqual(result.status, "completed")
            self.assertEqual(fetch_calls, [1])
            processing = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(processing["status"], "completed")
            self.assertEqual(json.loads(processing["output_asset_ids"]), [])

    def test_empty_acquisition_status_repairs_running_processing_without_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            _, run_id, fetch_calls = self.make_empty_completed_manager(directory)
            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["status"] = "running"
            rows[0]["end_time"] = None
            rows[0]["checksum"] = None
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = get_acquisition(run_id)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(fetch_calls, [1])
            self.assertEqual(EarthLake(directory).processing_run(f"acq-{run_id}")["status"], "completed")

    def test_empty_task_status_rejects_wrong_processing_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            _, run_id, fetch_calls = self.make_empty_completed_manager(directory)
            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            rows[0]["parameters_json"] = json.dumps(
                {"interface": "acquisition", "acquisition_run_id": "other-run"},
                sort_keys=True,
            )
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = get_task_status(run_id)

            self.assertEqual(result.status, "failed")
            self.assertEqual(fetch_calls, [1])
            unchanged = EarthLake(directory).processing_run(f"acq-{run_id}")
            self.assertEqual(json.loads(unchanged["parameters_json"])["acquisition_run_id"], "other-run")

    def test_task_status_rejects_completed_run_with_incomplete_pagination(self):
        with tempfile.TemporaryDirectory() as directory:
            manager, run_id, fetch_calls = self.make_empty_completed_manager(directory)
            with manager.store._lock, manager.store.transaction() as db:
                db.execute(
                    "UPDATE search_pages SET outgoing_cursor=? WHERE run_id=? AND page_number=1",
                    ("cursor-2", run_id),
                )

            result = get_task_status(run_id)

            self.assertEqual(result.status, "failed")
            self.assertEqual(fetch_calls, [1])

    def test_task_list_reconciles_completed_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            manager, first_id, calls = self.make_completed_manager(directory)
            second_id = manager.create_run(self.request(), "api-second")
            self.assertEqual(manager.run(second_id)["status"], "completed")

            first_result = list_tasks()
            self.assertEqual({item.task_id for item in first_result}, {first_id, second_id})

            lake = EarthLake(directory)
            rows = lake._read_rows("processing_runs")
            for row in rows:
                if row["run_id"] == f"acq-{second_id}":
                    row["status"] = "running"
                    row["end_time"] = None
                    row["checksum"] = None
            lake._write_table(lake.registry_dir / "processing_runs.parquet", rows, REGISTRY_SCHEMAS["processing_runs"])

            result = list_tasks()

            statuses = {item.task_id: item.status for item in result}
            self.assertEqual(statuses[first_id], "completed")
            self.assertEqual(statuses[second_id], "completed")
            self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
