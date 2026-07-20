import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

import stac_core
import stac_api


class FakeResponse:
    def __init__(self, payload=None, chunks=(), status_error=None, status_code=200, headers=None):
        self.payload = payload or {}
        self.chunks = chunks
        self.status_error = status_error
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def iter_content(self, chunk_size):
        return iter(self.chunks)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.params = None
        self.get_kwargs = None

    def get(self, url, **kwargs):
        self.params = kwargs.get("params")
        self.get_kwargs = kwargs
        return self.response

    def head(self, url, **kwargs):
        return self.response


class StacCoreTests(unittest.TestCase):
    def test_hls_alias_returns_all_data_assets(self):
        payload = {
            "feed": {
                "entry": [
                    {
                        "id": "granule-1",
                        "time_start": "2024-01-01T00:00:00Z",
                        "links": [
                            {"rel": "https://example/data#", "href": "https://example/HLS.B04.tif"},
                            {"rel": "https://example/data#", "href": "https://example/HLS.B08.tif"},
                            {"rel": "https://example/browse#", "href": "https://example/HLS.jpg"},
                        ],
                    }
                ]
            }
        }
        session = FakeSession(FakeResponse(payload))
        with patch("stac_core.http_session", return_value=session):
            items = stac_core.search_items(
                "nasa",
                "POINT (-100 40)",
                ["HLSL30_V2.0"],
                "2024-01-01",
                "2024-01-02",
                1,
            )

        self.assertEqual(session.params["short_name"], "HLSL30")
        self.assertEqual(session.params["version"], "2.0")
        self.assertEqual(set(items[0]["assets"]), {"B04", "B08"})

    def test_nasa_collection_metadata_uses_hls_alias_and_version(self):
        payload = {
            "feed": {
                "entry": [
                    {
                        "title": "HLS L30",
                        "summary": "Collection summary",
                        "time_start": "2013-01-01T00:00:00Z",
                    }
                ]
            }
        }
        session = FakeSession(FakeResponse(payload))
        with patch("stac_core.http_session", return_value=session):
            metadata = stac_core.get_collection_metadata("nasa", "HLSL30_V2.0")

        self.assertEqual(session.params["short_name"], "HLSL30")
        self.assertEqual(session.params["version"], "2.0")
        self.assertEqual(metadata["title"], "HLS L30")

    def test_nasa_page_cursor_advances_collection_and_page_number(self):
        session = FakeSession(FakeResponse({"feed": {"entry": []}}))
        with patch("stac_core.http_session", return_value=session):
            items, cursor = stac_core.search_items_page(
                "nasa", "POINT (-100 40)", ["HLSL30_V2.0", "HLSS30_V2.0"],
                "2024-01-01", "2024-01-02", None, 100,
            )
        self.assertEqual(items, [])
        self.assertEqual(session.params["page_num"], 1)
        self.assertEqual(session.params["page_size"], 100)
        self.assertEqual(__import__("json").loads(cursor), {"collection_index": 1, "page_num": 1})

    def test_item_directory_sanitizes_user_controlled_components(self):
        with tempfile.TemporaryDirectory() as directory:
            path = stac_core.item_directory(directory, "../../outside", "../item")
            self.assertEqual(path, Path(directory).resolve() / "outside" / "item")

    def test_hls_main_assets_use_sensor_specific_nir_band(self):
        l30 = {
            "collection": "HLSL30_V2.0",
            "assets": {key: {"href": f"https://example/{key}.tif"} for key in ("B04", "B05", "B08", "Fmask")},
        }
        s30 = {
            "collection": "HLSS30_V2.0",
            "assets": {key: {"href": f"https://example/{key}.tif"} for key in ("B04", "B05", "B8A", "Fmask")},
        }
        self.assertEqual({key for key, _ in stac_core.selected_assets(l30, True)}, {"B04", "B05", "Fmask"})
        self.assertEqual({key for key, _ in stac_core.selected_assets(s30, True)}, {"B04", "B8A", "Fmask"})

    def test_download_asset_writes_atomically(self):
        response = FakeResponse(chunks=(b"one", b"two"))
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            self.assertEqual(stac_core.download_asset(session, "https://example/asset", destination), 6)
            self.assertEqual(destination.read_bytes(), b"onetwo")
            self.assertFalse((Path(directory) / "asset.tif.part").exists())

    def test_download_asset_resumes_part_file_with_range(self):
        response = FakeResponse(chunks=(b"two",), status_code=206, headers={"ETag": '"v1"'})
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            destination.with_name("asset.tif.part").write_bytes(b"one")
            self.assertEqual(stac_core.download_asset(session, "https://example/asset", destination), 3)
            self.assertEqual(session.get_kwargs["headers"]["Range"], "bytes=3-")
            self.assertEqual(destination.read_bytes(), b"onetwo")

    def test_download_asset_preserves_part_file_after_failure(self):
        response = FakeResponse(chunks=(b"one",), status_error=OSError("interrupted"))
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            destination.with_name("asset.tif.part").write_bytes(b"partial")
            with self.assertRaises(OSError):
                stac_core.download_asset(session, "https://example/asset", destination)
            self.assertEqual(destination.with_name("asset.tif.part").read_bytes(), b"partial")

    def test_api_marks_all_download_failures_as_failed(self):
        task_id = "test-task"
        stac_api.TASKS[task_id] = {
            "status": "pending",
            "progress": 0.0,
            "message": "queued",
            "results": [],
            "skipped": [],
            "failures": [],
        }
        items = [{"id": "item", "collection": "collection", "assets": {"data": {"href": "https://example/data.tif"}}}]
        with tempfile.TemporaryDirectory() as directory:
            response = FakeResponse(status_error=None)
            with (
                patch.object(stac_api, "DOWNLOAD_DIR", directory),
                patch("stac_api.http_session", return_value=FakeSession(response)),
                patch("stac_api.download_asset", side_effect=OSError("network interrupted")),
                patch.object(stac_api.logger, "exception"),
            ):
                stac_api.download_worker(task_id, "nasa", items, only_main=False)

        self.assertEqual(stac_api.TASKS[task_id]["status"], "failed")
        self.assertEqual(len(stac_api.TASKS[task_id]["failures"]), 1)
        del stac_api.TASKS[task_id]

    def test_api_success_updates_earth_lake_registry(self):
        task_id = "successful-task"
        stac_api.TASKS[task_id] = {
            "status": "pending",
            "progress": 0.0,
            "message": "queued",
            "results": [],
            "skipped": [],
            "failures": [],
        }
        item = {
            "id": "granule-1",
            "collection": "HLSL30_V2.0",
            "bbox": [-101.0, 39.0, -99.0, 41.0],
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-101.0, 39.0], [-99.0, 39.0], [-99.0, 41.0], [-101.0, 41.0], [-101.0, 39.0]]],
            },
            "properties": {"datetime": "2024-01-01T12:00:00Z"},
            "assets": {"B04": {"href": "https://example/scene.B04.tif", "roles": ["data"]}},
        }

        def successful_download(session, url, destination, on_chunk):
            destination.write_bytes(b"downloaded-data")
            on_chunk(len(b"downloaded-data"))
            return len(b"downloaded-data")

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(stac_api, "DOWNLOAD_DIR", directory),
                patch("stac_api.http_session", return_value=FakeSession(FakeResponse())),
                patch("stac_api.download_asset", side_effect=successful_download),
            ):
                asset_ids = stac_api.download_worker(task_id, "nasa", [item], only_main=False)

            self.assertEqual(stac_api.TASKS[task_id]["status"], "completed")
            self.assertEqual(stac_api.TASKS[task_id]["total_files"], 1)
            self.assertEqual(stac_api.TASKS[task_id]["completed_files"], 1)
            self.assertIsNone(stac_api.TASKS[task_id]["current_file"])
            self.assertEqual(len(asset_ids), 1)
            assets = pq.read_table(Path(directory) / "registry" / "assets.parquet").to_pylist()
            self.assertEqual(len(assets), 1)
            self.assertEqual(assets[0]["asset_key"], "B04")
            self.assertTrue((Path(directory) / "catalog" / "stac" / "catalog.json").exists())
        del stac_api.TASKS[task_id]


if __name__ == "__main__":
    unittest.main()
