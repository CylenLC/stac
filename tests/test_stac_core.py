import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import stac_core
import stac_api


class FakeResponse:
    def __init__(self, payload=None, chunks=(), status_error=None):
        self.payload = payload or {}
        self.chunks = chunks
        self.status_error = status_error
        self.headers = {}

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

    def get(self, url, **kwargs):
        self.params = kwargs.get("params")
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

    def test_item_directory_sanitizes_user_controlled_components(self):
        with tempfile.TemporaryDirectory() as directory:
            path = stac_core.item_directory(directory, "../../outside", "../item")
            self.assertEqual(path, Path(directory).resolve() / "outside" / "item")

    def test_download_asset_writes_atomically(self):
        response = FakeResponse(chunks=(b"one", b"two"))
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            self.assertEqual(stac_core.download_asset(session, "https://example/asset", destination), 6)
            self.assertEqual(destination.read_bytes(), b"onetwo")
            self.assertFalse((Path(directory) / "asset.tif.part").exists())

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


if __name__ == "__main__":
    unittest.main()
