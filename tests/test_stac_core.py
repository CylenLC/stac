import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import stac_core
import stac_api
from stac_identity import AssetIdentity


def write_part_metadata(part: Path, destination: Path, identity: AssetIdentity, *, etag: str, remote_total: int) -> None:
    digest = hashlib.sha256(part.read_bytes()).hexdigest()
    part.with_name(f"{part.name}.meta").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "identity": identity.digest(),
                "destination": str(destination.resolve()),
                "partial_size": part.stat().st_size,
                "partial_sha256": digest,
                "remote_total": remote_total,
                "etag": etag,
                "last_modified": None,
            }
        )
    )


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
    def test_acquisition_manager_is_application_singleton(self):
        sentinel = object()
        original = getattr(stac_api.app.state, "acquisition_manager", sentinel)
        if original is not sentinel:
            delattr(stac_api.app.state, "acquisition_manager")
        try:
            with patch.object(stac_api, "AcquisitionManager") as constructor:
                manager = object()
                constructor.return_value = manager

                self.assertIs(stac_api.acquisition_manager(), manager)
                self.assertIs(stac_api.acquisition_manager(), manager)
                constructor.assert_called_once_with(stac_api.DOWNLOAD_DIR)
        finally:
            if hasattr(stac_api.app.state, "acquisition_manager"):
                delattr(stac_api.app.state, "acquisition_manager")
            if original is not sentinel:
                stac_api.app.state.acquisition_manager = original

    def test_task_list_reconciles_on_each_call(self):
        class Manager:
            calls = 0

            def list_runs(self, limit):
                self.calls += 1
                return {"items": []}

        manager = Manager()
        stac_api.invalidate_task_list_cache()
        try:
            with patch.object(stac_api, "acquisition_manager", return_value=manager):
                self.assertEqual(stac_api.list_tasks(), [])
                self.assertEqual(stac_api.list_tasks(), [])
            self.assertEqual(manager.calls, 2)
        finally:
            stac_api.invalidate_task_list_cache()

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
            self.assertTrue(path.is_relative_to(Path(directory).resolve()))
            self.assertIn("__", path.parts[-1])
            self.assertIn("__", path.parts[-2])

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
        response = FakeResponse(
            chunks=(b"two",),
            status_code=206,
            headers={"ETag": '"v1"', "Content-Length": "3", "Content-Range": "bytes 3-5/6"},
        )
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            identity = AssetIdentity("test", "collection", "item", "data")
            part = destination.with_name("asset.tif.part")
            part.write_bytes(b"one")
            write_part_metadata(part, destination, identity, etag='"v1"', remote_total=6)
            self.assertEqual(
                stac_core.download_asset(
                    session,
                    "https://example/asset",
                    destination,
                    asset_identity=identity,
                ),
                3,
            )
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
            self.assertFalse(destination.with_name("asset.tif.part").exists())

if __name__ == "__main__":
    unittest.main()
