import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from acquisition import AcquisitionManager, AcquisitionRequest
from earth_lake import EarthLake
from stac_core import download_asset_checked
from stac_identity import AssetIdentity
from stac_integrity import IntegrityError, IntegrityGate, TransferMetadata


class FakeResponse:
    def __init__(self, chunks=(), *, status_code=200, headers=None, status_error=None):
        self.chunks = tuple(chunks)
        self.status_code = status_code
        self.headers = headers or {}
        self.status_error = status_error

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

    def get(self, url, **kwargs):
        return self.response


class SequenceFakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class InterruptingResponse(FakeResponse):
    def __init__(self, prefix: bytes, error: Exception, **kwargs):
        super().__init__((prefix,), **kwargs)
        self.error = error

    def iter_content(self, chunk_size):
        yield from self.chunks
        raise self.error


def geotiff_bytes() -> bytes:
    with MemoryFile() as memory:
        with memory.open(
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(0, 2, 1, 1),
        ) as dataset:
            dataset.write(np.array([[1, 2], [3, 4]], dtype="uint8"), 1)
        return memory.read()


def write_geotiff(path: Path) -> None:
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


def write_resumable_meta(part: Path, destination: Path, identity: AssetIdentity, *, etag: str, remote_total: int) -> None:
    part.with_name(f"{part.name}.meta").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "identity": identity.digest(),
                "destination": str(destination.resolve()),
                "partial_size": part.stat().st_size,
                "partial_sha256": hashlib.sha256(part.read_bytes()).hexdigest(),
                "remote_total": remote_total,
                "etag": etag,
                "last_modified": None,
            }
        )
    )


class IntegrityGateTests(unittest.TestCase):
    def test_valid_binary_download_passes_and_publishes_atomically(self):
        payload = b"valid-binary"
        response = FakeResponse(
            (payload,),
            headers={"Content-Length": str(len(payload)), "Content-Type": "application/octet-stream"},
        )
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            result = download_asset_checked(FakeSession(response), "https://example/asset.bin", destination)

            self.assertTrue(result.ok)
            self.assertTrue(destination.is_file())
            self.assertFalse(destination.with_name("asset.bin.part").exists())

    def test_content_length_mismatch_fails_before_publication(self):
        response = FakeResponse((b"abc",), headers={"Content-Length": "4"})
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"

            with self.assertRaisesRegex(IntegrityError, "content_length_mismatch"):
                download_asset_checked(FakeSession(response), "https://example/asset.bin", destination)

            self.assertFalse(destination.exists())
            self.assertFalse(destination.with_name("asset.bin.part").exists())

    def test_zero_byte_scientific_download_fails(self):
        response = FakeResponse((b"",), headers={"Content-Length": "0", "Content-Type": "image/tiff"})
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "B04.tif"

            with self.assertRaisesRegex(IntegrityError, "zero_byte_file"):
                download_asset_checked(
                    FakeSession(response),
                    "https://example/B04.tif",
                    destination,
                    asset_metadata={"href": "https://example/B04.tif", "type": "image/tiff"},
                )

            self.assertFalse(destination.exists())

    def test_html_and_json_error_documents_are_rejected_for_tiff(self):
        for content_type, payload in (
            ("text/html", b"<html>login required</html>"),
            ("application/json", b'{"error":"unauthorized"}'),
        ):
            with self.subTest(content_type=content_type), tempfile.TemporaryDirectory() as directory:
                destination = Path(directory) / "B04.tif"
                response = FakeResponse(
                    (payload,),
                    headers={"Content-Length": str(len(payload)), "Content-Type": content_type},
                )

                with self.assertRaises(IntegrityError):
                    download_asset_checked(
                        FakeSession(response),
                        "https://example/B04.tif",
                        destination,
                        asset_metadata={"href": "https://example/B04.tif", "type": "image/tiff"},
                    )
                self.assertFalse(destination.exists())

    def test_invalid_and_valid_geotiff(self):
        with tempfile.TemporaryDirectory() as directory:
            invalid = Path(directory) / "invalid.tif"
            invalid.write_bytes(b"not-a-tiff")
            result = IntegrityGate.validate(invalid, {"href": invalid.as_uri(), "type": "image/tiff"})
            self.assertFalse(result.ok)
            self.assertEqual(result.reason_code, "invalid_geotiff")

            payload = geotiff_bytes()
            response = FakeResponse(
                (payload,),
                headers={"Content-Length": str(len(payload)), "Content-Type": "image/tiff"},
            )
            valid = Path(directory) / "valid.tif"
            result = download_asset_checked(
                FakeSession(response),
                "https://example/valid.tif",
                valid,
                asset_metadata={"href": "https://example/valid.tif", "type": "image/tiff"},
            )
            self.assertTrue(result.ok)
            self.assertEqual(result.detected_type, "GTiff")

    def test_existing_cache_size_and_structure_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            valid = Path(directory) / "valid.tif"
            write_geotiff(valid)
            asset = {"href": "https://example/valid.tif", "type": "image/tiff"}
            self.assertTrue(IntegrityGate.validate(valid, asset).ok)

            corrupt = Path(directory) / "corrupt.tif"
            corrupt.write_bytes(b"")
            result = IntegrityGate.validate(corrupt, {"href": "https://example/corrupt.tif", "type": "image/tiff"})
            self.assertFalse(result.ok)
            self.assertEqual(result.reason_code, "zero_byte_file")

    def test_source_size_and_checksum_are_checked(self):
        payload = b"source-payload"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "asset.bin"
            path.write_bytes(payload)
            checksum = hashlib.sha256(payload).hexdigest()
            asset = {
                "href": "https://example/asset.bin",
                "file:size": len(payload),
                "checksum:sha256": checksum,
            }
            result = IntegrityGate.validate(path, asset)
            self.assertTrue(result.ok)
            self.assertTrue(result.checksum_verified)
            self.assertEqual(result.local_sha256, checksum)

            mismatch = IntegrityGate.validate(path, {**asset, "file:size": len(payload) + 1})
            self.assertFalse(mismatch.ok)
            self.assertEqual(mismatch.reason_code, "source_size_mismatch")

            mismatch = IntegrityGate.validate(path, {**asset, "checksum:sha256": "0" * 64})
            self.assertFalse(mismatch.ok)
            self.assertEqual(mismatch.reason_code, "source_checksum_mismatch")

    def test_explicit_json_metadata_is_allowed(self):
        payload = b'{"id":"item","ok":true}'
        response = FakeResponse(
            (payload,),
            headers={"Content-Length": str(len(payload)), "Content-Type": "application/json"},
        )
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "metadata.json"
            result = download_asset_checked(
                FakeSession(response),
                "https://example/metadata.json",
                destination,
                asset_metadata={"href": "https://example/metadata.json", "type": "application/json"},
            )
            self.assertTrue(result.ok)
            self.assertEqual(result.detected_type, "JSON")

    def test_invalid_partial_is_removed_and_not_appended(self):
        payload = geotiff_bytes()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.tif"
            destination.with_name("asset.tif.part").write_bytes(b"corrupt-prefix")
            response = FakeResponse(
                (payload,),
                status_code=206,
                headers={"Content-Length": str(len(payload)), "Content-Type": "image/tiff"},
            )

            with self.assertRaises(IntegrityError):
                download_asset_checked(
                    FakeSession(response),
                    "https://example/asset.tif",
                    destination,
                    asset_metadata={"href": "https://example/asset.tif", "type": "image/tiff"},
                )
            self.assertFalse(destination.exists())
            self.assertFalse(destination.with_name("asset.tif.part").exists())

    def test_valid_range_resume_requires_saved_validator_and_matching_range(self):
        response = FakeResponse(
            (b"two",),
            status_code=206,
            headers={
                "Content-Length": "3",
                "Content-Range": "bytes 3-5/6",
                "ETag": '"v1"',
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            part = destination.with_name("asset.bin.part")
            part.write_bytes(b"one")
            identity = AssetIdentity("test", "collection", "item", "data")
            write_resumable_meta(part, destination, identity, etag='"v1"', remote_total=6)

            result = download_asset_checked(
                FakeSession(response),
                "https://example/asset.bin",
                destination,
                asset_identity=identity,
            )

            self.assertTrue(result.ok)
            self.assertEqual(destination.read_bytes(), b"onetwo")

    def test_interrupted_transfer_persists_bound_partial_metadata_and_resumes(self):
        identity = AssetIdentity("test", "collection", "item", "data")
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            first = InterruptingResponse(
                b"one",
                OSError("interrupted"),
                headers={"Content-Length": "6", "ETag": '"v1"'},
            )
            with self.assertRaisesRegex(OSError, "interrupted"):
                download_asset_checked(
                    FakeSession(first),
                    "https://example/asset.bin",
                    destination,
                    asset_identity=identity,
                )

            part = destination.with_name("asset.bin.part")
            metadata = json.loads(part.with_name("asset.bin.part.meta").read_text())
            self.assertEqual(metadata["identity"], identity.digest())
            self.assertEqual(metadata["destination"], str(destination.resolve()))
            self.assertEqual(metadata["partial_size"], 3)
            self.assertEqual(metadata["partial_sha256"], hashlib.sha256(b"one").hexdigest())

            response = FakeResponse(
                (b"two",),
                status_code=206,
                headers={"Content-Length": "3", "Content-Range": "bytes 3-5/6", "ETag": '"v1"'},
            )
            result = download_asset_checked(
                FakeSession(response),
                "https://example/asset.bin",
                destination,
                asset_identity=identity,
            )
            self.assertTrue(result.ok)
            self.assertEqual(destination.read_bytes(), b"onetwo")
            self.assertFalse(part.with_name("asset.bin.part.meta").exists())

    def test_partial_metadata_digest_mismatch_restarts_without_append(self):
        identity = AssetIdentity("test", "collection", "item", "data")
        payload = b"fresh-object"
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            part = destination.with_name("asset.bin.part")
            part.write_bytes(b"one")
            write_resumable_meta(part, destination, identity, etag='"v1"', remote_total=6)
            metadata_path = part.with_name("asset.bin.part.meta")
            metadata = json.loads(metadata_path.read_text())
            metadata["partial_sha256"] = hashlib.sha256(b"different").hexdigest()
            metadata_path.write_text(json.dumps(metadata))
            session = SequenceFakeSession(
                FakeResponse((payload,), headers={"Content-Length": str(len(payload)), "ETag": '"v2"'})
            )

            result = download_asset_checked(
                session,
                "https://example/asset.bin",
                destination,
                asset_identity=identity,
            )

            self.assertTrue(result.ok)
            self.assertEqual(destination.read_bytes(), payload)
            self.assertEqual(session.calls[0]["headers"], {})

    def test_complete_valid_part_is_published_after_crash_before_rename(self):
        identity = AssetIdentity("test", "collection", "item", "B04")
        payload = geotiff_bytes()

        class NoNetworkSession:
            def get(self, url, **kwargs):
                raise AssertionError("complete validated part should publish without HTTP")

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "B04.tif"
            part = destination.with_name("B04.tif.part")
            part.write_bytes(payload)
            write_resumable_meta(part, destination, identity, etag='"v1"', remote_total=len(payload))

            result = download_asset_checked(
                NoNetworkSession(),
                "https://example/B04.tif",
                destination,
                asset_metadata={"href": "https://example/B04.tif", "type": "image/tiff"},
                asset_identity=identity,
            )

            self.assertTrue(result.ok)
            self.assertEqual(destination.read_bytes(), payload)
            self.assertFalse(part.exists())
            self.assertFalse(part.with_name("B04.tif.part.meta").exists())

    def test_initial_partial_response_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            response = FakeResponse(
                (b"payload",),
                status_code=206,
                headers={"Content-Length": "7", "Content-Range": "bytes 0-6/7"},
            )

            with self.assertRaisesRegex(IntegrityError, "unexpected_partial_response"):
                download_asset_checked(FakeSession(response), "https://example/asset.bin", destination)
            self.assertFalse(destination.exists())

    def test_range_resume_rejects_wrong_start_and_malformed_range(self):
        for content_range in ("bytes 2-4/6", "bytes 3-5/7", "not-a-range"):
            with self.subTest(content_range=content_range), tempfile.TemporaryDirectory() as directory:
                destination = Path(directory) / "asset.bin"
                part = destination.with_name("asset.bin.part")
                part.write_bytes(b"one")
                identity = AssetIdentity("test", "collection", "item", "data")
                write_resumable_meta(part, destination, identity, etag='"v1"', remote_total=6)
                response = FakeResponse(
                    (b"two",),
                    status_code=206,
                    headers={
                        "Content-Length": "3",
                        "Content-Range": content_range,
                        "ETag": '"v1"',
                    },
                )

                with self.assertRaises(IntegrityError):
                    download_asset_checked(
                        FakeSession(response),
                        "https://example/asset.bin",
                        destination,
                        asset_identity=identity,
                    )
                self.assertFalse(destination.exists())
                self.assertFalse(part.exists())

    def test_changed_etag_restarts_from_zero_instead_of_appending(self):
        first = FakeResponse(
            (b"new-tail",),
            status_code=206,
            headers={"Content-Length": "8", "Content-Range": "bytes 3-10/11", "ETag": '"v2"'},
        )
        payload = b"fresh-object"
        second = FakeResponse(
            (payload,),
            status_code=200,
            headers={"Content-Length": str(len(payload)), "ETag": '"v2"'},
        )
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            identity = AssetIdentity("test", "collection", "item", "data")
            part = destination.with_name("asset.bin.part")
            part.write_bytes(b"old")
            write_resumable_meta(part, destination, identity, etag='"v1"', remote_total=11)
            session = SequenceFakeSession(first, second)

            result = download_asset_checked(
                session,
                "https://example/asset.bin",
                destination,
                asset_identity=identity,
            )

            self.assertTrue(result.ok)
            self.assertEqual(destination.read_bytes(), payload)
            self.assertEqual(session.calls[0]["headers"]["If-Range"], '"v1"')
            self.assertEqual(session.calls[1]["headers"], {})

    def test_malformed_transport_and_source_size_metadata_fail_closed(self):
        payload = b"payload"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "asset.bin"
            path.write_bytes(payload)
            for content_length in ("abc", "-1"):
                response = TransferMetadata(
                    status_code=200,
                    headers={"Content-Length": content_length},
                    bytes_received=len(payload),
                )
                result = IntegrityGate.validate(path, {"href": path.as_uri()}, response=response)
                self.assertFalse(result.ok)
                self.assertEqual(result.reason_code, "invalid_content_length")

            for value in ("abc", -1, None):
                result = IntegrityGate.validate(path, {"href": path.as_uri(), "file:size": value})
                self.assertFalse(result.ok)
                self.assertEqual(result.reason_code, "invalid_source_size")

    def test_format_dispatch_does_not_route_netcdf_or_zarr_to_geotiff(self):
        import xarray as xr

        with tempfile.TemporaryDirectory() as directory:
            netcdf = Path(directory) / "sample.nc"
            xr.Dataset({"value": ("x", np.array([1, 2], dtype="int16"))}).to_netcdf(netcdf)
            netcdf_result = IntegrityGate.validate(
                netcdf,
                {"href": netcdf.as_uri(), "type": "application/x-netcdf"},
            )
            self.assertTrue(netcdf_result.ok)
            self.assertEqual(netcdf_result.detected_type, "NetCDF")
            self.assertNotEqual(netcdf_result.reason_code, "invalid_geotiff")

            zarr = Path(directory) / "sample.zarr"
            zarr.write_bytes(b"not-a-directory-zarr")
            zarr_result = IntegrityGate.validate(
                zarr,
                {"href": zarr.as_uri(), "type": "application/vnd+zarr"},
            )
            self.assertFalse(zarr_result.ok)
            self.assertEqual(zarr_result.reason_code, "unsupported_asset_format")

    def test_format_conflicts_and_unknown_scientific_formats_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.tif"
            path.write_bytes(geotiff_bytes())
            for asset, code in (
                ({"href": path.as_uri(), "type": "application/x-netcdf"}, "asset_format_conflict"),
                ({"href": path.as_uri(), "type": "application/x-hdf5"}, "unsupported_asset_format"),
            ):
                with self.subTest(asset=asset):
                    result = IntegrityGate.validate(path, asset)
                    self.assertFalse(result.ok)
                    self.assertEqual(result.reason_code, code)


class IntegrityAcquisitionTests(unittest.TestCase):
    def test_registry_rejects_unvalidated_corrupt_raster(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLS", "bad") / "B04.tif"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"not-a-raster")

            with self.assertRaisesRegex(IntegrityError, "invalid_geotiff"):
                lake.record_asset(
                    run_id="run",
                    catalog="nasa",
                    item={"id": "bad", "collection": "HLS", "properties": {}},
                    asset_key="B04",
                    source_url="https://example/B04.tif",
                    local_path=path,
                    status="downloaded",
                )
            self.assertEqual(lake._read_rows("assets"), [])

    def test_registry_revalidates_integrity_result_bound_to_changed_file(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLS", "changed") / "B04.tif"
            path.parent.mkdir(parents=True)
            write_geotiff(path)
            asset = {"href": "https://example/B04.tif", "type": "image/tiff"}
            result = IntegrityGate.validate(path, asset)
            self.assertTrue(result.ok)
            path.write_bytes(b"not-a-raster")

            with self.assertRaisesRegex(IntegrityError, "invalid_geotiff"):
                lake.record_asset(
                    run_id="run",
                    catalog="nasa",
                    item={"id": "changed", "collection": "HLS", "assets": {"B04": asset}, "properties": {}},
                    asset_key="B04",
                    source_url=asset["href"],
                    local_path=path,
                    status="downloaded",
                    integrity=result,
                )

    def test_registry_rejects_same_stat_stale_integrity_result(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLS", "same-stat") / "B04.tif"
            path.parent.mkdir(parents=True)
            write_geotiff(path)
            asset = {"href": "https://example/B04.tif", "type": "image/tiff"}
            result = IntegrityGate.validate(path, asset)
            self.assertTrue(result.ok)
            original_size = path.stat().st_size
            original_mtime = path.stat().st_mtime_ns

            replacement = Path(directory) / "replacement.tif"
            write_geotiff(replacement)
            with rasterio.open(replacement, "r+") as dataset:
                dataset.write(np.array([[2]], dtype="uint8"), 1)
            self.assertEqual(replacement.stat().st_size, original_size)
            replacement_bytes = replacement.read_bytes()
            self.assertNotEqual(hashlib.sha256(replacement_bytes).hexdigest(), result.local_sha256)
            path.write_bytes(replacement_bytes)
            os.utime(path, ns=(original_mtime, original_mtime))
            self.assertEqual(path.stat().st_size, original_size)
            self.assertEqual(path.stat().st_mtime_ns, original_mtime)

            asset_id = lake.record_asset(
                run_id="run",
                catalog="nasa",
                item={"id": "same-stat", "collection": "HLS", "assets": {"B04": asset}, "properties": {}},
                asset_key="B04",
                source_url=asset["href"],
                local_path=path,
                status="downloaded",
                integrity=result,
            )

            registered = lake._find("assets", asset_id)
            self.assertEqual(registered["checksum_sha256"], hashlib.sha256(replacement_bytes).hexdigest())
            self.assertNotEqual(registered["checksum_sha256"], result.local_sha256)

    def test_integrity_failure_does_not_publish_registry_or_complete_attempt(self):
        def fetch_page(request, cursor, page_size):
            return ([{
                "id": "invalid",
                "collection": "HLS",
                "properties": {},
                "assets": {"B04": {"href": "https://example/B04.tif", "type": "image/tiff"}},
            }], None)

        def transfer(session, url, destination, on_chunk):
            destination.write_bytes(b"not-a-raster")
            on_chunk(12)

        with tempfile.TemporaryDirectory() as directory:
            manager = AcquisitionManager(directory, page_fetcher=fetch_page, transfer=transfer)
            request = AcquisitionRequest(
                catalog="nasa", collections=["HLS"], wkt="POINT (0 0)",
                start_date="2024-01-01", end_date="2024-01-02", only_main=False,
            )
            run_id = manager.create_run(request, "integrity-failure")

            result = manager.run(run_id)

            self.assertEqual(result["status"], "failed")
            attempt = manager.store.attempts_for_batch(run_id, 1)[0]
            self.assertEqual(attempt["status"], "failed")
            self.assertIn("invalid_geotiff", attempt["error"])
            assets = manager.store.root / "registry" / "assets.parquet"
            self.assertEqual(len(pq.read_table(assets)), 0)
            self.assertFalse(Path(attempt["destination"] + ".part").exists())


if __name__ == "__main__":
    unittest.main()
