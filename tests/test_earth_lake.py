import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow.parquet as pq
import pystac
import rasterio
from rasterio.transform import from_origin

from earth_lake import EarthLake, REGISTRY_SCHEMAS, sha256_file
from protocol_commit import ProtocolCommit, _atomic_json


class EarthLakeTests(unittest.TestCase):
    def test_protocol_recovery_ignores_macos_appledouble_sidecars(self):
        with tempfile.TemporaryDirectory() as directory:
            journal_root = Path(directory) / "manifests" / "protocol_commits"
            journal_root.mkdir(parents=True)
            (journal_root / "._broken.json").write_bytes(b"AppleDouble\x00\xb0")

            self.assertEqual(ProtocolCommit.recover(directory), [])

    def test_initialize_and_record_hls_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "test": True})
            item = {
                "id": "granule-1",
                "collection": "HLSL30_V2.0",
                "bbox": [-101.0, 39.0, -99.0, 41.0],
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-101.0, 39.0], [-99.0, 39.0], [-99.0, 41.0], [-101.0, 41.0], [-101.0, 39.0]]],
                },
                "properties": {"datetime": "2024-01-01T12:00:00Z"},
            }
            asset_path = lake.source_item_directory("nasa", item["collection"], item["id"]) / "scene.B04.tif"
            asset_path.parent.mkdir(parents=True)
            with rasterio.open(
                asset_path,
                "w",
                driver="GTiff",
                width=2,
                height=3,
                count=1,
                dtype="int16",
                crs="EPSG:32619",
                transform=from_origin(500000, 5500000, 30, 30),
                nodata=-9999,
            ) as dataset:
                dataset.write(np.array([[1, 2], [3, 4], [5, 6]], dtype="int16"), 1)

            asset_id = lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=asset_path,
                status="downloaded",
            )
            lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=asset_path,
                status="skipped",
            )
            lake.finish_run(run_id, "completed", [asset_id])

            self.assertTrue((lake.protocol_dir / "earth_zarr_protocol.json").exists())
            self.assertEqual(len(pq.read_table(lake.registry_dir / "assets.parquet")), 1)
            asset = pq.read_table(lake.registry_dir / "assets.parquet").to_pylist()[0]
            self.assertEqual(asset["checksum_sha256"], sha256_file(asset_path))
            self.assertEqual(asset["local_path"], "source/nasa/HLSL30_V2.0/granule-1/scene.B04.tif")
            self.assertEqual(asset["run_id"], run_id)
            self.assertEqual(asset["status"], "downloaded")

            variable = pq.read_table(lake.registry_dir / "variables.parquet").to_pylist()[0]
            self.assertEqual(variable["canonical_name"], "surface_reflectance_red")
            self.assertEqual(variable["unit"], "1")
            self.assertEqual(variable["central_wavelength_nm"], 655.0)
            self.assertEqual(variable["nodata"], "-9999.0")
            grid = pq.read_table(lake.registry_dir / "grids.parquet").to_pylist()[0]
            self.assertEqual(grid["epsg"], 32619)
            self.assertEqual(grid["x_origin"], 500000.0)
            self.assertEqual(grid["y_origin"], 5500000.0)
            self.assertEqual(grid["pixel_size_x"], 30.0)
            self.assertEqual(grid["pixel_size_y"], -30.0)

            product = pq.read_table(lake.registry_dir / "products.parquet").to_pylist()[0]
            self.assertEqual(product["title"], "Harmonized Landsat Sentinel-2 L30 Version 2.0")
            self.assertIn("hlsl30v002", product["documentation_urls_json"])

            run = pq.read_table(lake.registry_dir / "processing_runs.parquet").to_pylist()[0]
            self.assertEqual(run["status"], "completed")
            self.assertEqual(json.loads(run["output_asset_ids"]), [asset_id])

            catalog = pystac.Catalog.from_file(str(lake.stac_dir / "catalog.json"))
            collection = next(value for value in catalog.get_collections() if value.id == "nasa__hlsl30_v2.0")
            stac_item = next(collection.get_items("granule-1"))
            self.assertIn("B04", stac_item.assets)
            self.assertEqual(stac_item.assets["B04"].extra_fields["earthzarr:asset_id"], asset_id)
            self.assertEqual(stac_item.assets["B04"].extra_fields["earthzarr:integrity_status"], "passed")

    def test_processing_registry_first_publication_is_protocol_managed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = EarthLake(root)
            processing_path = lake.registry_dir / "processing_runs.parquet"
            self.assertFalse(processing_path.exists())

            for _ in range(3):
                lake.reconcile_processing_run("acq-first-publication", "completed", [])

            rows = lake._read_rows("processing_runs")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "acq-first-publication")
            self.assertEqual(rows[0]["status"], "completed")
            self.assertTrue(processing_path.exists())
            journals = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in (root / "manifests" / "protocol_commits").glob("*.json")
            ]
            self.assertTrue(any(payload.get("kind") == "processing_run_start" for payload in journals))
            self.assertTrue(any(payload.get("kind") == "processing_run_finish" for payload in journals))

    def test_processing_first_publication_recovers_after_activation_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = EarthLake(root)
            processing_path = lake.registry_dir / "processing_runs.parquet"

            with patch.object(ProtocolCommit, "_roll_forward", side_effect=RuntimeError("activation failed")):
                with self.assertRaisesRegex(RuntimeError, "activation failed"):
                    lake.reconcile_processing_run("acq-crash-first-publication", "completed", [])

            self.assertFalse(processing_path.exists())
            recovered = EarthLake(root)
            recovered.reconcile_processing_run("acq-crash-first-publication", "completed", [])
            row = recovered.processing_run("acq-crash-first-publication")
            self.assertEqual(row["status"], "completed")
            self.assertEqual(len(recovered._read_rows("processing_runs")), 1)

    def test_acquisition_processing_association_mismatch_fails_before_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = EarthLake(root)
            lake.start_run(
                {"interface": "acquisition", "acquisition_run_id": "A"},
                run_id="acq-A",
            )
            row = lake.processing_run("acq-A")
            row["parameters_json"] = json.dumps(
                {"interface": "acquisition", "acquisition_run_id": "B"},
                sort_keys=True,
            )
            lake._write_table(lake.registry_dir / "processing_runs.parquet", [row], REGISTRY_SCHEMAS["processing_runs"])

            for operation in (
                lambda: lake.start_run(
                    {"interface": "acquisition", "acquisition_run_id": "A"},
                    run_id="acq-A",
                ),
                lambda: lake.finish_run("acq-A", "completed", []),
                lambda: lake.reconcile_processing_run("acq-A", "completed", []),
            ):
                with self.assertRaisesRegex(ValueError, "not associated"):
                    operation()

            unchanged = EarthLake(root).processing_run("acq-A")
            self.assertEqual(unchanged["parameters_json"], row["parameters_json"])
            self.assertEqual(unchanged["status"], "running")

    def test_acquisition_processing_association_malformed_parameters_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = EarthLake(root)
            lake.start_run(
                {"interface": "acquisition", "acquisition_run_id": "A"},
                run_id="acq-A",
            )
            for raw_parameters in ("{}", "not-json", json.dumps([]), json.dumps({"acquisition_run_id": 1})):
                row = lake.processing_run("acq-A")
                row["parameters_json"] = raw_parameters
                lake._write_table(
                    lake.registry_dir / "processing_runs.parquet",
                    [row],
                    REGISTRY_SCHEMAS["processing_runs"],
                )
                with self.assertRaises(ValueError):
                    lake.reconcile_processing_run("acq-A", "completed", [])

    def test_corrupt_processing_registry_is_not_treated_as_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = EarthLake(root)
            processing_path = lake.registry_dir / "processing_runs.parquet"
            processing_path.write_bytes(b"corrupt processing registry")

            with self.assertRaises(Exception):
                EarthLake(root)

            self.assertEqual(processing_path.read_bytes(), b"corrupt processing registry")

    def test_hls_fmask_uses_native_quality_definition(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "test": True})
            item = {"id": "granule-1", "collection": "HLSL30_V2.0", "properties": {}}
            asset_path = lake.source_item_directory("nasa", item["collection"], item["id"]) / "scene.Fmask.tif"
            asset_path.parent.mkdir(parents=True)
            with rasterio.open(
                asset_path,
                "w",
                driver="GTiff",
                width=1,
                height=1,
                count=1,
                dtype="uint8",
                crs="EPSG:32619",
                transform=from_origin(500000, 5500000, 30, 30),
                nodata=255,
            ) as dataset:
                dataset.write(np.array([[0]], dtype="uint8"), 1)
                dataset.update_tags(**{"Fmask bit description": "cloud and water bits"})
            lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="Fmask",
                source_url="https://example/scene.Fmask.tif",
                local_path=asset_path,
                status="downloaded",
            )

            variable = pq.read_table(lake.registry_dir / "variables.parquet").to_pylist()[0]
            self.assertIsNone(variable["unit"])
            self.assertEqual(variable["nodata"], "255.0")
            self.assertEqual(variable["quality_flag_definition"], "cloud and water bits")

    def test_local_stac_preserves_resolved_category_for_explicit_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            item = {
                "id": "preview-item",
                "collection": "generic",
                "properties": {},
                "assets": {
                    "preview": {
                        "href": "https://example/preview.json",
                        "type": "application/json",
                    }
                },
            }
            path = lake.source_item_directory("test", "generic", "preview-item") / "preview.json"
            path.parent.mkdir(parents=True)
            path.write_text('{"preview": true}', encoding="utf-8")

            lake.record_asset(
                run_id="run",
                catalog="test",
                item=item,
                asset_key="preview",
                source_url=item["assets"]["preview"]["href"],
                local_path=path,
                status="downloaded",
            )

            catalog = pystac.Catalog.from_file(str(lake.stac_dir / "catalog.json"))
            collection = next(value for value in catalog.get_collections() if value.id == "test__generic")
            stac_item = next(collection.get_items("preview-item"))
            local_asset = stac_item.assets["preview"]
            self.assertEqual(local_asset.roles, ["thumbnail"])
            self.assertEqual(local_asset.extra_fields["earthzarr:resolved_category"], "thumbnail")

    def test_record_asset_publishes_registries_and_stac_as_one_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"interface": "test"})
            source = lake.source_item_directory("nasa", "HLSL30_V2.0", "atomic") / "scene.B04.bin"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"atomic-raster")

            asset_id = lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item={"id": "atomic", "collection": "HLSL30_V2.0", "properties": {}},
                asset_key="B04",
                source_url="https://example/scene.B04.bin",
                local_path=source,
                status="downloaded",
            )

            self.assertEqual(lake._find("assets", asset_id)["asset_id"], asset_id)
            journals = sorted((Path(directory) / "manifests" / "protocol_commits").glob("*.json"))
            payload = next(
                json.loads(path.read_text(encoding="utf-8"))
                for path in journals
                if json.loads(path.read_text(encoding="utf-8")).get("kind") == "record_asset"
            )
            self.assertEqual(payload["status"], "committed")
            self.assertIn("registry/assets.parquet", payload["outputs"])
            self.assertIn("catalog/stac/catalog.json", payload["outputs"])

    def test_failed_protocol_commit_does_not_publish_partial_registry_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"interface": "test"})
            source = lake.source_item_directory("nasa", "HLSL30_V2.0", "failed") / "scene.B04.bin"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"failed-raster")
            before = lake._read_rows("assets")

            with patch.object(lake, "_update_stac", side_effect=RuntimeError("stac failed")):
                with self.assertRaisesRegex(RuntimeError, "stac failed"):
                    lake.record_asset(
                        run_id=run_id,
                        catalog="nasa",
                        item={"id": "failed", "collection": "HLSL30_V2.0", "properties": {}},
                        asset_key="B04",
                        source_url="https://example/scene.B04.bin",
                        local_path=source,
                        status="downloaded",
                    )

            self.assertEqual(lake._read_rows("assets"), before)
            journals = sorted((Path(directory) / "manifests" / "protocol_commits").glob("*.json"))
            payload = next(
                json.loads(path.read_text(encoding="utf-8"))
                for path in journals
                if json.loads(path.read_text(encoding="utf-8")).get("kind") == "record_asset"
            )
            self.assertEqual(payload["status"], "aborted")

    def test_protocol_commit_recovery_rolls_forward_publishing_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commit = ProtocolCommit(root, kind="test")
            commit.__enter__()
            staged = commit.staged_path(root / "protocol" / "recovered.json")
            staged.write_text('{"ok":true}', encoding="utf-8")
            commit.payload.update(status="publishing", outputs=["protocol/recovered.json"])
            _atomic_json(commit.journal, commit.payload)

            recovered = ProtocolCommit.recover(root)

            self.assertEqual(recovered, [commit.commit_id])
            self.assertEqual((root / "protocol" / "recovered.json").read_text(), '{"ok":true}')

    def test_protocol_commit_staging_is_durable_and_missing_output_stays_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commit = ProtocolCommit(root, kind="test")
            commit.__enter__()
            staged = commit.staged_path(root / "protocol" / "missing.json")
            staged.write_text('{"ok":true}', encoding="utf-8")
            commit.payload.update(status="publishing", outputs=["protocol/missing.json"])
            _atomic_json(commit.journal, commit.payload)

            self.assertTrue(commit.stage.is_relative_to((root / "manifests").resolve()))
            shutil.rmtree(commit.stage)
            with self.assertRaises(FileNotFoundError):
                ProtocolCommit.recover(root)

            payload = json.loads(commit.journal.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "publishing")


if __name__ == "__main__":
    unittest.main()
