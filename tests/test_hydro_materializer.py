import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from hydro_materializer import HydroMaterializer
from protocol_commit import ProtocolCommit


class HydroMaterializerTests(unittest.TestCase):
    def test_interruption_publishes_manifest_for_completed_outputs(self):
        class PauseMaterialization(BaseException):
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            dataset = data_root / "camels_test"
            dataset.mkdir(parents=True)
            (dataset / "first_attributes.csv").write_text("basin_id,value\n001,1\n", encoding="utf-8")
            (dataset / "second_attributes.csv").write_text("basin_id,value\n002,2\n", encoding="utf-8")
            checks = 0

            def check_control():
                nonlocal checks
                checks += 1
                if checks == 2:
                    raise PauseMaterialization()

            materializer = HydroMaterializer(
                data_root,
                root / "lake",
                check_control=check_control,
            )

            with self.assertRaises(PauseMaterialization):
                materializer.materialize(kinds={"entities"})

            manifest = json.loads(materializer.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(manifest["records"]), 1)

    def test_materializes_attributes_and_reuses_existing_zarr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            lake_root = root / "lake"
            zarr_root = root / "published-zarr"

            dataset = data_root / "CAMELS_TEST"
            dataset.mkdir(parents=True)
            attributes = dataset / "catchment_attributes.csv"
            attributes.write_text("gauge_id,area_km2,name\n001,12.5,Alpha\n002,20.0,Beta\n", encoding="utf-8")

            source_store = zarr_root / "camels_test_timeseries.zarr"
            (source_store / "streamflow" / "c").mkdir(parents=True)
            (source_store / "zarr.json").write_text('{"zarr_format": 3, "node_type": "group"}', encoding="utf-8")
            source_chunk = source_store / "streamflow" / "c" / "0"
            source_chunk.write_bytes(b"zarr chunk")

            materializer = HydroMaterializer(data_root, lake_root, existing_zarr_root=zarr_root)
            first = materializer.materialize(kinds={"entities", "arrays"})

            self.assertEqual(first["materialized"], 2)
            self.assertEqual(first["failed"], 0)
            attribute_outputs = list((lake_root / "entities" / "basins" / "camels_test").rglob("*.parquet"))
            self.assertEqual(len(attribute_outputs), 1)
            table = pq.read_table(attribute_outputs[0])
            self.assertEqual(table["gauge_id"].to_pylist(), ["001", "002"])
            self.assertEqual(table["_earthzarr_dataset_id"].to_pylist(), ["camels_test", "camels_test"])

            cloned_chunk = (
                lake_root
                / "arrays"
                / "hydrology"
                / "basin_timeseries"
                / "dataset=camels_test"
                / "daily.zarr"
                / "streamflow"
                / "c"
                / "0"
            )
            self.assertEqual(cloned_chunk.read_bytes(), b"zarr chunk")
            self.assertEqual(source_chunk.stat().st_ino, cloned_chunk.stat().st_ino)

            second = HydroMaterializer(data_root, lake_root, existing_zarr_root=zarr_root).materialize(
                kinds={"entities", "arrays"}
            )
            self.assertEqual(second["materialized"], 0)
            self.assertEqual(second["skipped"], 2)
            manifest = json.loads(
                (lake_root / "manifests" / "materializations" / "hydrodatasets.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(manifest["records"]), 2)

    def test_inventory_discovers_supported_source_types(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            dataset = data_root / "CAMELS_X"
            dataset.mkdir(parents=True)
            (dataset / "camels_x_D.nc").write_bytes(b"netcdf")
            (dataset / "soil_attributes.txt").write_text("id value\n1 2\n", encoding="utf-8")
            zarr_root = root / "zarr"
            (zarr_root / "camels_x_timeseries.zarr").mkdir(parents=True)

            inventory = HydroMaterializer(data_root, root / "lake", existing_zarr_root=zarr_root).inventory()

            self.assertEqual(inventory["attribute_table_count"], 1)
            self.assertEqual(inventory["standard_netcdf_count"], 1)
            self.assertEqual(inventory["existing_zarr_count"], 1)

    def test_resumes_existing_partial_zarr_clone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "published-zarr" / "camels_test_timeseries.zarr"
            (source / "flow" / "c").mkdir(parents=True)
            (source / "zarr.json").write_text('{"zarr_format": 3, "node_type": "group"}', encoding="utf-8")
            source_chunk = source / "flow" / "c" / "0"
            source_chunk.write_bytes(b"already copied")

            lake_root = root / "lake"
            partial = (
                lake_root
                / "arrays"
                / "hydrology"
                / "basin_timeseries"
                / "dataset=camels_test"
                / ".daily.zarr.123.partial"
            )
            partial_chunk = partial / "flow" / "c" / "0"
            partial_chunk.parent.mkdir(parents=True)
            partial_chunk.write_bytes(source_chunk.read_bytes())
            partial_inode = partial_chunk.stat().st_ino

            result = HydroMaterializer(
                root / "data",
                lake_root,
                existing_zarr_root=root / "published-zarr",
            ).materialize(kinds={"arrays"})

            output_chunk = partial.parent / "daily.zarr" / "flow" / "c" / "0"
            self.assertEqual(result["materialized"], 1)
            self.assertEqual(output_chunk.stat().st_ino, partial_inode)

    def test_corrupt_existing_attribute_is_rebuilt_not_silently_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_test"
            data_root.mkdir(parents=True)
            source = data_root / "catchment_attributes.csv"
            source.write_text("basin_id,value\n001,12.5\n", encoding="utf-8")
            lake_root = root / "lake"
            first = HydroMaterializer(data_root.parent, lake_root).materialize(kinds={"entities"})
            output = lake_root / first["records"][0]["output"]
            output.write_bytes(b"corrupt")

            second = HydroMaterializer(data_root.parent, lake_root).materialize(kinds={"entities"})

            self.assertEqual(second["materialized"], 1)
            self.assertEqual(second["skipped"], 0)
            self.assertEqual(pq.read_table(output)["basin_id"].to_pylist(), ["001"])

    def test_partial_zarr_with_same_size_corruption_is_repaired(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "published-zarr" / "camels_test_timeseries.zarr"
            (source / "flow" / "c").mkdir(parents=True)
            (source / "zarr.json").write_text('{"zarr_format": 3}', encoding="utf-8")
            source_chunk = source / "flow" / "c" / "0"
            source_chunk.write_bytes(b"correct chunk")
            lake_root = root / "lake"
            partial = lake_root / "arrays" / "hydrology" / "basin_timeseries" / "dataset=camels_test" / ".daily.zarr.resume.partial"
            (partial / "flow" / "c").mkdir(parents=True)
            (partial / "zarr.json").write_text('{"zarr_format": 3}', encoding="utf-8")
            (partial / "flow" / "c" / "0").write_bytes(b"xxxxxxxxxxxxx")

            result = HydroMaterializer(
                root / "data", lake_root, existing_zarr_root=root / "published-zarr"
            ).materialize(kinds={"arrays"})

            output_chunk = lake_root / "arrays" / "hydrology" / "basin_timeseries" / "dataset=camels_test" / "daily.zarr" / "flow" / "c" / "0"
            self.assertEqual(result["materialized"], 1)
            self.assertEqual(output_chunk.read_bytes(), source_chunk.read_bytes())

    def test_concurrent_duplicate_materialization_preserves_manifest_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_test"
            data_root.mkdir(parents=True)
            for name, value in (("first", "1"), ("second", "2")):
                (data_root / f"{name}_attributes.csv").write_text(
                    f"basin_id,value\n001,{value}\n", encoding="utf-8"
                )
            lake_root = root / "lake"
            errors: list[Exception] = []

            def materialize() -> None:
                try:
                    HydroMaterializer(data_root.parent, lake_root).materialize(kinds={"entities"})
                except Exception as exc:  # pragma: no cover - assertion below reports it
                    errors.append(exc)

            threads = [threading.Thread(target=materialize) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

            self.assertEqual(errors, [])
            manifest = json.loads(
                (lake_root / "manifests" / "materializations" / "hydrodatasets.json").read_text()
            )
            self.assertEqual(len(manifest["records"]), 2)

    def test_adopts_matching_published_zarr_missing_from_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "published-zarr" / "camels_test_attributes.zarr"
            source.mkdir(parents=True)
            (source / "zarr.json").write_text('{"zarr_format": 3, "node_type": "group"}', encoding="utf-8")
            output = (
                root
                / "lake"
                / "arrays"
                / "static"
                / "basin_attributes"
                / "dataset=camels_test"
                / "attributes.zarr"
            )
            output.mkdir(parents=True)
            (output / "zarr.json").write_bytes((source / "zarr.json").read_bytes())

            result = HydroMaterializer(
                root / "data",
                root / "lake",
                existing_zarr_root=root / "published-zarr",
            ).materialize(kinds={"arrays"})

            self.assertEqual(result["materialized"], 1)
            self.assertEqual(result["failed"], 0)
            self.assertEqual(result["records"][0]["output"], output.relative_to(root / "lake").as_posix())

    def test_manifest_publication_failure_does_not_claim_completed_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_test"
            data_root.mkdir(parents=True)
            (data_root / "attributes.csv").write_text("basin_id,value\n001,1\n", encoding="utf-8")
            lake_root = root / "lake"

            with patch.object(ProtocolCommit, "publish", side_effect=OSError("publish failed")):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    HydroMaterializer(data_root.parent, lake_root).materialize(kinds={"entities"})

            manifest_path = lake_root / "manifests" / "materializations" / "hydrodatasets.json"
            self.assertFalse(manifest_path.exists())
            self.assertTrue(list((lake_root / "entities").rglob("*.parquet")))

            retry = HydroMaterializer(data_root.parent, lake_root).materialize(kinds={"entities"})
            self.assertEqual(retry["materialized"], 1)
            self.assertTrue(manifest_path.exists())

    def test_corrupt_manifest_is_preserved_and_materialization_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            dataset = data_root / "camels_us"
            dataset.mkdir(parents=True)
            (dataset / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            lake_root = root / "lake"
            HydroMaterializer(data_root, lake_root).materialize(kinds={"entities"})
            manifest_path = lake_root / "manifests" / "materializations" / "hydrodatasets.json"
            manifest_path.write_text("{invalid json", encoding="utf-8")
            original = manifest_path.read_bytes()

            with self.assertRaisesRegex(ValueError, "manifest"):
                HydroMaterializer(data_root, lake_root).materialize(kinds={"entities"})

            self.assertEqual(manifest_path.read_bytes(), original)

    def test_malformed_manifest_record_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_us"
            data_root.mkdir(parents=True)
            (data_root / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            lake_root = root / "lake"
            materializer = HydroMaterializer(root / "data", lake_root)
            materializer.materialize(kinds={"entities"})
            manifest_path = lake_root / "manifests" / "materializations" / "hydrodatasets.json"
            payload = json.loads(manifest_path.read_text())
            payload["records"][0].pop("logical_asset_id")
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")
            original = manifest_path.read_bytes()

            with self.assertRaisesRegex(ValueError, "logical_asset_id"):
                HydroMaterializer(root / "data", lake_root).materialize(kinds={"entities"})

            self.assertEqual(manifest_path.read_bytes(), original)

    def test_same_relative_source_across_roots_has_one_ownership_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake_root = root / "lake"
            for machine in ("machine-a", "machine-b"):
                source = root / machine / "data" / "camels_us"
                source.mkdir(parents=True)
                (source / "attributes.csv").write_text(
                    "basin_id,value\n001,1\n", encoding="utf-8"
                )

            first = HydroMaterializer(root / "machine-a" / "data", lake_root).materialize(
                kinds={"entities"}
            )
            second = HydroMaterializer(root / "machine-b" / "data", lake_root).materialize(
                kinds={"entities"}
            )
            payload = json.loads(
                (lake_root / "manifests" / "materializations" / "hydrodatasets.json").read_text()
            )

            self.assertEqual(first["materialized"], 1)
            self.assertEqual(second["skipped"], 1)
            self.assertEqual(len(payload["records"]), 1)
            self.assertEqual(
                payload["records"][0]["source"],
                str((root / "machine-b" / "data" / "camels_us" / "attributes.csv").resolve()),
            )

    def test_same_relative_source_changed_content_updates_one_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake_root = root / "lake"
            for machine, value in (("machine-a", "1"), ("machine-b", "2")):
                source = root / machine / "data" / "camels_us"
                source.mkdir(parents=True)
                (source / "attributes.csv").write_text(
                    f"basin_id,value\n001,{value}\n", encoding="utf-8"
                )

            HydroMaterializer(root / "machine-a" / "data", lake_root).materialize(
                kinds={"entities"}
            )
            second = HydroMaterializer(root / "machine-b" / "data", lake_root).materialize(
                kinds={"entities"}
            )
            payload = json.loads(
                (lake_root / "manifests" / "materializations" / "hydrodatasets.json").read_text()
            )
            output = lake_root / payload["records"][0]["output"]

            self.assertEqual(second["materialized"], 1)
            self.assertEqual(len(payload["records"]), 1)
            self.assertEqual(pq.read_table(output)["value"].to_pylist(), [2])

    def test_different_relative_sources_with_same_basename_are_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_source = root / "data" / "camels_us" / "region-a" / "attributes.csv"
            second_source = root / "data" / "camels_us" / "region-b" / "attributes.csv"
            first_source.parent.mkdir(parents=True)
            second_source.parent.mkdir(parents=True)
            first_source.write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            second_source.write_text(
                "basin_id,cover\n001,forest\n", encoding="utf-8"
            )

            result = HydroMaterializer(root / "data", root / "lake").materialize(
                kinds={"entities"}
            )
            manifest = json.loads(
                (root / "lake" / "manifests" / "materializations" / "hydrodatasets.json").read_text()
            )

            self.assertEqual(result["materialized"], 2)
            self.assertEqual(len(manifest["records"]), 2)
            self.assertEqual(
                {record["logical_asset_id"] for record in manifest["records"]},
                {record["logical_asset_id"] for record in result["records"]},
            )
            self.assertNotEqual(
                manifest["records"][0]["output"], manifest["records"][1]["output"]
            )

    def test_concurrent_cross_root_materialization_keeps_one_logical_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake_root = root / "lake"
            for machine in ("machine-a", "machine-b"):
                source = root / machine / "data" / "camels_us"
                source.mkdir(parents=True)
                (source / "attributes.csv").write_text(
                    "basin_id,value\n001,1\n", encoding="utf-8"
                )
            errors: list[Exception] = []

            def materialize(machine: str) -> None:
                try:
                    HydroMaterializer(
                        root / machine / "data", lake_root
                    ).materialize(kinds={"entities"})
                except Exception as exc:  # pragma: no cover - assertion below reports it
                    errors.append(exc)

            threads = [
                threading.Thread(target=materialize, args=(machine,))
                for machine in ("machine-a", "machine-b")
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

            payload = json.loads(
                (lake_root / "manifests" / "materializations" / "hydrodatasets.json").read_text()
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(payload["records"]), 1)

    def test_duplicate_logical_id_in_manifest_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_us"
            data_root.mkdir(parents=True)
            (data_root / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            lake_root = root / "lake"
            materializer = HydroMaterializer(root / "data", lake_root)
            materializer.materialize(kinds={"entities"})
            manifest_path = lake_root / "manifests" / "materializations" / "hydrodatasets.json"
            payload = json.loads(manifest_path.read_text())
            payload["records"].append(dict(payload["records"][0]))
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "duplicate logical_asset_id"):
                HydroMaterializer(root / "data", lake_root)

    def test_selected_dataset_limits_source_scan_and_parses_top_level_netcdf_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            for name in ("camels_us", "camels_gb"):
                dataset = data_root / name
                dataset.mkdir(parents=True)
                (dataset / "soil_attributes.csv").write_text("basin_id,value\n001,1\n", encoding="utf-8")
            (data_root / "camels_us" / "~$soil_attributes.xlsx").write_bytes(b"office lock")
            (data_root / "camels_us_D.nc").write_bytes(b"netcdf")

            materializer = HydroMaterializer(data_root, root / "lake")
            _, attributes, netcdf = materializer._discover_sources({"camels_us"})

            self.assertEqual([path.parent.name for path in attributes], ["camels_us"])
            self.assertEqual([path.name for path in netcdf], ["camels_us_D.nc"])
            inventory = materializer.inventory()
            self.assertIn("camels_us", inventory["datasets"])
            self.assertNotIn("camels_us_d.nc", inventory["datasets"])


if __name__ == "__main__":
    unittest.main()
