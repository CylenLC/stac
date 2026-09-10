import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from materialization import MaterializationManager, MaterializationStore
from protocol_commit import ProtocolCommit


class MaterializationRunTests(unittest.TestCase):
    def test_create_rejects_missing_data_root(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = MaterializationManager(Path(directory) / "lake")

            with self.assertRaisesRegex(ValueError, "data_root is not a readable directory"):
                manager.create_run({"data_root": str(Path(directory) / "missing")})

    def test_run_materializes_sources_and_persists_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            dataset = data_root / "CAMELS_TEST"
            dataset.mkdir(parents=True)
            (dataset / "basin_attributes.csv").write_text(
                "basin_id,area\n001,12.5\n002,15.0\n", encoding="utf-8"
            )
            manager = MaterializationManager(root / "lake")
            run = manager.create_run({
                "data_root": str(data_root),
                "existing_zarr_root": str(root / "zarr"),
                "hydrodataset_project": str(root / "hydrodataset"),
                "kinds": ["entities"],
            })

            completed = manager.run(run["run_id"])

            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["total_sources"], 1)
            self.assertEqual(completed["completed_sources"], 1)
            self.assertEqual(completed["materialized_count"], 1)
            self.assertEqual(len(completed["result"]["records"]), 1)
            self.assertTrue(list((root / "lake" / "entities").rglob("*.parquet")))

    def test_pause_resume_and_cancel_are_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "data").mkdir()
            manager = MaterializationManager(Path(directory) / "lake")
            run = manager.create_run({"data_root": str(Path(directory) / "data")})

            self.assertEqual(manager.pause_run(run["run_id"])["status"], "paused")
            self.assertEqual(MaterializationManager(Path(directory) / "lake").get_run(run["run_id"])["status"], "paused")
            self.assertEqual(manager.resume_run(run["run_id"])["status"], "queued")
            self.assertEqual(manager.cancel_run(run["run_id"])["status"], "cancelled")

    def test_interrupted_running_run_is_requeued_on_store_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "lake"
            data_root = Path(directory) / "data"
            data_root.mkdir()
            manager = MaterializationManager(root)
            run = manager.create_run({"data_root": str(data_root)})
            manager.store.update(
                run["run_id"],
                status="running",
                total_sources=5,
                completed_sources=3,
                materialized_count=2,
                skipped_count=1,
            )

            recovered = MaterializationStore(root).get(run["run_id"])

            self.assertEqual(recovered["status"], "queued")
            self.assertEqual(recovered["completed_sources"], 0)
            self.assertEqual(recovered["materialized_count"], 0)
            self.assertEqual(recovered["skipped_count"], 0)

    def test_interrupted_finalizing_run_is_requeued_on_store_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "lake"
            data_root = Path(directory) / "data"
            data_root.mkdir()
            manager = MaterializationManager(root)
            run = manager.create_run({"data_root": str(data_root)})
            manager.store.update(run["run_id"], status="finalizing")

            recovered = MaterializationStore(root).get(run["run_id"])

            self.assertEqual(recovered["status"], "queued")

    def test_pause_wins_race_with_inventory_transition(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "data").mkdir()
            manager = MaterializationManager(Path(directory) / "lake")
            run = manager.create_run({"data_root": str(Path(directory) / "data")})

            def pause_during_inventory(*args, **kwargs):
                manager.pause_run(run["run_id"])
                return 0

            with patch.object(manager, "_source_count", side_effect=pause_during_inventory):
                paused = manager.run(run["run_id"])

            self.assertEqual(paused["status"], "paused")

    def test_cancel_is_rejected_during_manifest_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_us"
            data_root.mkdir(parents=True)
            (data_root / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            manager = MaterializationManager(root / "lake")
            run = manager.create_run({
                "data_root": str(root / "data"),
                "existing_zarr_root": str(root / "zarr"),
                "hydrodataset_project": str(root / "hydrodataset"),
                "kinds": ["entities"],
            })
            entered = threading.Event()
            release = threading.Event()
            original_publish = ProtocolCommit.publish

            def blocked_publish(commit):
                entered.set()
                self.assertTrue(release.wait(timeout=5))
                return original_publish(commit)

            with patch.object(ProtocolCommit, "publish", blocked_publish):
                worker = threading.Thread(target=manager.run, args=(run["run_id"],))
                worker.start()
                self.assertTrue(entered.wait(timeout=5))
                self.assertEqual(manager.get_run(run["run_id"])["status"], "finalizing")
                with self.assertRaisesRegex(ValueError, "cannot cancel finalizing run"):
                    manager.cancel_run(run["run_id"])
                release.set()
                worker.join(timeout=10)

            completed = manager.get_run(run["run_id"])
            manifest = root / "lake" / "manifests" / "materializations" / "hydrodatasets.json"
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(len(json.loads(manifest.read_text())["records"]), 1)

    def test_manifest_publication_failure_leaves_run_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data" / "camels_us"
            data_root.mkdir(parents=True)
            (data_root / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            manager = MaterializationManager(root / "lake")
            run = manager.create_run({
                "data_root": str(root / "data"),
                "existing_zarr_root": str(root / "zarr"),
                "hydrodataset_project": str(root / "hydrodataset"),
                "kinds": ["entities"],
            })

            with patch.object(ProtocolCommit, "publish", side_effect=OSError("publish failed")):
                failed = manager.run(run["run_id"])

            manifest = root / "lake" / "manifests" / "materializations" / "hydrodatasets.json"
            self.assertEqual(failed["status"], "failed")
            self.assertIn("publish failed", failed["error"])
            self.assertFalse(manifest.exists())


if __name__ == "__main__":
    unittest.main()
