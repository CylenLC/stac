import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from acquisition.store import AcquisitionStore
import pyarrow.parquet as pq
from earth_lake import EarthLake, REGISTRY_SCHEMAS
from stac_core import asset_filename
from stac_identity import AssetIdentity, safe_component


class CanonicalAssetIdentityTests(unittest.TestCase):
    def test_cross_collection_same_item_and_asset_are_distinct(self):
        first = AssetIdentity("test", "collection-a", "SAME", "data")
        second = AssetIdentity("test", "collection-b", "SAME", "data")
        self.assertNotEqual(first, second)
        self.assertNotEqual(first.digest(), second.digest())

    def test_same_item_different_asset_key_has_different_path(self):
        red = AssetIdentity("test", "collection", "ITEM", "red")
        nir = AssetIdentity("test", "collection", "ITEM", "nir")
        red_path = asset_filename("red", {"href": "https://a.example/data.tif"}, red)
        nir_path = asset_filename("nir", {"href": "https://b.example/data.tif"}, nir)
        self.assertNotEqual(red_path, nir_path)

    def test_same_basename_has_different_path_for_different_assets(self):
        first = AssetIdentity("test", "collection", "ITEM-A", "data")
        second = AssetIdentity("test", "collection", "ITEM-B", "data")
        first_path = asset_filename("data", {"href": "https://a.example/data.tif"}, first)
        second_path = asset_filename("data", {"href": "https://b.example/data.tif"}, second)
        self.assertNotEqual(first_path, second_path)

    def test_sanitization_and_truncation_collisions_are_separated(self):
        self.assertNotEqual(safe_component("A/B"), safe_component("A:B"))
        prefix = "x" * 121
        self.assertNotEqual(safe_component(prefix + "a"), safe_component(prefix + "b"))

    def test_signed_url_changes_do_not_change_identity_or_path(self):
        identity = AssetIdentity("test", "collection", "ITEM", "data")
        first = asset_filename("data", {"href": "https://a.example/data.tif?token=AAA"}, identity)
        second = asset_filename("data", {"href": "https://a.example/data.tif?token=BBB"}, identity)
        self.assertEqual(first, second)
        self.assertEqual(identity.serialize(), identity.serialize())

    def test_registry_uses_canonical_id_and_preserves_catalog_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            ids = []
            for collection in ("A", "B"):
                item = {"id": "SAME", "collection": collection, "properties": {}}
                path = lake.source_item_directory("test", collection, "SAME") / "data.bin"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(collection.encode())
                ids.append(
                    lake.record_asset(
                        run_id="run",
                        catalog="test",
                        item=item,
                        asset_key="data",
                        source_url=f"https://{collection}/data.bin?token=one",
                        local_path=path,
                        status="downloaded",
                    )
                )
            self.assertEqual(len(set(ids)), 2)
            rows = pq.read_table(lake.registry_dir / "assets.parquet").to_pylist()
            self.assertEqual({row["collection_id"] for row in rows}, {"A", "B"})
            self.assertEqual({row["catalog"] for row in rows}, {"test"})
            self.assertEqual(len({row["local_path"] for row in rows}), 2)

    def test_registry_asset_id_is_stable_when_source_url_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            item = {"id": "ITEM", "collection": "A", "properties": {}}
            path = lake.source_item_directory("test", "A", "ITEM") / "data.bin"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"data")
            first = lake.record_asset(
                run_id="run",
                catalog="test",
                item=item,
                asset_key="data",
                source_url="https://example/data.bin?token=AAA",
                local_path=path,
                status="downloaded",
            )
            second = lake.record_asset(
                run_id="run",
                catalog="test",
                item=item,
                asset_key="data",
                source_url="https://example/data.bin?token=BBB",
                local_path=path,
                status="skipped",
            )
            self.assertEqual(first, second)
            self.assertEqual(len(pq.read_table(lake.registry_dir / "assets.parquet")), 1)

    @staticmethod
    def _asset_row(**values):
        row = {name: None for name in REGISTRY_SCHEMAS["assets"].names}
        row.update(values)
        return row

    def test_registry_migration_backfills_identity_from_canonical_source_path(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            row = self._asset_row(
                asset_id="legacy-id",
                source_item_id="ITEM",
                asset_key="data",
                local_path="source/test/A/ITEM/data.bin",
            )
            pq.write_table(pa.Table.from_pylist([row], schema=REGISTRY_SCHEMAS["assets"]), lake.registry_dir / "assets.parquet")

            migrated = EarthLake(directory)
            rows = migrated._read_rows("assets")
            self.assertEqual(rows[0]["asset_id"], AssetIdentity("test", "A", "ITEM", "data").digest())
            self.assertEqual(rows[0]["catalog"], "test")
            self.assertEqual(rows[0]["collection_id"], "A")

    def test_registry_migration_rejects_ambiguous_legacy_row(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            row = self._asset_row(
                asset_id="legacy-id",
                source_item_id="ITEM",
                asset_key="data",
                local_path="legacy/data.bin",
            )
            pq.write_table(pa.Table.from_pylist([row], schema=REGISTRY_SCHEMAS["assets"]), lake.registry_dir / "assets.parquet")

            with self.assertRaisesRegex(ValueError, "ambiguous canonical identity"):
                EarthLake(directory)

    def test_registry_migration_rejects_duplicate_canonical_identity_even_same_path(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            rows = [
                self._asset_row(asset_id="old-1", catalog="test", collection_id="A", source_item_id="ITEM", asset_key="data", local_path="source/test/A/ITEM/data.bin"),
                self._asset_row(asset_id="old-2", catalog="test", collection_id="A", source_item_id="ITEM", asset_key="data", local_path="source/test/A/ITEM/data.bin"),
            ]
            pq.write_table(pa.Table.from_pylist(rows, schema=REGISTRY_SCHEMAS["assets"]), lake.registry_dir / "assets.parquet")

            with self.assertRaisesRegex(ValueError, "duplicate canonical asset identity"):
                EarthLake(directory)

    def test_registry_identity_migration_rolls_back_all_staged_outputs_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            row = self._asset_row(
                asset_id="legacy-id",
                source_item_id="ITEM",
                asset_key="data",
                local_path="source/test/A/ITEM/data.bin",
            )
            pq.write_table(
                pa.Table.from_pylist([row], schema=REGISTRY_SCHEMAS["assets"]),
                lake.registry_dir / "assets.parquet",
            )

            with patch.object(EarthLake, "_migrate_stac_asset_ids", side_effect=RuntimeError("stac migration failed")):
                with self.assertRaisesRegex(RuntimeError, "stac migration failed"):
                    EarthLake(directory)

            active = pq.read_table(lake.registry_dir / "assets.parquet").to_pylist()
            self.assertEqual(active[0]["asset_id"], "legacy-id")

            migrated = EarthLake(directory)
            self.assertEqual(
                migrated._read_rows("assets")[0]["asset_id"],
                AssetIdentity("test", "A", "ITEM", "data").digest(),
            )

    def test_legacy_attempt_with_missing_destination_reconstructs_canonical_path(self):
        with tempfile.TemporaryDirectory() as directory:
            store = AcquisitionStore(directory)
            request = {
                "catalog": "test",
                "collections": ["A"],
                "wkt": "POINT (0 0)",
                "start_date": "2024-01-01",
                "end_date": "2024-01-02",
            }
            run_id, _ = store.create_run("run", "legacy-destination", request)
            item = {
                "id": "ITEM",
                "collection": "A",
                "properties": {},
                "assets": {"data": {"href": "https://example/data.tif"}},
            }
            store.commit_page(run_id, 1, None, None, "page.jsonl.gz", "checksum", [item], "test")
            with store.transaction() as db:
                db.execute(
                    "INSERT INTO download_batches VALUES(?,?,'queued',?,0,0,?,?)",
                    (run_id, 1, 1, "2024-01-01", "2024-01-01"),
                )
                db.execute("DROP TABLE download_attempts")
                db.execute(
                    """
                    CREATE TABLE download_attempts (
                        attempt_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        batch_number INTEGER NOT NULL,
                        source_item_id TEXT NOT NULL,
                        asset_key TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        expected_bytes INTEGER NOT NULL DEFAULT 0,
                        downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                        etag TEXT,
                        error TEXT,
                        updated_at TEXT NOT NULL,
                        UNIQUE (run_id, source_item_id, asset_key),
                        FOREIGN KEY (run_id, batch_number) REFERENCES download_batches(run_id, batch_number)
                    )
                    """
                )
                db.execute(
                    """
                    INSERT INTO download_attempts VALUES(
                        'legacy-attempt',?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        run_id,
                        1,
                        "ITEM",
                        "data",
                        "https://example/data.tif?token=A",
                        "",
                        "queued",
                        0,
                        0,
                        0,
                        None,
                        None,
                        "2024-01-01",
                    ),
                )

            migrated = AcquisitionStore(directory)
            attempt = migrated.attempts_for_batch(run_id, 1)[0]
            expected = str(
                Path(directory).resolve()
                / "source"
                / "test"
                / "A"
                / "ITEM"
                / f"data__{AssetIdentity('test', 'A', 'ITEM', 'data').digest(16)}.tif"
            )
            self.assertEqual(attempt["destination"], expected)
            self.assertNotIn(attempt["destination"], {"", ".", str(Path(directory).resolve())})


class AcquisitionIdentityStoreTests(unittest.TestCase):
    @staticmethod
    def _request(collections):
        return {
            "catalog": "test",
            "collections": collections,
            "wkt": "POINT (0 0)",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        }

    def test_store_lookup_and_attempts_include_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            store = AcquisitionStore(directory)
            run_id, _ = store.create_run("run", "identity", self._request(["A", "B"]))
            store.commit_page(
                run_id,
                1,
                None,
                None,
                "page.jsonl.gz",
                "checksum",
                [
                    {"id": "SAME", "collection": "A", "assets": {}},
                    {"id": "SAME", "collection": "B", "assets": {}},
                ],
                "test",
            )
            store.replace_plan(
                run_id,
                [[
                    {
                        "catalog": "test",
                        "collection": "A",
                        "source_item_id": "SAME",
                        "asset_key": "data",
                        "source_url": "https://a/data.tif",
                        "destination": "source/test/A/SAME/data-a.tif",
                    },
                    {
                        "catalog": "test",
                        "collection": "B",
                        "source_item_id": "SAME",
                        "asset_key": "data",
                        "source_url": "https://b/data.tif",
                        "destination": "source/test/B/SAME/data-b.tif",
                    },
                ]],
            )

            attempts = store.attempts_for_batch(run_id, 1)
            self.assertEqual(len(attempts), 2)
            self.assertEqual({row["collection_id"] for row in attempts}, {"A", "B"})
            self.assertNotEqual(attempts[0]["attempt_id"], attempts[1]["attempt_id"])
            self.assertEqual(store.item(run_id, "test", "A", "SAME")["collection"], "A")
            self.assertEqual(store.item(run_id, "test", "B", "SAME")["collection"], "B")

    def test_legacy_attempt_with_ambiguous_collection_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            store = AcquisitionStore(directory)
            run_id, _ = store.create_run("run", "legacy", self._request(["A", "B"]))
            with store.transaction() as db:
                db.execute(
                    "INSERT INTO download_batches VALUES(?,?,'queued',?,0,0,?,?)",
                    (run_id, 1, 1, "2024-01-01", "2024-01-01"),
                )
                db.execute("DROP TABLE download_attempts")
                db.execute(
                    """
                    CREATE TABLE download_attempts (
                        attempt_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        batch_number INTEGER NOT NULL,
                        source_item_id TEXT NOT NULL,
                        asset_key TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        expected_bytes INTEGER NOT NULL DEFAULT 0,
                        downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                        etag TEXT,
                        error TEXT,
                        updated_at TEXT NOT NULL,
                        UNIQUE (run_id, source_item_id, asset_key),
                        FOREIGN KEY (run_id, batch_number) REFERENCES download_batches(run_id, batch_number)
                    )
                    """
                )
                db.execute(
                    """
                    INSERT INTO download_attempts VALUES(
                        'legacy-attempt',?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        run_id,
                        1,
                        "SAME",
                        "data",
                        "https://example/data.tif",
                        "source/data.tif",
                        "queued",
                        0,
                        0,
                        0,
                        None,
                        None,
                        "2024-01-01",
                    ),
                )

            migrated = AcquisitionStore(directory)
            attempt = migrated.attempts_for_batch(run_id, 1)[0]
            self.assertEqual(attempt["catalog"], AcquisitionStore.LEGACY_CATALOG)
            self.assertEqual(attempt["collection_id"], AcquisitionStore.LEGACY_COLLECTION)
            self.assertEqual(attempt["status"], "failed")
            self.assertIn("ambiguous Collection", attempt["error"])


if __name__ == "__main__":
    unittest.main()
