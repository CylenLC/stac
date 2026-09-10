import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from earth_lake import EarthLake
from lake_health import LakeHealthAuditor, LakeHealthManager, _normalize_sqlite_default
from materialization import MaterializationStore
from acquisition.store import AcquisitionStore
from hydro_materializer import HydroMaterializer


class LakeHealthTests(unittest.TestCase):
    @staticmethod
    def _recreate_table(path, table, transform, after_create=None):
        with sqlite3.connect(path) as db:
            ddl = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            db.execute(f'DROP TABLE "{table}"')
            db.execute(transform(ddl))
            if after_create:
                after_create(db)

    @staticmethod
    def _append_column(ddl, declaration):
        statement = ddl.rstrip().rstrip(";")
        return f"{statement[:-1]}, {declaration})"

    def test_audit_persists_missing_unregistered_and_unmanifested_issues(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            orphan = Path(directory) / "source" / "nasa" / "HLS" / "orphan" / "asset.tif"
            orphan.parent.mkdir(parents=True)
            orphan.write_bytes(b"orphan")
            entity = Path(directory) / "entities" / "basins" / "orphan.parquet"
            entity.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"id": ["001"]}), entity)

            report = LakeHealthAuditor(directory).run("audit-test")

            self.assertEqual(report["status"], "completed")
            codes = {issue["code"] for issue in report["issues"]}
            self.assertIn("unregistered_source", codes)
            self.assertIn("unmanifested_materialization", codes)
            persisted = json.loads(
                (Path(directory) / "manifests" / "health" / "audit-test.json").read_text()
            )
            self.assertEqual(persisted["summary"]["total"], len(persisted["issues"]))

    def test_corrupt_registry_is_reported_instead_of_treated_as_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            (lake.registry_dir / "assets.parquet").write_bytes(b"not parquet")

            report = LakeHealthAuditor(directory).run("audit-corrupt")

            issue = next(item for item in report["issues"] if item["code"] == "registry_unreadable")
            self.assertEqual(issue["severity"], "critical")

    def test_pending_protocol_commit_is_reported_as_critical(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            journal = Path(directory) / "manifests" / "protocol_commits" / "pending.json"
            journal.parent.mkdir(parents=True, exist_ok=True)
            journal.write_text(
                json.dumps({"commit_id": "pending", "status": "publishing"}),
                encoding="utf-8",
            )

            report = LakeHealthAuditor(directory).run("audit-pending-commit")

            issue = next(item for item in report["issues"] if item["code"] == "protocol_commit_pending")
            self.assertEqual(issue["severity"], "critical")
            self.assertEqual(report["stats"]["pending_protocol_commits"], 1)

    def test_audit_ignores_macos_appledouble_stac_files(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            (lake.stac_dir / "._item.json").write_bytes(b"not json")

            report = LakeHealthAuditor(directory).run("audit-appledouble")

            self.assertNotIn("stac_unreadable", {item["code"] for item in report["issues"]})

    def test_manager_runs_audit_in_background_and_lists_it(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            manager = LakeHealthManager(directory)

            queued = manager.start()
            thread = manager._threads[queued["run_id"]]
            thread.join(timeout=10)
            completed = manager.get(queued["run_id"])

            self.assertEqual(completed["status"], "completed")
            self.assertEqual(manager.latest()["run_id"], queued["run_id"])
            self.assertEqual(manager.list()[0]["run_id"], queued["run_id"])

    def test_manager_rejects_overlapping_audits(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            manager = LakeHealthManager(directory)
            started = threading.Event()
            release = threading.Event()

            def blocked_audit(*args, **kwargs):
                started.set()
                release.wait(timeout=5)

            with patch.object(manager.auditor, "run", side_effect=blocked_audit):
                first = manager.start()
                self.assertTrue(started.wait(timeout=2))
                thread = manager._threads[first["run_id"]]
                with self.assertRaisesRegex(ValueError, "already running"):
                    manager.start()
                release.set()
                thread.join(timeout=5)

    def test_readiness_is_bounded_read_only_and_external_stac_is_optional(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            health_root = Path(directory) / "manifests" / "health"
            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "ready")
            self.assertEqual(result["external_stac"]["status"], "not_checked")
            self.assertFalse(health_root.exists())

    def test_repeated_readiness_is_read_only_and_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            paths = [
                Path(directory) / "protocol" / "schema_version.json",
                Path(directory) / "registry" / "acquisition_state.sqlite",
                Path(directory) / "registry" / "materialization_state.sqlite",
            ]
            before = {path: path.read_bytes() for path in paths}

            first = LakeHealthAuditor(directory).readiness()
            second = LakeHealthAuditor(directory).readiness()

            self.assertEqual(first, second)
            self.assertEqual(before, {path: path.read_bytes() for path in paths})

    def test_readiness_does_not_instantiate_store_constructors(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)

            with patch.object(AcquisitionStore, "__init__", side_effect=AssertionError("constructor")), patch.object(
                MaterializationStore, "__init__", side_effect=AssertionError("constructor")
            ):
                result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "ready")

    def test_readiness_fails_closed_for_corrupt_critical_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            (Path(directory) / "registry" / "assets.parquet").write_bytes(b"corrupt")

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("registry:assets", result["failed_checks"])

    def test_readiness_validates_protocol_identity_and_required_fields(self):
        variants = [
            {"protocol": "OtherProtocol", "version": "0.1.0", "processing_version": "2026.07.1"},
            {"protocol": "EarthZarrProtocol", "version": "999.0.0", "processing_version": "2026.07.1"},
            {"protocol": "EarthZarrProtocol", "processing_version": "2026.07.1"},
            {"protocol": "EarthZarrProtocol", "version": "0.1.0"},
            {"protocol": "EarthZarrProtocol", "version": 1, "processing_version": "2026.07.1"},
        ]
        for variant in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                (Path(directory) / "protocol" / "schema_version.json").write_text(
                    json.dumps(variant), encoding="utf-8"
                )

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")
                self.assertIn("protocol_schema", result["failed_checks"])

        for content in ("{not json", None):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                schema_path = Path(directory) / "protocol" / "schema_version.json"
                if content is None:
                    schema_path.unlink()
                else:
                    schema_path.write_text(content, encoding="utf-8")

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")
                self.assertIn("protocol_schema", result["failed_checks"])

    def test_readiness_validates_sqlite_application_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "ready")

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            registry = Path(directory) / "registry"
            for name in ("acquisition_state.sqlite", "materialization_state.sqlite"):
                (registry / name).touch()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            (Path(directory) / "registry" / "acquisition_state.sqlite").write_bytes(b"not sqlite")

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "acquisition_state.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("DROP TABLE acquisition_runs")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "materialization_state.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("DROP TABLE materialization_runs")
                db.execute("CREATE TABLE materialization_runs (run_id TEXT, status TEXT)")
                db.execute("PRAGMA user_version = 1")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "acquisition_state.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA user_version = 999")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])

    def test_readiness_rejects_runtime_incompatible_store_with_partial_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)

            acquisition_path = Path(directory) / "registry" / "acquisition_state.sqlite"
            materialization_path = Path(directory) / "registry" / "materialization_state.sqlite"
            with sqlite3.connect(acquisition_path) as db:
                db.execute("DROP TABLE acquisition_runs")
                db.execute(
                    """
                    CREATE TABLE acquisition_runs (
                        run_id TEXT PRIMARY KEY,
                        request_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("PRAGMA user_version = 3")
                db.commit()
            with sqlite3.connect(materialization_path) as db:
                db.execute("DROP TABLE materialization_runs")
                db.execute(
                    """
                    CREATE TABLE materialization_runs (
                        run_id TEXT PRIMARY KEY,
                        request_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("PRAGMA user_version = 1")
                db.commit()

            before = {
                path: path.read_bytes()
                for path in (acquisition_path, materialization_path)
            }
            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])
            self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_readiness_rejects_wrong_runtime_column_affinity(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "materialization_state.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("DROP TABLE materialization_runs")
                db.execute(
                    """
                    CREATE TABLE materialization_runs (
                        run_id TEXT PRIMARY KEY,
                        request_json TEXT NOT NULL,
                        status BLOB NOT NULL,
                        message TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        total_sources INTEGER NOT NULL DEFAULT 0,
                        completed_sources INTEGER NOT NULL DEFAULT 0,
                        materialized_count INTEGER NOT NULL DEFAULT 0,
                        skipped_count INTEGER NOT NULL DEFAULT 0,
                        failed_count INTEGER NOT NULL DEFAULT 0,
                        current_source TEXT,
                        result_json TEXT NOT NULL DEFAULT '{}',
                        error TEXT
                    )
                    """
                )
                db.execute("PRAGMA user_version = 1")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])

    def test_readiness_rejects_missing_runtime_columns_in_attempt_and_control_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            acquisition_path = Path(directory) / "registry" / "acquisition_state.sqlite"
            materialization_path = Path(directory) / "registry" / "materialization_state.sqlite"
            with sqlite3.connect(acquisition_path) as db:
                db.execute("DROP TABLE download_attempts")
                db.execute(
                    """
                    CREATE TABLE download_attempts (
                        attempt_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        batch_number INTEGER NOT NULL,
                        catalog TEXT NOT NULL,
                        collection_id TEXT NOT NULL,
                        source_item_id TEXT NOT NULL,
                        asset_key TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        expected_bytes INTEGER NOT NULL DEFAULT 0,
                        downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                        etag TEXT,
                        error TEXT,
                        updated_at TEXT NOT NULL,
                        UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),
                        FOREIGN KEY (run_id, batch_number)
                            REFERENCES download_batches(run_id, batch_number)
                    )
                    """
                )
                db.commit()
            with sqlite3.connect(materialization_path) as db:
                db.execute("DROP TABLE materialization_runs")
                db.execute(
                    """
                    CREATE TABLE materialization_runs (
                        run_id TEXT PRIMARY KEY,
                        request_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        message TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        total_sources INTEGER NOT NULL DEFAULT 0,
                        completed_sources INTEGER NOT NULL DEFAULT 0,
                        materialized_count INTEGER NOT NULL DEFAULT 0,
                        skipped_count INTEGER NOT NULL DEFAULT 0,
                        failed_count INTEGER NOT NULL DEFAULT 0,
                        result_json TEXT NOT NULL DEFAULT '{}',
                        error TEXT
                    )
                    """
                )
                db.execute("PRAGMA user_version = 1")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:acquisition_state.sqlite", result["failed_checks"])
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])

    def test_readiness_rejects_missing_runtime_primary_key(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "materialization_state.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("DROP TABLE materialization_runs")
                db.execute(
                    """
                    CREATE TABLE materialization_runs (
                        run_id TEXT,
                        request_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        message TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        total_sources INTEGER NOT NULL DEFAULT 0,
                        completed_sources INTEGER NOT NULL DEFAULT 0,
                        materialized_count INTEGER NOT NULL DEFAULT 0,
                        skipped_count INTEGER NOT NULL DEFAULT 0,
                        failed_count INTEGER NOT NULL DEFAULT 0,
                        current_source TEXT,
                        result_json TEXT NOT NULL DEFAULT '{}',
                        error TEXT
                    )
                    """
                )
                db.execute("PRAGMA user_version = 1")
                db.commit()

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            self.assertIn("store:materialization_state.sqlite", result["failed_checks"])

    def test_readiness_accepts_complete_zero_row_stores_with_extra_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            with sqlite3.connect(Path(directory) / "registry" / "acquisition_state.sqlite") as db:
                db.execute("ALTER TABLE acquisition_runs ADD COLUMN harmless_extra TEXT")
            with sqlite3.connect(Path(directory) / "registry" / "materialization_state.sqlite") as db:
                db.execute("ALTER TABLE materialization_runs ADD COLUMN harmless_extra TEXT")

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "ready")

    def test_readiness_rejects_extra_primary_key_members_before_runtime_smoke_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "materialization_state.sqlite"

            def add_extra_primary_key(ddl):
                statement = ddl.replace("run_id TEXT PRIMARY KEY", "run_id TEXT", 1)
                statement = statement.rstrip().rstrip(";")
                return f"{statement[:-1]}, PRIMARY KEY (run_id, status))"

            self._recreate_table(path, "materialization_runs", add_extra_primary_key)
            with sqlite3.connect(path) as db:
                db.execute(
                    """
                    INSERT INTO materialization_runs(
                        run_id, request_json, status, message, created_at, updated_at
                    ) VALUES ('same', '{}', 'queued', '', 'now', 'now')
                    """
                )
                db.execute(
                    """
                    INSERT INTO materialization_runs(
                        run_id, request_json, status, message, created_at, updated_at
                    ) VALUES ('same', '{}', 'failed', '', 'now', 'now')
                    """
                )

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")
            store = MaterializationStore(directory)
            with self.assertRaises(KeyError):
                store.update("same", message="would update two rows")

    def test_readiness_accepts_equivalent_default_literals_but_rejects_different_default(self):
        for replacement, expected_status in (
            ('DEFAULT "{}"', "ready"),
            ("DEFAULT '[]'", "not_ready"),
        ):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "materialization_state.sqlite"
                self._recreate_table(
                    path,
                    "materialization_runs",
                    lambda ddl: ddl.replace("DEFAULT '{}'", replacement, 1),
                )

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], expected_status)

    def test_readiness_rejects_incompatible_nullability_for_materialization_and_acquisition(self):
        mutations = (
            (
                "materialization_state.sqlite",
                "materialization_runs",
                "current_source TEXT,",
                "current_source TEXT NOT NULL,",
            ),
            (
                "acquisition_state.sqlite",
                "acquisition_runs",
                "started_at TEXT,",
                "started_at TEXT NOT NULL,",
            ),
            (
                "materialization_state.sqlite",
                "materialization_runs",
                "status TEXT NOT NULL,",
                "status TEXT,",
            ),
        )
        for filename, table, old, new in mutations:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / filename
                self._recreate_table(
                    path,
                    table,
                    lambda ddl, old=old, new=new: ddl.replace(old, new, 1),
                )

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")
                self.assertIn(f"store:{filename}", result["failed_checks"])

    def test_readiness_rejects_missing_or_wrong_required_foreign_key(self):
        mutations = (
            ("missing", lambda ddl: ddl.replace(
                ",\n                    FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)", "", 1
            )),
            ("wrong-target", lambda ddl: ddl.replace(
                "REFERENCES acquisition_runs(run_id)", "REFERENCES acquisition_runs(other_id)", 1
            )),
        )
        for label, mutation in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "acquisition_state.sqlite"
                self._recreate_table(path, "search_pages", mutation)

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")

    def test_sqlite_default_normalization_is_narrow_and_semantic(self):
        equivalent = (
            ("NULL", "(NULL)"),
            ("0", "+0.0"),
            ("'{}'", '"{}"'),
            ("'foo'", '"foo"'),
        )
        for left, right in equivalent:
            with self.subTest(left=left, right=right):
                self.assertEqual(
                    _normalize_sqlite_default(left), _normalize_sqlite_default(right)
                )
        self.assertNotEqual(
            _normalize_sqlite_default("'{}'"), _normalize_sqlite_default("'[]'")
        )

    def test_readiness_rejects_missing_wrong_and_partial_required_unique_constraint(self):
        mutations = (
            ("missing", lambda ddl: ddl.replace(
                "UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),", "", 1
            )),
            ("wrong-vector", lambda ddl: ddl.replace(
                "UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),",
                "UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key, status),",
                1,
            )),
        )
        for label, mutation in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "acquisition_state.sqlite"
                self._recreate_table(path, "download_attempts", mutation)

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")

        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "acquisition_state.sqlite"

            def partial_unique(ddl):
                statement = ddl.replace(
                    "UNIQUE (run_id, catalog, collection_id, source_item_id, asset_key),", "", 1
                )
                return statement

            def add_partial_index(db):
                db.execute(
                    """
                    CREATE UNIQUE INDEX download_attempts_partial_identity
                    ON download_attempts(run_id, catalog, collection_id, source_item_id, asset_key)
                    WHERE status = 'completed'
                    """
                )

            self._recreate_table(path, "download_attempts", partial_unique, add_partial_index)
            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")

    def test_readiness_rejects_extra_required_column_without_default_and_accepts_usable_default(self):
        for declaration, expected_status in (
            ("extra_required TEXT NOT NULL", "not_ready"),
            ("extra_required TEXT NOT NULL DEFAULT 'x'", "ready"),
        ):
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "materialization_state.sqlite"
                self._recreate_table(
                    path,
                    "materialization_runs",
                    lambda ddl, declaration=declaration: self._append_column(ddl, declaration),
                )

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], expected_status)
                if expected_status == "ready":
                    run_id = MaterializationStore(directory).create({"source": "smoke"})
                    self.assertIsNotNone(run_id)

    def test_readiness_rejects_unexpected_check_constraints(self):
        def append_constraint(ddl, constraint):
            statement = ddl.rstrip().rstrip(";")
            return f"{statement[:-1]}, {constraint})"

        mutations = (
            (
                "extra-required",
                lambda ddl: self._append_column(
                    ddl,
                    "extra_required TEXT NOT NULL DEFAULT 'x' CHECK(extra_required = 'y')",
                ),
            ),
            (
                "runtime-column",
                lambda ddl: ddl.replace(
                    "status TEXT NOT NULL,",
                    "status TEXT NOT NULL CHECK(status = 'never'),",
                    1,
                ),
            ),
            (
                "extra-optional",
                lambda ddl: self._append_column(
                    ddl,
                    "extra_optional TEXT CHECK(extra_optional = 'x')",
                ),
            ),
            (
                "compatible-looking-default",
                lambda ddl: self._append_column(
                    ddl,
                    "extra_required TEXT NOT NULL DEFAULT 'x' CHECK(extra_required = 'x')",
                ),
            ),
            (
                "table-level",
                lambda ddl: append_constraint(ddl, "CHECK(status != 'never')"),
            ),
            (
                "nested",
                lambda ddl: append_constraint(ddl, "CHECK(length(status) > 0)"),
            ),
            (
                "case-and-whitespace",
                lambda ddl: append_constraint(ddl, "Check\n( status != 'never' )"),
            ),
        )
        for label, mutation in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "materialization_state.sqlite"
                self._recreate_table(path, "materialization_runs", mutation)

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "not_ready")

    def test_check_detector_ignores_quoted_text_and_comments(self):
        mutations = (
            (
                "quoted-default",
                lambda ddl: self._append_column(
                    ddl,
                    "extra_text TEXT NOT NULL DEFAULT 'CHECK(foo)'",
                ),
                True,
            ),
            (
                "comment",
                lambda ddl: (
                    lambda statement: f"{statement[:-1]}, extra_text TEXT -- CHECK(foo)\n)"
                )(ddl.rstrip().rstrip(";")),
                True,
            ),
        )
        for label, mutation, expected_ready in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                EarthLake(directory)
                AcquisitionStore(directory)
                MaterializationStore(directory)
                path = Path(directory) / "registry" / "materialization_state.sqlite"
                self._recreate_table(path, "materialization_runs", mutation)

                result = LakeHealthAuditor(directory).readiness()

                self.assertEqual(result["status"], "ready" if expected_ready else "not_ready")
                if expected_ready:
                    self.assertIsNotNone(MaterializationStore(directory).create({"source": label}))

    def test_readiness_rejects_reordered_composite_primary_key(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            path = Path(directory) / "registry" / "acquisition_state.sqlite"
            self._recreate_table(
                path,
                "search_pages",
                lambda ddl: ddl.replace(
                    "PRIMARY KEY (run_id, page_number)",
                    "PRIMARY KEY (page_number, run_id)",
                    1,
                ),
            )

            result = LakeHealthAuditor(directory).readiness()

            self.assertEqual(result["status"], "not_ready")

    def test_runtime_schema_metadata_covers_fresh_store_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            stores = (
                (AcquisitionStore, "acquisition_state.sqlite"),
                (MaterializationStore, "materialization_state.sqlite"),
            )

            for store_type, filename in stores:
                with self.subTest(store=store_type.__name__):
                    path = Path(directory) / "registry" / filename
                    with sqlite3.connect(path) as db:
                        actual_tables = {
                            str(row[0])
                            for row in db.execute(
                                "SELECT name FROM sqlite_master "
                                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                            )
                        }
                        self.assertTrue(set(store_type.RUNTIME_SCHEMA) <= actual_tables)
                        for table, table_schema in store_type.RUNTIME_SCHEMA.items():
                            actual_columns = {
                                str(row[1]): str(row[2]).upper()
                                for row in db.execute(f"PRAGMA table_info({table})")
                            }
                            expected_columns = {
                                column: str(spec["type"]).upper()
                                for column, spec in table_schema["columns"].items()
                            }
                            self.assertEqual(actual_columns, expected_columns)

    def test_runtime_schema_metadata_matches_fresh_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            AcquisitionStore(directory)
            MaterializationStore(directory)
            stores = (
                (AcquisitionStore, "acquisition_state.sqlite"),
                (MaterializationStore, "materialization_state.sqlite"),
            )

            for store_type, filename in stores:
                with self.subTest(store=store_type.__name__):
                    path = Path(directory) / "registry" / filename
                    with sqlite3.connect(path) as db:
                        for table, table_schema in store_type.RUNTIME_SCHEMA.items():
                            rows = db.execute(f"PRAGMA table_info(\"{table}\")").fetchall()
                            actual_pk = tuple(
                                row[1]
                                for row in sorted((row for row in rows if row[5]), key=lambda row: row[5])
                            )
                            expected_pk = tuple(
                                column
                                for column, spec in table_schema["columns"].items()
                                if "primary_key" in spec
                            )
                            self.assertEqual(actual_pk, expected_pk)
                            for row in rows:
                                column = row[1]
                                expected = table_schema["columns"][column]
                                self.assertEqual(bool(row[3]), expected["not_null"])
                                if "default" in expected:
                                    self.assertEqual(
                                        _normalize_sqlite_default(row[4]),
                                        _normalize_sqlite_default(expected["default"]),
                                    )
                            indexes = db.execute(f"PRAGMA index_list(\"{table}\")").fetchall()
                            for expected_unique in table_schema.get("unique_constraints", ()):
                                self.assertTrue(
                                    any(
                                        bool(index[2])
                                        and not bool(index[4])
                                        and tuple(
                                            item[2]
                                            for item in sorted(
                                                db.execute(
                                                    f"PRAGMA index_info(\"{index[1]}\")"
                                                ).fetchall(),
                                                key=lambda item: item[0],
                                            )
                                        ) == tuple(expected_unique)
                                        for index in indexes
                                    )
                                )
                            grouped_foreign_keys = {}
                            for row in db.execute(f"PRAGMA foreign_key_list(\"{table}\")"):
                                grouped_foreign_keys.setdefault(row[0], []).append(
                                    (row[1], row[3], row[2], row[4])
                                )
                            actual_foreign_keys = set()
                            for rows_for_key in grouped_foreign_keys.values():
                                ordered = sorted(rows_for_key, key=lambda item: item[0])
                                actual_foreign_keys.add(
                                    (
                                        ordered[0][2],
                                        tuple((item[1], item[3]) for item in ordered),
                                    )
                                )
                            for expected_foreign_key in table_schema.get("foreign_keys", ()):
                                self.assertIn(expected_foreign_key, actual_foreign_keys)

    def test_health_reports_corrupt_materialized_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data" / "camels_test"
            data.mkdir(parents=True)
            source = data / "attributes.csv"
            source.write_text("basin_id,value\n001,1\n", encoding="utf-8")
            first = HydroMaterializer(root / "data", root / "lake").materialize(kinds={"entities"})
            output = root / "lake" / first["records"][0]["output"]
            output.write_bytes(b"x" * output.stat().st_size)

            report = LakeHealthAuditor(root / "lake").run("audit-corrupt-materialization", full_checksum=True)

            issue = next(
                item for item in report["issues"]
                if item["code"] == "materialized_output_integrity_mismatch"
            )
            self.assertEqual(issue["severity"], "critical")
            self.assertEqual(report["health_status"], "unhealthy")

    def test_health_reports_corrupt_materialization_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data" / "camels_test"
            data.mkdir(parents=True)
            (data / "attributes.csv").write_text(
                "basin_id,value\n001,1\n", encoding="utf-8"
            )
            HydroMaterializer(root / "data", root / "lake").materialize(kinds={"entities"})
            manifest = root / "lake" / "manifests" / "materializations" / "hydrodatasets.json"
            manifest.write_text("{invalid json", encoding="utf-8")

            report = LakeHealthAuditor(root / "lake").run("audit-corrupt-materialization-manifest")

            issue = next(
                item for item in report["issues"]
                if item["code"] == "materialization_manifest_unreadable"
            )
            self.assertEqual(issue["severity"], "critical")
            self.assertEqual(report["health_status"], "unhealthy")


if __name__ == "__main__":
    unittest.main()
