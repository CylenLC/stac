import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import zarr
from pyproj import CRS as PyprojCRS
from rasterio.transform import from_origin
from shapely.geometry import Point

from earth_lake import EarthLake
from lake_footprint import valid_data_footprint
from lake_preview import render_preview
from lake_monitor import LakeMonitor


class LakeMonitorTests(unittest.TestCase):
    def test_summary_and_catalog_views_use_registered_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "interface": "test"})
            asset_path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-1") / "scene.B04.bin"
            asset_path.parent.mkdir(parents=True)
            asset_path.write_bytes(b"sample-raster")
            item = {
                "id": "granule-1",
                "collection": "HLSL30_V2.0",
                "bbox": [-101.0, 39.0, -99.0, 41.0],
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-101.0, 39.0],
                        [-99.0, 39.0],
                        [-99.0, 41.0],
                        [-101.0, 41.0],
                        [-101.0, 39.0],
                    ]],
                },
                "properties": {"datetime": "2024-01-01T12:00:00Z"},
            }
            asset_id = lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.bin",
                local_path=asset_path,
                status="downloaded",
            )
            lake.finish_run(run_id, "completed", [asset_id])
            (asset_path.parent / "._scene.B04.bin").write_bytes(b"macOS metadata")
            (asset_path.parent / ".DS_Store").write_bytes(b"macOS metadata")

            monitor = LakeMonitor(directory)
            summary = monitor.summary()
            self.assertEqual(summary["registry_counts"]["assets"], 1)
            self.assertEqual(summary["available_assets"], 1)
            self.assertEqual(summary["missing_assets"], 0)
            self.assertEqual(summary["unregistered_source_files"], 0)

            products = monitor.products()
            self.assertEqual(products[0]["asset_count"], 1)
            self.assertEqual(products[0]["variable_count"], 1)
            self.assertEqual(products[0]["bboxes"], [[-101.0, 39.0, -99.0, 41.0]])
            self.assertEqual(monitor.product("hlsl30_v2.0")["variables"][0]["source_name"], "B04")
            self.assertIsNone(monitor.product("missing"))

            asset = monitor.asset(asset_id)
            self.assertEqual(asset["bbox"], [-101.0, 39.0, -99.0, 41.0])
            self.assertEqual(asset["local_path"], "source/nasa/HLSL30_V2.0/granule-1/scene.B04.bin")
            spatial = monitor.spatial_assets()
            self.assertEqual(spatial["type"], "FeatureCollection")
            self.assertEqual(spatial["features"][0]["properties"]["asset_id"], asset_id)
            self.assertEqual(spatial["features"][0]["geometry"]["type"], "Polygon")
            self.assertIsNone(spatial["features"][0]["properties"]["preview_coordinates"])
            self.assertIsNone(spatial["features"][0]["properties"]["preview_cache_key"])

    def test_registry_pagination_and_query(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            monitor = LakeMonitor(directory)
            rows = monitor.registry_rows("sources", query="nasa", offset=0, limit=10)
            self.assertEqual(rows["total"], 0)
            self.assertEqual(rows["columns"][0], "source_id")
            with self.assertRaises(KeyError):
                monitor.registry_rows("not-a-table")

    def test_resources_do_not_escape_lake_root(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            entity = Path(directory) / "entities" / "stations" / "stations.parquet"
            entity.write_bytes(b"table")
            (entity.parent / ".stations.parquet.123.tmp").write_bytes(b"partial table")
            manifest = Path(directory) / "manifests" / "materializations" / "hydrodatasets.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "output": "entities/stations/stations.parquet",
                                "dataset_id": "camels_test",
                                "kind": "entity_stations",
                                "row_count": 2,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            resources = LakeMonitor(directory).resources("entities")
            by_path = {item["path"]: item for item in resources["items"]}
            paths = set(by_path)
            self.assertIn("entities/stations/stations.parquet", paths)
            self.assertEqual(by_path["entities/stations/stations.parquet"]["materialization"]["dataset_id"], "camels_test")
            self.assertTrue(all(not path.startswith("/") for path in paths))

    def test_resource_inventory_is_reused_within_cache_ttl(self):
        with tempfile.TemporaryDirectory() as directory:
            EarthLake(directory)
            entity = Path(directory) / "entities" / "stations" / "stations.parquet"
            entity.parent.mkdir(parents=True, exist_ok=True)
            entity.write_bytes(b"table")
            monitor = LakeMonitor(directory)

            with patch("lake_monitor.os.walk", wraps=os.walk) as walk:
                first = monitor.resources("entities")
                calls_after_first_read = walk.call_count
                second = monitor.resources("entities")

            self.assertEqual(first, second)
            self.assertGreater(calls_after_first_read, 0)
            self.assertEqual(walk.call_count, calls_after_first_read)

    def test_arrays_collapse_variable_groups_and_ignore_partial_stores(self):
        with tempfile.TemporaryDirectory() as directory:
            array_root = Path(directory) / "arrays" / "hydrology"
            store = array_root / "daily.zarr"
            (store / "flow").mkdir(parents=True)
            (store / "zarr.json").write_text('{"zarr_format": 3, "node_type": "group"}', encoding="utf-8")
            (store / "flow" / "zarr.json").write_text('{"zarr_format": 3, "node_type": "array"}', encoding="utf-8")
            partial = array_root / ".daily.zarr.123.partial"
            partial.mkdir(parents=True)
            (partial / "zarr.json").write_text('{"zarr_format": 3, "node_type": "group"}', encoding="utf-8")

            arrays = LakeMonitor(directory).arrays()
            resources = LakeMonitor(directory).resources("arrays")

            self.assertEqual([item["path"] for item in arrays], ["arrays/hydrology/daily.zarr"])
            resource_paths = {item["path"] for item in resources["items"]}
            self.assertIn("arrays/hydrology/daily.zarr", resource_paths)
            self.assertNotIn("arrays/hydrology/daily.zarr/zarr.json", resource_paths)
            self.assertTrue(all("partial" not in path for path in resource_paths))

    def test_entity_and_array_details_are_compact_and_confined_to_lake(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entity = root / "entities" / "basins" / "basins.parquet"
            entity.parent.mkdir(parents=True)
            table = pa.table({"basin_id": ["001", "002"], "area_km2": [12.5, 34.0]})
            table = table.replace_schema_metadata(
                {b"earthzarr": b'{"entity_type":"basin"}', b"geo": b'{"version":"1.1.0"}'}
            )
            pq.write_table(table, entity)

            store = root / "arrays" / "hydrology" / "daily.zarr"
            store.mkdir(parents=True)
            (store / "zarr.json").write_text(
                json.dumps(
                    {
                        "zarr_format": 3,
                        "node_type": "group",
                        "attributes": {"title": "Daily flow"},
                        "consolidated_metadata": {
                            "metadata": {
                                "flow": {
                                    "node_type": "array",
                                    "shape": [2, 3],
                                    "data_type": "float32",
                                    "dimension_names": ["time", "basin"],
                                    "chunk_grid": {"configuration": {"chunk_shape": [1, 3]}},
                                    "attributes": {"units": "m3/s"},
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            monitor = LakeMonitor(directory)
            entity_detail = monitor.resource_detail("entities/basins/basins.parquet")
            array_detail = monitor.array_detail("arrays/hydrology/daily.zarr")

            self.assertEqual(entity_detail["format"], "GeoParquet")
            self.assertEqual(entity_detail["row_count"], 2)
            self.assertEqual(entity_detail["schema"][0]["name"], "basin_id")
            self.assertEqual(entity_detail["sample_rows"][1]["basin_id"], "002")
            self.assertEqual(array_detail["zarr_format"], 3)
            self.assertEqual(array_detail["variables"][0]["name"], "flow")
            self.assertEqual(array_detail["variables"][0]["chunks"], [1, 3])
            with self.assertRaises(KeyError):
                monitor.resource_detail("../outside.parquet")

    def test_entity_page_features_and_cached_zarr_slice(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entity = root / "entities" / "basins" / "basins.geoparquet"
            entity.parent.mkdir(parents=True)
            table = pa.table({
                "basin_id": ["001", "002", "003"],
                "area_km2": [12.5, 34.0, 50.0],
                "geometry": [Point(-100, 40).wkb, Point(-99, 41).wkb, Point(-98, 42).wkb],
            })
            table = table.replace_schema_metadata({
                b"geo": json.dumps({
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB"}},
                }).encode(),
            })
            pq.write_table(table, entity)
            store = root / "arrays" / "hydrology" / "daily.zarr"
            group = zarr.open_group(store, mode="w", zarr_format=3)
            group.create_array("flow", data=np.arange(12, dtype="float32").reshape(3, 4))
            zarr.consolidate_metadata(store, zarr_format=3)

            monitor = LakeMonitor(directory)
            page = monitor.entity_page(
                "entities/basins/basins.geoparquet", offset=1, limit=1
            )
            features = monitor.entity_features(
                "entities/basins/basins.geoparquet", limit=2
            )
            first_slice = monitor.array_slice(
                "arrays/hydrology/daily.zarr", "flow", max_cells=6
            )
            second_slice = monitor.array_slice(
                "arrays/hydrology/daily.zarr", "flow", max_cells=6
            )

            self.assertEqual(page["total"], 3)
            self.assertEqual(page["items"][0]["basin_id"], "002")
            self.assertNotIn("geometry", page["columns"])
            self.assertEqual(len(features["features"]), 2)
            self.assertEqual(features["features"][0]["geometry"]["type"], "Point")
            self.assertEqual(first_slice["stats"]["min"], 0.0)
            self.assertFalse(first_slice["cached"])
            self.assertTrue(second_slice["cached"])

    def test_projected_geoparquet_features_are_transformed_to_wgs84(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entity = root / "entities" / "stations" / "projected.geoparquet"
            entity.parent.mkdir(parents=True)
            table = pa.table({"station_id": ["001"], "geometry": [Point(500000, 0).wkb]})
            table = table.replace_schema_metadata({
                b"geo": json.dumps({
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "crs": PyprojCRS.from_epsg(32631).to_json_dict(),
                        }
                    },
                }).encode(),
            })
            pq.write_table(table, entity)

            features = LakeMonitor(directory).entity_features(
                "entities/stations/projected.geoparquet"
            )

            longitude, latitude = features["features"][0]["geometry"]["coordinates"]
            self.assertAlmostEqual(longitude, 3.0, places=5)
            self.assertAlmostEqual(latitude, 0.0, places=5)

    def test_array_detail_reads_zarr_v2_consolidated_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "arrays" / "legacy.zarr"
            store.mkdir(parents=True)
            (store / ".zgroup").write_text('{"zarr_format":2}', encoding="utf-8")
            (store / ".zmetadata").write_text(json.dumps({
                "zarr_consolidated_format": 1,
                "metadata": {
                    "flow/.zarray": {"shape": [2, 3], "chunks": [1, 3], "dtype": "<f4"},
                    "flow/.zattrs": {"_ARRAY_DIMENSIONS": ["time", "basin"], "units": "m3 s-1"},
                },
            }), encoding="utf-8")

            detail = LakeMonitor(directory).array_detail("arrays/legacy.zarr")

            self.assertEqual(detail["zarr_format"], 2)
            self.assertEqual(detail["variables"][0]["name"], "flow")
            self.assertEqual(detail["variables"][0]["chunks"], [1, 3])

    def test_geotiff_preview_is_cached_by_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-preview") / "scene.B04.tif"
            path.parent.mkdir(parents=True)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=4,
                height=3,
                count=1,
                dtype="uint16",
                crs="EPSG:4326",
                transform=from_origin(-101, 41, 0.01, 0.01),
                nodata=0,
            ) as dataset:
                dataset.write(np.array([[0, 100, 200, 300], [400, 500, 600, 700], [800, 900, 1000, 1100]], dtype="uint16"), 1)
            asset_id = lake.record_asset(
                run_id=lake.start_run({"interface": "test"}),
                catalog="nasa",
                item={"id": "granule-preview", "collection": "HLSL30_V2.0", "bbox": [-101, 40.97, -100.96, 41], "geometry": None, "properties": {}},
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=path,
                status="downloaded",
            )
            asset = LakeMonitor(directory).asset(asset_id)
            first = render_preview(directory, asset, max_size=256)
            second = render_preview(directory, asset, max_size=256)
            first_footprint = valid_data_footprint(directory, asset)
            second_footprint = valid_data_footprint(directory, asset)
            self.assertFalse(first.cached)
            self.assertTrue(second.cached)
            self.assertFalse(first_footprint.cached)
            self.assertTrue(second_footprint.cached)
            self.assertEqual(first.path, second.path)
            self.assertTrue(first.path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
            feature = LakeMonitor(directory).spatial_assets()["features"][0]
            self.assertEqual(len(feature["properties"]["preview_coordinates"]), 4)
            self.assertTrue(feature["properties"]["preview_cache_key"].endswith("preview-v1"))
            self.assertEqual(feature["properties"]["geometry_source"], "valid_data")
            self.assertEqual(feature["geometry"]["type"], "Polygon")
            self.assertNotEqual(
                feature["geometry"]["coordinates"][0],
                feature["properties"]["preview_coordinates"] + [feature["properties"]["preview_coordinates"][0]],
            )
            self.assertEqual(len(list((Path(directory) / "cache" / "footprints").glob("*.geojson"))), 1)

            with patch("lake_monitor.valid_data_footprint", side_effect=AssertionError("source raster accessed")):
                lightweight = LakeMonitor(directory).spatial_assets(exact=False)["features"][0]
            self.assertEqual(lightweight["properties"]["geometry_source"], "raster_grid")
            self.assertEqual(lightweight["properties"]["preview_cache_key"], feature["properties"]["preview_cache_key"])

    def test_protocol_ignores_macos_appledouble_files(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            protocol_dir = Path(directory) / "protocol"
            (protocol_dir / "collection.json").write_text('{"title": "HLS"}', encoding="utf-8")
            (protocol_dir / "._collection.json").write_bytes(b"\x00\x05\x16\x07macOS metadata")

            documents = LakeMonitor(directory).protocol()

            self.assertEqual(documents["collection.json"], {"title": "HLS"})
            self.assertNotIn("._collection.json", documents)

    def test_valid_data_footprint_drops_internal_nodata_holes(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-hole") / "scene.B04.tif"
            path.parent.mkdir(parents=True)
            values = np.ones((5, 5), dtype="uint16")
            values[2, 2] = 0
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=5,
                height=5,
                count=1,
                dtype="uint16",
                crs="EPSG:4326",
                transform=from_origin(-101, 41, 0.01, 0.01),
                nodata=0,
            ) as dataset:
                dataset.write(values, 1)
            asset_id = lake.record_asset(
                run_id=lake.start_run({"interface": "test"}),
                catalog="nasa",
                item={"id": "granule-hole", "collection": "HLSL30_V2.0", "properties": {}},
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=path,
                status="downloaded",
            )

            geometry = valid_data_footprint(directory, LakeMonitor(directory).asset(asset_id)).geometry

            self.assertEqual(geometry["type"], "Polygon")
            self.assertEqual(len(geometry["coordinates"]), 1)


if __name__ == "__main__":
    unittest.main()
